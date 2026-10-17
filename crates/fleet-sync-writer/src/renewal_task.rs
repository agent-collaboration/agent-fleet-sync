use crate::{
    OpenError, RegistryRenewal, RenewalCause, ReplicatedSchema, WriterJournalScope,
    WriterJournalStore,
    registration_bookkeeping::renewal_key,
    writer_authority::{AuthorityObservation, save_observation},
    writer_file_record::{read_file_record, write_file_record},
};
use durable_steps::{
    BoxFuture, Host, JournalScope, OperationContext, OperationFailure, OperationId, RequestKey,
    StepError, StepName, StoreError, Task,
};
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, time::Duration};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RenewLeaseInput {
    pub receiver: ReceiverId,
    pub force: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RenewalOperation {
    pub operation: OperationId,
    pub key: RequestKey,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum RenewalPlan {
    Wait {
        after_ms: u64,
    },
    Send {
        request: RenewRequest,
        started: SyncTime,
    },
    Stop,
}
pub struct RenewLease<S, H>(PhantomData<fn() -> (S, H)>);
impl<S, H> RenewLease<S, H> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}
impl<S, H> Default for RenewLease<S, H> {
    fn default() -> Self {
        Self::new()
    }
}
impl<S: ReplicatedSchema, H: Host> Task for RenewLease<S, H> {
    const KIND: &'static str = "fleet.writer-renewal";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = WriterJournalStore<S, H>;
    type Input = RenewLeaseInput;
    type Output = ();
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        ctx: &mut C,
        input: &RenewLeaseInput,
    ) -> Result<(), OperationFailure> {
        let now = ctx.now();
        let force = input.force;
        let plan = ctx
            .local(step("renewal-due")?, move |scope| {
                Box::pin(async move {
                    match scope
                        .renewal_plan(now, force)
                        .await
                        .map_err(writer_step_error)?
                    {
                        RenewalPlan::Wait { after_ms } => Err(StepError::Retry {
                            after: Duration::from_millis(after_ms),
                        }),
                        plan => Ok(plan),
                    }
                })
            })
            .await?;
        if let RenewalPlan::Send { request, started } = plan {
            let answer = ctx
                .call(
                    step("renew")?,
                    RegistryRenewal {
                        receiver: input.receiver,
                        cause: if input.force {
                            RenewalCause::Notice
                        } else {
                            RenewalCause::Periodic
                        },
                    },
                    request,
                )
                .await?;
            ctx.local(step("observe-renewal")?, move |scope| {
                Box::pin(async move {
                    scope
                        .observe_task_renewal(answer, started)
                        .await
                        .map_err(writer_step_error)?;
                    Ok(())
                })
            })
            .await?;
        }
        let (operation, receiver) = (*ctx.id(), input.receiver);
        ctx.local(step("schedule-next-renewal")?, move |scope| {
            Box::pin(async move {
                scope
                    .finish_renewal::<S>(operation, receiver, force)
                    .await
                    .map_err(writer_step_error)?;
                Ok(())
            })
        })
        .await?;
        Ok(())
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut WriterJournalScope<'t, H>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(async move {
            let mut record = read_file_record(scope.maintenance.connection)
                .await
                .map_err(StoreError::backend)?
                .ok_or_else(|| StoreError::backend("writer record missing"))?;
            record.renewal_failure = Some(failure.clone());
            if record
                .renewal_operation
                .as_ref()
                .is_some_and(|current| current.key == *key)
            {
                record.renewal_operation = None;
            }
            write_file_record(scope.maintenance.connection, &record)
                .await
                .map_err(StoreError::backend)
        })
    }
}
pub(crate) async fn renew_inline<
    S: ReplicatedSchema,
    H: Host,
    C: OperationContext<Store = WriterJournalStore<S, H>>,
>(
    ctx: &mut C,
    receiver: ReceiverId,
    name: String,
) -> Result<(), OperationFailure> {
    let now = ctx.now();
    let plan = ctx
        .local(step(format!("{name}:due"))?, move |scope| {
            Box::pin(async move {
                scope
                    .renewal_plan(now, false)
                    .await
                    .map_err(writer_step_error)
            })
        })
        .await?;
    if let RenewalPlan::Send { request, started } = plan {
        let answer = ctx
            .call(
                step(format!("{name}:call"))?,
                RegistryRenewal {
                    receiver,
                    cause: RenewalCause::Seeding,
                },
                request,
            )
            .await?;
        ctx.local(step(format!("{name}:observe"))?, move |scope| {
            Box::pin(async move {
                scope
                    .observe_task_renewal(answer, started)
                    .await
                    .map_err(writer_step_error)?;
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}
impl<H: Host> WriterJournalScope<'_, H> {
    pub(crate) async fn renewal_plan(
        &mut self,
        now: SyncTime,
        force: bool,
    ) -> Result<RenewalPlan, OpenError> {
        let state = match self.holder.writer_state(now) {
            Ok(state) => state,
            Err(_) => return Ok(RenewalPlan::Stop),
        };
        let epoch = match state {
            fleet_sync_lease::WriterState::Writing { epoch }
            | fleet_sync_lease::WriterState::Paused { epoch, .. }
            | fleet_sync_lease::WriterState::Seeding { epoch }
            | fleet_sync_lease::WriterState::Draining { epoch, .. }
            | fleet_sync_lease::WriterState::Released { epoch, .. } => epoch,
            _ => return Ok(RenewalPlan::Stop),
        };
        let Some(due) = self.holder.renewal_at() else {
            return Ok(RenewalPlan::Stop);
        };
        if !force && now < due {
            return Ok(RenewalPlan::Wait {
                after_ms: u64::try_from(due.unix_millis().saturating_sub(now.unix_millis()))
                    .unwrap_or(0),
            });
        }
        let attempt = RequestId::new(self.host.new_id())
            .map_err(|_| OpenError::corrupt("renewal.attempt"))?;
        Ok(RenewalPlan::Send {
            request: RenewRequest {
                attempt,
                database: self.holder.database(),
                epoch,
            },
            started: now,
        })
    }
    pub(crate) async fn observe_task_renewal(
        &mut self,
        answer: RenewAnswer,
        started: SyncTime,
    ) -> Result<(), OpenError> {
        let observation = AuthorityObservation::Renewal {
            answer,
            request_started: started,
        };
        let epoch = self.holder_epoch().await?;
        observation.restore(&mut self.holder, epoch)?;
        save_observation(self.maintenance.connection, observation).await?;
        let mut record = read_file_record(self.maintenance.connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        record.renewal_failure = None;
        write_file_record(self.maintenance.connection, &record).await
    }
    pub(crate) async fn ensure_renewal<S: ReplicatedSchema>(
        &mut self,
        receiver: ReceiverId,
    ) -> Result<(), OpenError> {
        let mut record = read_file_record(self.maintenance.connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        if record.renewal_operation.is_some()
            || matches!(
                self.renewal_plan(self.host.now(), false).await?,
                RenewalPlan::Stop
            )
        {
            return Ok(());
        }
        let key = renewal_key(self.host.new_id()).map_err(|error| OpenError::Storage {
            message: error.to_string(),
        })?;
        let admitted = self
            .admit::<RenewLease<S, H>>(
                &key,
                &RenewLeaseInput {
                    receiver,
                    force: false,
                },
            )
            .await
            .map_err(|error| OpenError::Storage {
                message: error.to_string(),
            })?;
        record.renewal_operation = Some(RenewalOperation {
            operation: admitted.id(),
            key,
        });
        write_file_record(self.maintenance.connection, &record).await
    }
    async fn finish_renewal<S: ReplicatedSchema>(
        &mut self,
        operation: OperationId,
        receiver: ReceiverId,
        force: bool,
    ) -> Result<(), OpenError> {
        if force {
            return Ok(());
        }
        let mut record = read_file_record(self.maintenance.connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        if record
            .renewal_operation
            .as_ref()
            .is_some_and(|current| current.operation == operation)
        {
            record.renewal_operation = None;
            write_file_record(self.maintenance.connection, &record).await?;
            self.ensure_renewal::<S>(receiver).await?;
        }
        Ok(())
    }
}
pub(crate) fn writer_step_error(error: OpenError) -> StepError {
    match error {
        error @ OpenError::Storage { .. } => StepError::Storage(StoreError::backend(error)),
        error => StepError::Failed(OperationFailure::Refused {
            code: "writer_rule_refused".into(),
            detail: error.to_string(),
        }),
    }
}
pub(crate) fn step(name: impl Into<String>) -> Result<StepName, OperationFailure> {
    StepName::new(name).map_err(|error| OperationFailure::InvalidPayload {
        message: error.to_string(),
    })
}
