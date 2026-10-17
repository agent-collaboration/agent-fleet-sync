use crate::{
    InstallFile, InstallFileAnswer, InstallFileRequest, InstallFileResult, InstallRecord,
    InstallRequest, InstallSource, InstallStatus, StagedPage, TokioObject,
    install_contract::corrupt, install_records::InstallRecords,
};
use durable_steps::{
    BoxFuture, Host, OperationContext, OperationFailure, RequestKey, Resume, StepName, StoreError,
    Task, Uuid, turso_store::TursoScope,
};
use fleet_sync_receiver::SnapshotInstall;
use std::sync::Arc;

pub struct InstallSnapshot<H: Host> {
    host: Arc<H>,
}
impl<H: Host> InstallSnapshot<H> {
    pub fn new(host: Arc<H>) -> Self {
        Self { host }
    }
}
impl<H: Host> Task for InstallSnapshot<H> {
    const KIND: &'static str = "fleet.install-snapshot";
    const VERSION: u32 = 1;
    type Store = TokioObject<H>;
    type Input = InstallRequest;
    type Output = InstallRecord;
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        ctx: &mut C,
        input: &InstallRequest,
    ) -> Result<InstallRecord, OperationFailure> {
        let first = input.attempt;
        let mut record = ctx
            .local(step("start")?, move |scope| {
                Box::pin(async move { Ok(InstallRecords(scope).get(first).await?) })
            })
            .await?;
        let mut cycle = 0u64;
        loop {
            if !matches!(record.status, InstallStatus::Running { .. }) {
                return Ok(record);
            }
            let source = ctx
                .call(
                    step(format!("source:{cycle}"))?,
                    InstallSource {
                        receiver: input.source,
                        database: input.database,
                    },
                    (),
                )
                .await?
                .map_err(OperationFailure::from)?;
            if source.database != input.database {
                return Err(invalid("source answered another database"));
            }
            let snapshot = SnapshotInstall {
                install_id: record.install_id,
                database: input.database,
                position: source.acked,
                schema: source.schema.clone(),
            };
            let pinned = snapshot.clone();
            record = ctx
                .local(step(format!("pin:{cycle}"))?, move |scope| {
                    Box::pin(async move {
                        Ok(InstallRecords(scope).pin(pinned.install_id, pinned).await?)
                    })
                })
                .await?;
            if !matches!(record.status, InstallStatus::Running { .. }) {
                return Ok(record);
            }
            let file = InstallFile {
                install_id: record.install_id,
            };
            let prepared = ctx
                .call(
                    step(format!("prepare:{cycle}"))?,
                    file.clone(),
                    InstallFileRequest::Prepare { source },
                )
                .await?
                .map_err(OperationFailure::from)?;
            if !matches!(prepared, InstallFileResult::Prepared) {
                return outcome_record(ctx, &record, prepared, cycle).await;
            }
            let mut cursor = None;
            let mut page = 0u32;
            let moved = loop {
                let answer = ctx
                    .call(
                        step(format!("page:{cycle}:{page}"))?,
                        file.clone(),
                        InstallFileRequest::Page { page, cursor },
                    )
                    .await?
                    .map_err(OperationFailure::from)?;
                match answer {
                    InstallFileResult::Page(StagedPage::Receipt(receipt))
                        if receipt.install_id == record.install_id && receipt.page == page =>
                    {
                        cursor = receipt.next;
                        if cursor.is_none() {
                            break false;
                        }
                        page = page
                            .checked_add(1)
                            .ok_or_else(|| invalid("snapshot page index exhausted"))?;
                    }
                    InstallFileResult::Page(StagedPage::Moved { .. })
                    | InstallFileResult::Moved { .. } => break true,
                    other => return outcome_record(ctx, &record, other, cycle).await,
                }
            };
            let moved = if moved {
                true
            } else {
                let answer = ctx
                    .call(
                        step(format!("verify:{cycle}"))?,
                        file.clone(),
                        InstallFileRequest::Verify,
                    )
                    .await?
                    .map_err(OperationFailure::from)?;
                match answer {
                    InstallFileResult::Verified => false,
                    InstallFileResult::Moved { .. } => true,
                    other => return outcome_record(ctx, &record, other, cycle).await,
                }
            };
            if moved {
                let request = input.clone();
                let previous = record.install_id;
                let host = self.host.clone();
                record = ctx
                    .local(step(format!("restart:{cycle}"))?, move |scope| {
                        Box::pin(async move {
                            let mut records = InstallRecords(scope);
                            let previous = records.get(previous).await?;
                            if !matches!(previous.status, InstallStatus::Running { .. }) {
                                return Ok(previous);
                            }
                            Ok(records.reserve(&request, host.new_id()).await?)
                        })
                    })
                    .await?;
                cycle = cycle
                    .checked_add(1)
                    .ok_or_else(|| invalid("snapshot restart index exhausted"))?;
                continue;
            }
            let published = ctx
                .call(
                    step(format!("publish:{cycle}"))?,
                    file.clone(),
                    InstallFileRequest::Publish,
                )
                .await?
                .map_err(OperationFailure::from)?;
            if !matches!(
                published,
                InstallFileResult::Outcome(InstallFileAnswer::Published { .. })
            ) {
                return outcome_record(ctx, &record, published, cycle).await;
            }
            let ready = ctx
                .call(
                    step(format!("ready:{cycle}"))?,
                    file,
                    InstallFileRequest::Ready,
                )
                .await?
                .map_err(OperationFailure::from)?;
            return outcome_record(ctx, &record, ready, cycle).await;
        }
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut TursoScope<'t, H>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(async move {
            let (target, attempt) = parse_key(key)?;
            // Decision60: the install record owns the outcome. The engine row
            // is diagnostic and may fail after a newer install superseded this
            // one. Never replace terminal Ready or Superseded with that failure.
            InstallRecords(scope)
                .fail_attempt(&target, attempt, failure)
                .await
        })
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut TursoScope<'t, H>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<InstallRecord>, StoreError>> {
        Box::pin(async move {
            let (target, attempt) = parse_key(key)?;
            let record = InstallRecords(scope)
                .history(&target)
                .await?
                .into_iter()
                .rev()
                .find(|record| record.request.attempt == attempt)
                .ok_or_else(|| corrupt("install attempt has no record"))?;
            Ok(if matches!(record.status, InstallStatus::Running { .. }) {
                Resume::Continue
            } else {
                Resume::Finish(record)
            })
        })
    }
}
async fn outcome_record<C: OperationContext<Store = TokioObject<H>>, H: Host>(
    ctx: &mut C,
    record: &InstallRecord,
    answer: InstallFileResult,
    cycle: u64,
) -> Result<InstallRecord, OperationFailure> {
    if !matches!(answer, InstallFileResult::Outcome(_)) {
        return Err(invalid("unexpected install file answer"));
    }
    let id = record.install_id;
    let latest = ctx
        .local(step(format!("outcome:{cycle}"))?, move |scope| {
            Box::pin(async move { Ok(InstallRecords(scope).get(id).await?) })
        })
        .await?;
    if matches!(latest.status, InstallStatus::Running { .. }) {
        return Err(invalid(
            "file answered a terminal outcome without a terminal record",
        ));
    }
    Ok(latest)
}
pub(crate) fn request_key(request: &InstallRequest) -> Result<RequestKey, StoreError> {
    RequestKey::new(format!(
        "install:{}:{}",
        request.target.as_str(),
        request.attempt
    ))
    .map_err(StoreError::backend)
}
fn parse_key(key: &RequestKey) -> Result<(crate::ObjectKey, Uuid), StoreError> {
    let (target, attempt) = key
        .as_str()
        .strip_prefix("install:")
        .and_then(|key| key.split_once(':'))
        .ok_or_else(|| corrupt("invalid install request key"))?;
    Ok((
        crate::ObjectKey::new(target).map_err(StoreError::backend)?,
        Uuid::parse_str(attempt).map_err(StoreError::backend)?,
    ))
}
fn step(name: impl Into<String>) -> Result<StepName, OperationFailure> {
    StepName::new(name).map_err(|error| OperationFailure::InvalidPayload {
        message: error.to_string(),
    })
}

fn invalid(message: &str) -> OperationFailure {
    OperationFailure::Storage {
        error: corrupt(message),
    }
}
