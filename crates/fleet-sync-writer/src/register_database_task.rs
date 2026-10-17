use crate::{
    RegisterDatabaseInput, RegistryRegistration, ReplicatedSchema, SeedCopy, SeedCopyInput,
    WriterJournalScope, WriterJournalStore, WriterRegistrationOutcome,
    renewal_task::{step, writer_step_error},
};
use durable_steps::{
    BoxFuture, Host, OperationContext, OperationFailure, RequestKey, Resume, StoreError, Task,
};
use fleet_sync_protocol::{CommitSequence, Origin, ReplicationPosition};
use std::{marker::PhantomData, sync::Arc};

pub struct RegisterDatabase<S, H> {
    host: Arc<H>,
    schema: PhantomData<fn() -> S>,
}
impl<S, H> RegisterDatabase<S, H> {
    pub fn new(host: Arc<H>) -> Self {
        Self {
            host,
            schema: PhantomData,
        }
    }
}
impl<S: ReplicatedSchema, H: Host> Task for RegisterDatabase<S, H> {
    const KIND: &'static str = "fleet.writer-registration";
    const VERSION: u32 = 1;
    type Store = WriterJournalStore<S, H>;
    type Input = RegisterDatabaseInput;
    type Output = WriterRegistrationOutcome;
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        ctx: &mut C,
        input: &RegisterDatabaseInput,
    ) -> Result<Self::Output, OperationFailure> {
        let now = ctx.now();
        let started = ctx
            .local(step("registration-started")?, move |_| {
                Box::pin(async move { Ok(now) })
            })
            .await?;
        let answer = ctx
            .call(
                step("register")?,
                RegistryRegistration {
                    receiver: input.receiver,
                },
                input.request.clone(),
            )
            .await?;
        let (origin, receiver) = (input.request.origin.clone(), input.receiver);
        let observed = ctx
            .local(step("observe-registration")?, move |scope| {
                Box::pin(async move {
                    scope
                        .record_registration_answer(answer, started)
                        .await
                        .map_err(writer_step_error)?;
                    if let Origin::Empty = origin
                        && let WriterRegistrationOutcome::Ready { lease } = scope
                            .registration_outcome()
                            .await
                            .map_err(writer_step_error)?
                    {
                        scope
                            .register_sender_copy(
                                receiver,
                                ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
                            )
                            .await
                            .map_err(writer_step_error)?;
                    }
                    scope
                        .ensure_renewal::<S>(receiver)
                        .await
                        .map_err(writer_step_error)?;
                    scope
                        .registration_outcome()
                        .await
                        .map_err(writer_step_error)
                })
            })
            .await?;
        if !matches!(observed, WriterRegistrationOutcome::Pending) {
            return Ok(observed);
        }
        let Origin::Seeded { position } = input.request.origin else {
            return Err(OperationFailure::InvalidPayload {
                message: "empty registration did not finish".into(),
            });
        };
        let position = SeedCopy::<S, H>::new(self.host.clone())
            .run(
                ctx,
                &SeedCopyInput {
                    receiver: input.receiver,
                    position,
                },
            )
            .await?;
        let receiver = input.receiver;
        ctx.local(step("registration-ready")?, move |scope| {
            Box::pin(async move {
                scope.complete_seed().await.map_err(writer_step_error)?;
                scope
                    .register_sender_copy(receiver, position)
                    .await
                    .map_err(writer_step_error)?;
                scope
                    .ensure_renewal::<S>(receiver)
                    .await
                    .map_err(writer_step_error)?;
                scope
                    .registration_outcome()
                    .await
                    .map_err(writer_step_error)
            })
        })
        .await
        .map_err(Into::into)
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut WriterJournalScope<'t, H>,
        _: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(async move {
            scope
                .fail_registration(failure)
                .await
                .map_err(StoreError::backend)
        })
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut WriterJournalScope<'t, H>,
        _: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<Self::Output>, StoreError>> {
        Box::pin(async move {
            Ok(
                match scope
                    .registration_outcome()
                    .await
                    .map_err(StoreError::backend)?
                {
                    WriterRegistrationOutcome::Pending => Resume::Continue,
                    outcome => Resume::Finish(outcome),
                },
            )
        })
    }
}
