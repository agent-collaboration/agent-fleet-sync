#![cfg(feature = "turso")]
#![allow(clippy::panic_in_result_fn)]
mod support;

use durable_steps::{
    AtomicStore, Engine, Host, OperationContext, OperationFailure, OperationState, RequestKey,
    StepName, StoreError, Task, storage::TransactionScope, turso_store::TursoStore,
};
use sqlx_turso::Turso;
use std::{marker::PhantomData, sync::Arc};

struct GenericTask<S>(PhantomData<fn() -> S>);
impl<S: AtomicStore> Task for GenericTask<S>
where
    for<'t> S::Scope<'t>: TransactionScope<Database = Turso>,
{
    const KIND: &'static str = "send-future";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        context: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        let output = *input;
        context
            .local(
                StepName::new("generic-scope").map_err(|error| OperationFailure::Storage {
                    error: StoreError::backend(error),
                })?,
                move |scope| {
                    Box::pin(async move {
                        let _executor = scope.executor();
                        Ok(output)
                    })
                },
            )
            .await
            .map_err(Into::into)
    }
}

fn require_send<T: Send>(value: T) -> T {
    value
}

async fn drive_generic_task<S: AtomicStore, H: Host>(
    engine: Arc<Engine<S, H>>,
) -> Result<(), StoreError>
where
    for<'t> S::Scope<'t>: TransactionScope<Database = Turso>,
{
    let key = RequestKey::new("generic-send").map_err(StoreError::backend)?;
    let id = require_send(engine.start::<GenericTask<S>>(&key, 41))
        .await
        .map_err(StoreError::backend)?;
    require_send(engine.drive_due()).await?;
    let status = require_send(engine.status(&id)).await?;
    assert_eq!(
        status.state,
        OperationState::Completed {
            result: b"41".to_vec()
        }
    );
    Ok(())
}

#[tokio::test]
async fn store_generic_engine_futures_are_send_and_can_be_spawned() -> Result<(), StoreError> {
    let directory = tempfile::tempdir().map_err(StoreError::backend)?;
    let host = support::TestHost::new();
    let store = Arc::new(
        TursoStore::open_and_migrate(directory.path().join("send.db"), Arc::clone(&host)).await?,
    );
    let mut engine = Engine::new(store, host);
    engine.register(GenericTask(PhantomData))?;
    tokio::spawn(require_send(drive_generic_task(Arc::new(engine))))
        .await
        .map_err(StoreError::backend)?
}
