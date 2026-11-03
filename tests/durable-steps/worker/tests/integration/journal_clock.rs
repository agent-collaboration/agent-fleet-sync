use crate::{
    conformance::{check, step},
    support::TestHost,
};
use durable_steps::{
    AtomicStore, DriveLease, JournalScope, OperationContext, OperationFailure, OperationId,
    RequestKey, StoreError, SyncTime, Task, Uuid,
    durable_object_store::DurableObjectStore,
    storage::{JournalStorage, StepRecord, StepState, TransactionScope},
};
use sqlx_core::query_as::query_as;
use sqlx_durable_object::Do;
use std::sync::atomic::Ordering;

struct ClockTask;
impl Task for ClockTask {
    const KIND: &'static str = "fixture-metadata-clock";
    const VERSION: u32 = 1;
    type Store = DurableObjectStore<TestHost>;
    type Input = u64;
    type Output = ();
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        _ctx: &mut C,
        _input: &u64,
    ) -> Result<(), OperationFailure> {
        Ok(())
    }
}
#[derive(Debug, PartialEq, sqlx::FromRow)]
struct TimeRow {
    id: String,
    created_at: i64,
    updated_at: i64,
}
async fn times(
    store: &DurableObjectStore<TestHost>,
    id: OperationId,
) -> Result<Vec<TimeRow>, StoreError> {
    store.transaction(move |scope|Box::pin(async move {
        let key=id.as_uuid().to_string();
        Ok(query_as::<Do,TimeRow>("SELECT 'operation' AS part,operation_id AS id,created_at,updated_at FROM _fleet_sync_operations WHERE operation_id=? UNION ALL SELECT 'step' AS part,id,created_at,updated_at FROM _fleet_sync_steps WHERE operation_id=? ORDER BY part").bind(key.clone()).bind(key).fetch_all(scope.executor()).await?)
    })).await
}
pub(crate) async fn run(
    store: &DurableObjectStore<TestHost>,
    host: &TestHost,
) -> Result<(), StoreError> {
    // Freeze and advance this injected clock; no elapsed-time assertion or wait.
    let first = host.clock.load(Ordering::SeqCst) + 12_345;
    host.clock.store(first, Ordering::SeqCst);
    let token = Uuid::from_u128(0x0197abc0def071238123abcdef012349);
    let operation = store
        .transaction(move |scope| {
            Box::pin(async move {
                let id = scope
                    .admit::<ClockTask>(
                        &RequestKey::new("metadata-clock").map_err(StoreError::backend)?,
                        &1,
                    )
                    .await
                    .map_err(StoreError::backend)?
                    .id();
                check(
                    scope
                        .claim(
                            id,
                            SyncTime::from_unix_millis(first),
                            DriveLease {
                                token,
                                until: SyncTime::from_unix_millis(first + 30_000),
                            },
                        )
                        .await?
                        .is_some(),
                    "clock fixture claim missing",
                )?;
                scope
                    .save_step(
                        StepRecord {
                            operation: id,
                            position: 0,
                            name: step("clock-local"),
                            state: StepState::Local {
                                output: b"1".to_vec(),
                            },
                        },
                        token,
                    )
                    .await?;
                Ok(id)
            })
        })
        .await?;
    let before = times(store, operation).await?;
    check(
        before.len() == 2
            && before.iter().all(|row| {
                runtime_primitives::canonical_uuid_v7(&row.id)
                    && row.created_at == first
                    && row.updated_at == first
            }),
        "DO initial writes did not use the injected clock",
    )?;
    host.clock.store(first + 100, Ordering::SeqCst);
    store
        .transaction(move |scope| {
            Box::pin(async move {
                scope
                    .save_step(
                        StepRecord {
                            operation,
                            position: 0,
                            name: step("clock-local"),
                            state: StepState::Local {
                                output: b"2".to_vec(),
                            },
                        },
                        token,
                    )
                    .await?;
                scope
                    .retry(
                        operation,
                        token,
                        SyncTime::from_unix_millis(first + 50_000),
                        1,
                    )
                    .await
            })
        })
        .await?;
    let after = times(store, operation).await?;
    check(
        after.len() == 2
            && after.iter().zip(&before).all(|(after, before)| {
                after.id == before.id
                    && after.created_at == first
                    && after.updated_at == first + 100
            }),
        "DO updates changed identity/creation or ignored the injected clock",
    )
}
