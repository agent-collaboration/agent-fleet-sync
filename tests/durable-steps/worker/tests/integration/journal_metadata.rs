//! The real DO bootstrap upgrades frozen v1 journal rows before replay.
use crate::{
    conformance::{check, step},
    legacy_rows::{LegacyOperationRow, LegacyStepRow, OPERATIONS, STEPS},
    support::TestHost,
};
use durable_steps::{
    AtomicStore, CallTarget, Engine, JournalScope, OperationContext, OperationFailure, OperationId,
    OperationState, RequestKey, StoreError, durable_object_store::DurableObjectStore,
    storage::TransactionScope,
};
use sqlx_core::{executor::Executor, query::query, query_as::query_as};
use sqlx_durable_object::{Do, DoConnection};
use std::sync::{Arc, atomic::Ordering};
use worker::State;

#[derive(Debug, PartialEq, sqlx::FromRow)]
pub(super) struct MetadataRow {
    pub id: String,
    pub created_at: i64,
    pub updated_at: i64,
}
#[derive(sqlx::FromRow)]
struct CountRow {
    count: i64,
}

pub(super) async fn metadata(
    store: &DurableObjectStore<TestHost>,
) -> Result<Vec<MetadataRow>, StoreError> {
    store.transaction(|scope|Box::pin(async move {
        Ok(query_as::<Do,MetadataRow>("SELECT operation_id AS id,created_at,updated_at FROM _fleet_sync_operations UNION ALL SELECT id,created_at,updated_at FROM _fleet_sync_steps UNION ALL SELECT id,created_at,updated_at FROM _fleet_sync_schema ORDER BY id")
            .fetch_all(scope.executor()).await?)
    })).await
}
pub(super) async fn history(
    store: &DurableObjectStore<TestHost>,
) -> Result<Vec<LegacyStepRow>, StoreError> {
    store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(query_as::<Do, LegacyStepRow>(STEPS)
                    .fetch_all(scope.executor())
                    .await?)
            })
        })
        .await
}
pub(super) async fn operations(
    store: &DurableObjectStore<TestHost>,
) -> Result<Vec<LegacyOperationRow>, StoreError> {
    store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(query_as::<Do, LegacyOperationRow>(OPERATIONS)
                    .fetch_all(scope.executor())
                    .await?)
            })
        })
        .await
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Double;
impl CallTarget for Double {
    const KIND: &'static str = "metadata-double";
    type Request = u64;
    type Answer = u64;
}
struct ReplayTask;
impl durable_steps::Task for ReplayTask {
    const KIND: &'static str = "metadata-replay";
    const VERSION: u32 = 1;
    type Store = DurableObjectStore<TestHost>;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        ctx: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        let input = *input;
        let saved = ctx
            .local(step("local"), move |scope| {
                Box::pin(async move {
                    query::<Do>("UPDATE effects SET value=value+1")
                        .execute(scope.executor())
                        .await
                        .map_err(StoreError::backend)?;
                    Ok(input)
                })
            })
            .await?;
        ctx.call(step("remote"), Double, saved)
            .await
            .map_err(Into::into)
    }
}

pub(crate) async fn run(
    state: &State,
    store: Arc<DurableObjectStore<TestHost>>,
    host: Arc<TestHost>,
) -> Result<Vec<&'static str>, StoreError> {
    DoConnection::new(state.storage())
        .transaction(|transaction| async move {
            (&transaction)
                .execute(include_str!(
                    "../../../../../crates/durable-steps/migrations/0001_durable_steps.sql"
                ))
                .await?;
            (&transaction)
                .execute(include_str!(
                    "../../../../../crates/durable-steps/tests/integration/fixtures/v1_pending.sql"
                ))
                .await?;
            Ok::<(), StoreError>(())
        })
        .await?;
    let before = history(&store).await?;
    let operation_before = operations(&store).await?;
    check(
        operation_before.len() == 2
            && operation_before
                .get(1)
                .is_some_and(|row| row.attempt == 7 && row.drive_until == Some(5_000_000_001_000)),
        "nondefault legacy operation fixture missing",
    )?;
    store.migrate().await?;
    check(
        history(&store).await? == before,
        "upgrade changed frozen saved call/local bytes",
    )?;
    check(
        operations(&store).await? == operation_before,
        "upgrade changed complete historical operation bytes",
    )?;
    let saved = metadata(&store).await?;
    check(
        saved.len() == 6
            && saved.iter().all(|row| {
                runtime_primitives::canonical_uuid_v7(&row.id)
                    && row.created_at != 0
                    && row.updated_at != 0
                    && row.updated_at >= row.created_at
            }),
        "all three DO journal tables need valid keys and timestamps",
    )?;
    let replayed = store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(scope
                    .admit::<ReplayTask>(
                        &RequestKey::new("old-request").map_err(StoreError::backend)?,
                        &7,
                    )
                    .await
                    .map_err(StoreError::backend)?
                    .id())
            })
        })
        .await?;
    check(
        replayed
            == OperationId::try_from("0197abc0-def0-7123-8123-abcdef012345".to_owned())
                .map_err(StoreError::backend)?,
        "old request admission changed identity",
    )?;
    store.transaction(|scope|Box::pin(async move {
        let tables=query_as::<Do,CountRow>("SELECT count(*) AS count FROM sqlite_master WHERE type='table' AND name GLOB '_fleet_sync_*'").fetch_one(scope.executor()).await?;
        check(tables.count==3,"DO must not invent native migration receipts")
    })).await?;
    store.migrate().await?;
    check(
        metadata(&store).await? == saved,
        "repeat bootstrap changed row ids or times",
    )?;
    crate::journal_constraints::run(&store).await?;
    crate::journal_id_refusals::run(&store).await?;
    crate::journal_clock::run(&store, &host).await?;
    let mut engine = Engine::new(store.clone(), host.clone());
    engine.register(ReplayTask)?;
    engine.drive_due().await?;
    let id = OperationId::try_from("0197abc0-def0-7123-8123-abcdef012345".to_owned())
        .map_err(StoreError::backend)?;
    check(
        matches!(engine.status(&id).await?.state,OperationState::Completed{result} if result==b"14"),
        "pending historical call did not finish",
    )?;
    check(
        host.effects.load(Ordering::SeqCst) == 1 && host.deliveries.load(Ordering::SeqCst) == 1,
        "remote dispatch count changed",
    )?;
    store
        .transaction(|scope| {
            Box::pin(async move {
                let row = query_as::<Do, CountRow>("SELECT value AS count FROM effects")
                    .fetch_one(scope.executor())
                    .await?;
                check(row.count == 1, "saved local effect ran twice")
            })
        })
        .await?;
    Ok(vec![
        "v1 history and metadata survive upgrade",
        "repeat bootstrap preserves row ids",
        "saved local effect replays once",
        "all natural keys and DO primary keys enforced",
        "DO malformed metadata ids refused",
        "DO writes use the injected clock",
    ])
}
