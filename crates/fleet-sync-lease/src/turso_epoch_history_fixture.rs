//! Real registry and copy transitions for the public history projection.
use crate::{turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, RequestKey, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::sync::Arc;

pub(super) async fn page(
    store: &TestStore,
    after: Option<u64>,
    before: u64,
    limit: u32,
) -> TestResult<Result<EpochHistoryPage, LeaseError>> {
    let database = database()?;
    let after = after.map(Epoch::new).transpose()?;
    let before = Epoch::new(before)?;
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move { Ok(epoch_history(scope, database, after, before, limit).await) })
        })
        .await?)
}

pub(super) async fn move_to(
    engine: &TestEngine,
    id: TransferId,
    from: u64,
    to: u128,
) -> TestResult<HolderChangeAnswer> {
    task_answer::<HolderChangeTask<TestStore, Turso>>(
        engine,
        &holder_change_request_key(id)?,
        HolderChangeRequest::Move {
            reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
            attempt: id,
            database: database()?,
            from_epoch: Epoch::new(from)?,
            to: peer(to)?,
        },
    )
    .await
}

pub(super) async fn takeover(
    engine: &TestEngine,
    id: TransferId,
    to: u128,
) -> TestResult<HolderChangeAnswer> {
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        engine,
        &RequestKey::new(format!("history-preview-{}", id.as_uuid()))?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    task_answer::<HolderChangeTask<TestStore, Turso>>(
        engine,
        &holder_change_request_key(id)?,
        HolderChangeRequest::Takeover {
            reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
            attempt: id,
            database: database()?,
            preview,
            to: peer(to)?,
        },
    )
    .await
}

pub(super) async fn records(store: &TestStore) -> TestResult<Vec<String>> {
    Ok(store
        .transaction(|scope| Box::pin(records_in_scope(scope)))
        .await?)
}

pub(super) async fn records_in_scope(
    scope: &mut <TestStore as AtomicStore>::Scope<'_>,
) -> Result<Vec<String>, durable_steps::StoreError> {
    sqlx_core::query::query::<Turso>(
        "SELECT 'history:' || database || ':' || epoch || ':' || record AS value FROM _fleet_sync_epoch_history UNION ALL SELECT 'lease:' || database || ':' || record AS value FROM _fleet_sync_leases UNION ALL SELECT 'transfer:' || attempt || ':' || database || ':' || intent || ':' || coalesce(outcome,'') || ':' || phase || ':' || phase_history AS value FROM _fleet_sync_transfers UNION ALL SELECT 'floor:' || database || ':' || epoch AS value FROM _fleet_sync_epoch_floor ORDER BY value"
    ).fetch_all(scope.executor()).await?.into_iter().map(|row| row.try_get("value").map_err(storage)).collect()
}

pub(super) async fn registered(
    directory: &std::path::Path,
) -> TestResult<(Arc<TestHost>, Arc<TestStore>, TestEngine)> {
    let host = TestHost::open(directory, false).await?;
    let store = open_store(&directory.join("registry.db"), Arc::clone(&host), true).await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    crate::turso_change_tests::register_first(&engine).await?;
    Ok((host, store, engine))
}
