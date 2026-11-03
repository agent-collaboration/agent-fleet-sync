#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_epoch_history_fixture::{records, registered},
    turso_registry_fixture::*,
    *,
};
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn history_surrogate_id_is_validated_before_returning_a_grant() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (_, store, _) = registered(directory.path()).await?;
    store.transaction(|scope|Box::pin(async move {query::<Turso>("UPDATE _fleet_sync_epoch_history SET id='019eef00-0000-4000-8000-000000000099'").execute(scope.executor()).await?;Ok(())})).await?;
    let before = records(&store).await?;
    let result = store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(epoch_holder(
                    scope,
                    database().map_err(storage)?,
                    Epoch::new(1).map_err(storage)?,
                )
                .await)
            })
        })
        .await?;
    assert!(
        result.is_err(),
        "history returned a row with a v4 surrogate id"
    );
    assert_eq!(records(&store).await?, before);
    Ok(())
}
async fn renewal_rows(store: &TestStore) -> TestResult<Vec<String>> {
    Ok(store.transaction(|scope|Box::pin(async move {
        query::<Turso>("SELECT json_array(id,database,attempt,request_epoch,holder,answer_kind,recorded_epoch,recorded_expires_at,current_database,current_epoch,current_holder,current_state,current_expires_at,current_remaining_ms,transfer_in_progress,replayed,refusal_kind,refusal_current_epoch,refusal_ended_epoch,refusal_ended_sequence,created_at,updated_at) AS value FROM _fleet_sync_renewal_attempts ORDER BY id").fetch_all(scope.executor()).await?.into_iter().map(|row|row.try_get("value").map_err(storage)).collect()
    })).await?)
}
#[tokio::test]
async fn renewal_surrogate_id_is_validated_before_replay_without_repair() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (_, store, _) = registered(directory.path()).await?;
    let request = RenewRequest {
        database: database()?,
        attempt: RequestId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000004901,
        ))?,
        epoch: Epoch::new(1)?,
    };
    let first = request.clone();
    store
        .transaction(move |scope| {
            Box::pin(async move {
                renew(
                    scope,
                    &first,
                    &peer(1).map_err(storage)?,
                    SyncTime::from_unix_millis(1100),
                    &config().map_err(storage)?.lease,
                    &NoLeaseChangeHook,
                )
                .await
                .map_err(storage)?;
                query::<Turso>("UPDATE _fleet_sync_renewal_attempts SET id='garbage'")
                    .execute(scope.executor())
                    .await?;
                Ok(())
            })
        })
        .await?;
    let before = records(&store).await?;
    let receipts = renewal_rows(&store).await?;
    let result = store
        .transaction(move |scope| {
            Box::pin(async move {
                Ok(renew(
                    scope,
                    &request,
                    &peer(1).map_err(storage)?,
                    SyncTime::from_unix_millis(1200),
                    &config().map_err(storage)?.lease,
                    &NoLeaseChangeHook,
                )
                .await)
            })
        })
        .await?;
    assert!(
        result.is_err(),
        "renewal replay accepted garbage surrogate id"
    );
    assert_eq!(records(&store).await?, before);
    assert_eq!(renewal_rows(&store).await?, receipts);
    Ok(())
}
#[tokio::test]
async fn phase_history_surrogate_id_is_validated_before_admin_read_without_repair() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (host, store, engine) = registered(directory.path()).await?;
    host.lose_release.store(true, Ordering::SeqCst);
    let id = crate::turso_workflow_tests::attempt(4903)?;
    engine
        .start::<HolderChangeTask<TestStore, Turso, NoLeaseChangeHook>>(
            &holder_change_request_key(id)?,
            crate::turso_workflow_tests::move_request(id)?,
        )
        .await?;
    store.transaction(|scope|Box::pin(async move {query::<Turso>("UPDATE _fleet_sync_transfer_phase_history SET id='019eef00-0000-4000-8000-000000000099' WHERE position=1").execute(scope.executor()).await?;Ok(())})).await?;
    let before = records(&store).await?;
    let result = store
        .transaction(move |scope| Box::pin(async move { Ok(holder_change_view(scope, id).await) }))
        .await?;
    assert!(
        result.is_err(),
        "admin read accepted v4 phase-history surrogate id"
    );
    assert_eq!(records(&store).await?, before);
    Ok(())
}
