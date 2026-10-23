//! Ready and its follow-ups share a commit; replay never admits them again.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_follow_up_fixture::*, *};
use durable_steps::{
    AtomicStore, Engine, StoreError as JournalError, storage::TransactionScope,
    turso_store::TursoStore,
};
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

type Store = TursoStore<TestHost>;

async fn ready_and_admit(
    store: &Store,
    hook: Arc<ReceiverTestRules>,
    install: SnapshotInstall,
    rollback: bool,
) -> Result<bool, JournalError> {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let first = match SnapshotInstallRules(scope)
                    .mark_ready(&install, &*hook)
                    .await
                    .map_err(storage)?
                {
                    SnapshotReady::Ready {
                        installed,
                        follow_ups,
                    } => {
                        admit_snapshot_follow_ups::<Store, String>(scope, &installed, follow_ups)
                            .await
                            .map_err(storage)?;
                        true
                    }
                    SnapshotReady::AlreadyReady { .. } => false,
                };
                if rollback {
                    return Err(JournalError::Aborted {
                        reason: "injected after Ready admission".into(),
                    });
                }
                Ok(first)
            })
        })
        .await
}

async fn ready_state(store: &Store) -> TestResult<Vec<String>> {
    Ok(store.transaction(|scope| Box::pin(async move {
        sqlx_core::query::query::<Turso>(
            "SELECT 'copy:'||record AS value FROM _fleet_sync_copy UNION ALL SELECT 'install:'||record FROM _fleet_sync_install UNION ALL SELECT 'hook:'||value FROM consumer_applied UNION ALL SELECT 'operation:'||operation_id||':'||state||':'||hex(input_digest) FROM _fleet_sync_operations ORDER BY value",
        ).fetch_all(scope.executor()).await?.into_iter()
            .map(|row| row.try_get("value").map_err(JournalError::from)).collect()
    })).await?)
}

#[tokio::test]
async fn ready_admission_rolls_back_then_delivers_once_across_reopen_and_replay() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("copy.db");
    let host = host(&directory.path().join("consumer.db")).await?;
    let store = Arc::new(Store::open_and_migrate(&path, Arc::clone(&host)).await?);
    let (install, status, page, manifest) = crate::turso_snapshot_install_tests::source()?;
    let staged_install = install.clone();
    let hook = Arc::clone(&host.receiver_rules);
    store
        .transaction(move |scope| {
            Box::pin(async move {
                sqlx_core::raw_sql::raw_sql(concat!(
                    include_str!("../migrations/0001_copy_rules.sql"),
                    "\n",
                    include_str!("../migrations/0002_replicated_schema.sql"),
                    "\n",
                    include_str!("../migrations/0003_snapshot_install.sql")
                ))
                .execute(scope.executor())
                .await?;
                initialize_hook(scope, &*hook).await.map_err(storage)?;
                let mut rules = SnapshotInstallRules(scope);
                rules
                    .prepare(&staged_install, &status)
                    .await
                    .map_err(storage)?;
                rules
                    .stage_page(&staged_install, 0, None, &page)
                    .await
                    .map_err(storage)?;
                rules
                    .verify(&staged_install, &manifest)
                    .await
                    .map_err(storage)?;
                // A retained piece item at this position has a different key namespace.
                admit_follow_ups::<Store, String>(
                    scope,
                    staged_install.database,
                    staged_install.position.epoch,
                    staged_install.position,
                    vec!["earlier-piece".into()],
                )
                .await
                .map_err(storage)?;
                Ok(())
            })
        })
        .await?;
    let before = ready_state(&store).await?;

    assert!(matches!(
        ready_and_admit(
            &store,
            Arc::clone(&host.receiver_rules),
            install.clone(),
            true
        )
        .await,
        Err(JournalError::Aborted { .. })
    ));
    assert_eq!(ready_state(&store).await?, before);
    assert!(host.calls.lock().await.is_empty());
    assert_eq!(
        host.follow_up_delivery
            .follow_up_calls
            .load(Ordering::SeqCst),
        0
    );

    assert!(
        ready_and_admit(
            &store,
            Arc::clone(&host.receiver_rules),
            install.clone(),
            false
        )
        .await?
    );
    let committed = ready_state(&store).await?;
    assert_eq!(host.receiver_rules.snapshot_calls.load(Ordering::SeqCst), 2);
    assert!(
        !ready_and_admit(
            &store,
            Arc::clone(&host.receiver_rules),
            install.clone(),
            false
        )
        .await?
    );
    assert_eq!(ready_state(&store).await?, committed);
    assert_eq!(host.receiver_rules.snapshot_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        host.follow_up_delivery
            .follow_up_calls
            .load(Ordering::SeqCst),
        0
    );

    let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
    engine.register(ApplyFollowUp::<Store, String>::new())?;
    let first = engine.drive_due().await?;
    assert_eq!(first.completed.len(), 1);
    assert_eq!(first.retried.len(), 1);
    assert!(first.failed.is_empty());
    drop(engine);
    drop(store);
    host.now.store(1_000_000, Ordering::SeqCst);
    let store = Arc::new(Store::open_and_migrate(&path, Arc::clone(&host)).await?);
    let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
    engine.register(ApplyFollowUp::<Store, String>::new())?;
    let resumed = engine.drive_due().await?;
    assert_eq!(resumed.completed.len(), 1);
    assert!(resumed.retried.is_empty());
    assert!(resumed.failed.is_empty());
    let calls = host.calls.lock().await.clone();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls.first(), calls.last());
    assert!(!ready_and_admit(&store, Arc::clone(&host.receiver_rules), install, false).await?);
    assert!(engine.drive_due().await?.completed.is_empty());
    assert_eq!(host.calls.lock().await.len(), 3);
    assert_eq!(host.receiver_rules.snapshot_calls.load(Ordering::SeqCst), 2);

    let mut delivered = host.follow_up_delivery.delivered.lock().await;
    let rows = sqlx_core::query::query::<Turso>(
        "SELECT item,CAST(count(*) AS TEXT) AS effects FROM delivered GROUP BY item ORDER BY item",
    )
    .fetch_all(&mut *delivered)
    .await?;
    let effects = rows
        .iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>("item")?,
                row.try_get::<String, _>("effects")?,
            ))
        })
        .collect::<Result<Vec<_>, sqlx_core::Error>>()?;
    assert_eq!(
        effects,
        [
            ("earlier-piece".into(), "1".into()),
            ("snapshot:1:7:v3:2".into(), "1".into())
        ]
    );
    Ok(())
}
