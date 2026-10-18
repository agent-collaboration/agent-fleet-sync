//! Follow-up admission and replay over the actual journal and consumer effect store.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_follow_up_fixture::*, *};
use durable_steps::{Engine, turso_store::TursoStore};
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

#[tokio::test]
async fn follow_up_admission_rolls_back_with_apply_and_recovers_after_lost_answer() -> TestResult {
    let directory = tempfile::tempdir()?;
    let journal_path = directory.path().join("copy.db");
    let host = host(&directory.path().join("consumer.db")).await?;
    let store = Arc::new(TursoStore::open_and_migrate(&journal_path, Arc::clone(&host)).await?);
    setup(&store, Arc::clone(&host.receiver_rules)).await?;
    let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
    engine.register(ApplyFollowUp::<TursoStore<TestHost>, String>::new())?;
    assert!(matches!(
        apply_and_admit(&store, Arc::clone(&host.receiver_rules), true).await,
        Err(durable_steps::StoreError::Aborted { .. })
    ));
    assert_eq!(
        counts(&store).await?,
        ("0".into(), "0".into(), "0".into(), "1".into())
    );
    assert!(engine.drive_due().await?.retried.is_empty());
    assert!(host.calls.lock().await.is_empty());
    apply_and_admit(&store, Arc::clone(&host.receiver_rules), false).await?;
    assert_eq!(
        counts(&store).await?,
        ("1".into(), "1".into(), "1".into(), "2".into())
    );
    let report = engine.drive_due().await?;
    assert_eq!(report.retried.len(), 1);
    assert_eq!(host.calls.lock().await.len(), 1);
    drop(engine);
    drop(store);
    host.now.store(1_000_000, Ordering::SeqCst);
    let reopened = Arc::new(TursoStore::open_and_migrate(&journal_path, Arc::clone(&host)).await?);
    let mut engine = Engine::new(Arc::clone(&reopened), Arc::clone(&host));
    engine.register(ApplyFollowUp::<TursoStore<TestHost>, String>::new())?;
    assert_eq!(engine.drive_due().await?.completed.len(), 1);
    let calls = host.calls.lock().await.clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls.first(), calls.last());
    let mut delivered = host.follow_up_delivery.delivered.lock().await;
    let row = sqlx_core::query::query::<Turso>(
        "SELECT CAST(count(*) AS TEXT) AS effects,item FROM delivered",
    )
    .fetch_one(&mut *delivered)
    .await?;
    assert_eq!(row.try_get::<String, _>("effects")?, "1");
    assert_eq!(row.try_get::<String, _>("item")?, "notice");
    drop(delivered);
    // KEEP_RESULT=false removes the finished journal row; replay remains the piece owner's job.
    assert_eq!(
        counts(&reopened).await?,
        ("1".into(), "1".into(), "0".into(), "2".into())
    );
    apply_and_admit(&reopened, Arc::clone(&host.receiver_rules), false).await?;
    assert!(engine.drive_due().await?.completed.is_empty());
    assert_eq!(host.calls.lock().await.len(), 2);
    Ok(())
}
