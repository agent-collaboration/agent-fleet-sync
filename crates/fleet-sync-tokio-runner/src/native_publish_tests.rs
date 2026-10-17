#![allow(clippy::panic_in_result_fn)]
use crate::{native_publish::checkpoint_for_publish, *};
use durable_steps::{
    AtomicStore, CallFailure, NotSentReason, StoreError, storage::TransactionScope,
};
use runtime_primitives::SystemClock;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct NoCalls;
impl CallRouter for NoCalls {
    async fn dispatch(&self, _: CallEnvelope) -> Result<serde_json::Value, CallFailure> {
        Err(CallFailure::NotSent {
            reason: NotSentReason::Unreachable { last_seen: None },
        })
    }
}
type NativeHost = TokioHost<NoCalls>;
async fn object(
    path: &std::path::Path,
) -> TestResult<(ObjectStore<NativeHost>, Arc<TokioObject<NativeHost>>)> {
    let store = ObjectStore::open(path).await?;
    let host = Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock));
    let object = store
        .object(
            ObjectKey::new("copy-1")?,
            ObjectOptions {
                foreign_keys: false,
            },
            host,
        )
        .await?;
    object.transaction(|scope|Box::pin(async move {
        scope.executor().execute("CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO records VALUES(1,'old')").await.map_err(StoreError::backend)?;
        Ok(())
    })).await?;
    Ok((store, object))
}

#[tokio::test]
async fn checkpoint_closes_before_rename_and_reopens_the_published_file() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (store, object) = object(directory.path()).await?;
    let staging = directory.path().join("staging.db");
    let mut connection = TursoConnectOptions::new()
        .filename(&staging)
        .create_if_missing(true)
        .connect()
        .await?;
    connection.execute("CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO records VALUES(2,'verified snapshot')").await?;
    checkpoint_for_publish(&mut connection).await?;
    connection.close().await?;
    object
        .with_drained_file(move |live| {
            Box::pin(async move {
                tokio::fs::rename(&staging, live)
                    .await
                    .map_err(PublishError::io)?;
                let parent = live.parent().ok_or_else(|| PublishError::io("no parent"))?;
                tokio::fs::File::open(parent)
                    .await
                    .map_err(PublishError::io)?
                    .sync_all()
                    .await
                    .map_err(PublishError::io)?;
                Ok(())
            })
        })
        .await?;
    let values = object
        .transaction(|scope| {
            Box::pin(async move {
                let row = query::<Turso>("SELECT id,value FROM records")
                    .fetch_one(scope.executor())
                    .await
                    .map_err(StoreError::backend)?;
                Ok((
                    row.try_get::<i64, _>("id").map_err(StoreError::backend)?,
                    row.try_get::<String, _>("value")
                        .map_err(StoreError::backend)?,
                ))
            })
        })
        .await?;
    assert_eq!(values, (2, "verified snapshot".into()));
    store.close().await?;
    Ok(())
}

#[tokio::test]
async fn held_native_reader_refuses_publish_and_leaves_the_old_file_available() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (store, object) = object(directory.path()).await?;
    let mut connection = TursoConnectOptions::new()
        .filename(object.path())
        .connect()
        .await?;
    let mut reader = connection.begin().await?;
    let old = query::<Turso>("SELECT value FROM records WHERE id=1")
        .fetch_one(&mut *reader)
        .await?
        .try_get::<String, _>("value")?;
    assert_eq!(old, "old");
    object
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .executor()
                    .execute("UPDATE records SET value='committed after reader'")
                    .await
                    .map_err(StoreError::backend)?;
                Ok(())
            })
        })
        .await?;
    let entered = Arc::new(AtomicBool::new(false));
    let observed = entered.clone();
    let answer = object
        .with_drained_file(move |_| {
            Box::pin(async move {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            })
        })
        .await;
    println!("OBS held native reader checkpoint: {answer:?}");
    assert!(matches!(
        answer,
        Err(PublishError::CheckpointBusy {
            busy: 1,
            log: None,
            checkpointed: None
        })
    ));
    assert!(
        !entered.load(Ordering::SeqCst),
        "rename callback must not run after a busy checkpoint"
    );
    reader.rollback().await?;
    connection.close().await?;
    object
        .with_drained_file(|_| Box::pin(async { Ok(()) }))
        .await?;
    store.close().await?;
    Ok(())
}
