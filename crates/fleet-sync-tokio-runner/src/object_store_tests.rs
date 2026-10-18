#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{
    AtomicStore, CallFailure, NotSentReason, ReborrowScope, StoreError, storage::TransactionScope,
};
use runtime_primitives::SystemClock;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::{
    future::{Future, poll_fn},
    path::Path,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};
use tokio::{sync::watch, time::timeout};

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
fn host() -> Arc<NativeHost> {
    Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock))
}
async fn polls_pending(mut future: Pin<&mut impl Future>) -> bool {
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context).is_pending())).await
}
async fn connection(directory: &Path, key: &ObjectKey) -> TestResult<TursoConnection> {
    Ok(TursoConnectOptions::new()
        .filename(directory.join(format!("{}.db", key.as_str())))
        .create_if_missing(true)
        .connect()
        .await?)
}
async fn wait_for_held_initialization(
    initializing: &mut (impl Future<Output = Result<Arc<TokioObject<NativeHost>>, StoreError>> + Unpin),
    changes: &mut watch::Receiver<(Option<ObjectKey>, usize)>,
    key: &ObjectKey,
) -> TestResult {
    // The real file's BEGIN is held. Observe the newly opened connection, then
    // let that initialization remain pending at its actual transaction attempt.
    timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = initializing => Err("initialization completed while its BEGIN lock was held".into()),
            observed = changes.wait_for(|(opened, _)| opened.as_ref() == Some(key)) => {
                let _ = observed?;
                Ok(())
            }
        }
    })
    .await?
}
async fn write_and_read(object: &TokioObject<NativeHost>) -> TestResult<i64> {
    Ok(object
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .executor()
                    .execute("CREATE TABLE owner_values(value INTEGER NOT NULL); INSERT INTO owner_values VALUES(42)")
                    .await
                    .map_err(StoreError::backend)?;
                query::<Turso>("SELECT value FROM owner_values")
                    .fetch_one(scope.executor())
                    .await
                    .map_err(StoreError::backend)?
                    .try_get("value")
                    .map_err(StoreError::backend)
            })
        })
        .await?)
}

#[tokio::test]
async fn reborrowed_object_scope_keeps_the_parent_transaction() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = ObjectStore::open(directory.path()).await?;
    let object = store
        .object(
            ObjectKey::new("reborrow")?,
            ObjectOptions::default(),
            host(),
        )
        .await?;
    object.transaction(|parent| Box::pin(async move {
        parent.executor().execute("CREATE TABLE reborrow_values(value INTEGER NOT NULL); INSERT INTO reborrow_values VALUES (3)").await?;
        {
            let mut view = TokioObject::<NativeHost>::reborrow(parent);
            let before: i64 = query::<Turso>("SELECT value FROM reborrow_values").fetch_one(view.executor()).await?.try_get("value")?;
            assert_eq!(before, 3);
            view.executor().execute("UPDATE reborrow_values SET value = 4").await?;
        }
        let after: i64 = query::<Turso>("SELECT value FROM reborrow_values").fetch_one(parent.executor()).await?.try_get("value")?;
        assert_eq!(after, 4);
        Ok(())
    })).await?;
    let aborted = object
        .transaction(|parent| {
            Box::pin(async move {
                let mut view = TokioObject::<NativeHost>::reborrow(parent);
                view.executor()
                    .execute("UPDATE reborrow_values SET value = 5")
                    .await?;
                Err::<(), _>(StoreError::Aborted {
                    reason: "object reborrow rollback".into(),
                })
            })
        })
        .await;
    assert!(
        matches!(aborted, Err(StoreError::Aborted { reason }) if reason == "object reborrow rollback")
    );
    let committed: i64 = object
        .transaction(|parent| {
            Box::pin(async move {
                query::<Turso>("SELECT value FROM reborrow_values")
                    .fetch_one(parent.executor())
                    .await?
                    .try_get("value")
                    .map_err(StoreError::backend)
            })
        })
        .await?;
    store.close().await?;
    assert_eq!(committed, 4);
    Ok(())
}

#[tokio::test]
async fn held_initialization_does_not_block_another_key_or_a_cached_lookup() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = ObjectStore::open(directory.path()).await?;
    let options = ObjectOptions::default();
    let cached_key = ObjectKey::new("cached")?;
    let cached = store.object(cached_key.clone(), options, host()).await?;
    let blocked_key = ObjectKey::new("blocked")?;
    let mut lock_holder = connection(directory.path(), &blocked_key).await?;
    lock_holder
        .execute("CREATE TABLE lock_witness(value INTEGER)")
        .await?;
    let held = lock_holder.begin_with("BEGIN IMMEDIATE").await?;
    let mut changes = store.initialization_connections.subscribe();
    let mut initializing = Box::pin(store.object(blocked_key.clone(), options, host()));
    wait_for_held_initialization(&mut initializing, &mut changes, &blocked_key).await?;

    let (new_owner, cached_lookup) = tokio::join!(
        timeout(Duration::from_secs(1), async {
            let object = store
                .object(ObjectKey::new("independent")?, options, host())
                .await?;
            write_and_read(&object).await
        }),
        timeout(
            Duration::from_secs(1),
            store.object(cached_key, options, host())
        ),
    );
    // Release and finish the blocked owner even when the old global lock made
    // the independent operations time out; the assertions below preserve red.
    held.rollback().await?;
    timeout(Duration::from_secs(2), initializing).await??;
    lock_holder.close().await?;
    store.close().await?;
    assert!(
        new_owner.is_ok() && cached_lookup.is_ok(),
        "new owner completed: {}; cached lookup completed: {}",
        new_owner.is_ok(),
        cached_lookup.is_ok()
    );
    assert_eq!(new_owner??, 42);
    let cached_lookup = cached_lookup??;
    assert!(Arc::ptr_eq(&cached, &cached_lookup));
    Ok(())
}

#[tokio::test]
async fn concurrent_same_key_acquisitions_share_one_connection_and_keep_options() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = ObjectStore::open(directory.path()).await?;
    let key = ObjectKey::new("coalesced")?;
    let options = ObjectOptions::default();
    let mut lock_holder = connection(directory.path(), &key).await?;
    lock_holder
        .execute("CREATE TABLE lock_witness(value INTEGER)")
        .await?;
    let held = lock_holder.begin_with("BEGIN IMMEDIATE").await?;
    let mut changes = store.initialization_connections.subscribe();
    let mut initializing = Box::pin(store.object(key.clone(), options, host()));
    wait_for_held_initialization(&mut initializing, &mut changes, &key).await?;
    let mut waiting = (0..8)
        .map(|_| Box::pin(store.object(key.clone(), options, host())))
        .collect::<Vec<_>>();
    for acquisition in &mut waiting {
        assert!(polls_pending(acquisition.as_mut()).await);
    }
    let mut mismatched = Box::pin(store.object(
        key,
        ObjectOptions {
            foreign_keys: false,
        },
        host(),
    ));
    assert!(polls_pending(mismatched.as_mut()).await);

    held.rollback().await?;
    let owner = timeout(Duration::from_secs(2), initializing).await??;
    for acquisition in waiting {
        let observed = timeout(Duration::from_secs(2), acquisition).await??;
        assert!(Arc::ptr_eq(&owner, &observed));
    }
    assert!(
        matches!(mismatched.await, Err(StoreError::Aborted { reason }) if reason == "object connection options changed")
    );
    assert_eq!(store.initialization_connections.borrow().1, 1);
    assert_eq!(store.objects().await.len(), 1);
    assert_eq!(write_and_read(&owner).await?, 42);
    lock_holder.close().await?;
    store.close().await?;
    Ok(())
}

#[tokio::test]
async fn failed_journal_initialization_leaves_the_key_retryable() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = ObjectStore::open(directory.path()).await?;
    let key = ObjectKey::new("retry-failure")?;
    let mut repair = connection(directory.path(), &key).await?;
    repair
        .execute("CREATE TABLE _fleet_sync_migrations(invalid INTEGER)")
        .await?;

    assert!(
        store
            .object(key.clone(), ObjectOptions::default(), host())
            .await
            .is_err()
    );
    assert!(store.objects().await.is_empty());
    repair.execute("DROP TABLE _fleet_sync_migrations").await?;
    let owner = timeout(
        Duration::from_secs(2),
        store.object(
            key,
            ObjectOptions {
                foreign_keys: false,
            },
            host(),
        ),
    )
    .await??;
    assert!(!owner.options.foreign_keys);
    assert_eq!(store.initialization_connections.borrow().1, 2);
    assert_eq!(write_and_read(&owner).await?, 42);
    repair.close().await?;
    store.close().await?;
    Ok(())
}

#[tokio::test]
async fn dropping_the_initializer_lets_an_existing_waiter_retry() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = ObjectStore::open(directory.path()).await?;
    let key = ObjectKey::new("retry-cancelled")?;
    let options = ObjectOptions::default();
    let mut lock_holder = connection(directory.path(), &key).await?;
    lock_holder
        .execute("CREATE TABLE lock_witness(value INTEGER)")
        .await?;
    let held = lock_holder.begin_with("BEGIN IMMEDIATE").await?;
    let mut changes = store.initialization_connections.subscribe();
    let mut initializing = Box::pin(store.object(key.clone(), options, host()));
    wait_for_held_initialization(&mut initializing, &mut changes, &key).await?;
    let mut waiting = Box::pin(store.object(key.clone(), options, host()));
    assert!(polls_pending(waiting.as_mut()).await);

    drop(initializing);
    held.rollback().await?;
    let owner = timeout(Duration::from_secs(2), waiting).await??;
    let cached = store.object(key, options, host()).await?;
    assert!(Arc::ptr_eq(&owner, &cached));
    assert_eq!(store.initialization_connections.borrow().1, 2);
    assert_eq!(write_and_read(&owner).await?, 42);
    lock_holder.close().await?;
    store.close().await?;
    Ok(())
}
