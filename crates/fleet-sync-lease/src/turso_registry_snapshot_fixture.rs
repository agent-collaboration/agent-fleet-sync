//! Public snapshot observations and actual transaction rollback injection.
use crate::{turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, BoxFuture, Engine, StoreError};
use sqlx_turso::Turso;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

pub(super) async fn snapshot(store: &TestStore) -> TestResult<Option<RegistrySnapshot>> {
    let database = database()?;
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move { registry_snapshot(scope, database).await.map_err(storage) })
        })
        .await?)
}

pub(super) async fn required_snapshot(store: &TestStore) -> TestResult<RegistrySnapshot> {
    snapshot(store)
        .await?
        .ok_or_else(|| "missing registry snapshot".into())
}

pub(super) async fn renew_at(
    store: &TestStore,
    id: u128,
    at: i64,
    duration: u64,
) -> TestResult<RenewAnswer> {
    let request = RenewRequest {
        attempt: import_request(id, "a")?.attempt,
        database: database()?,
        epoch: Epoch::new(1)?,
    };
    let holder = peer(1)?;
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                renew(
                    scope,
                    &request,
                    &holder,
                    SyncTime::from_unix_millis(at),
                    &LeasePolicy {
                        lease_duration: Duration::from_millis(duration),
                        renew_interval: Duration::from_millis(1),
                    },
                )
                .await
                .map_err(storage)
            })
        })
        .await?)
}

pub(super) fn assert_revision(snapshot: &RegistrySnapshot, epoch: u64, change: u64, at: i64) {
    assert_eq!(snapshot.revision.epoch.value(), epoch);
    assert_eq!(snapshot.revision.change.get(), change);
    assert_eq!(snapshot.changed_at, SyncTime::from_unix_millis(at));
}

pub(super) struct RollbackRegistryStore {
    pub inner: Arc<TestStore>,
    pub revision: RegistryRevision,
    pub fail: Arc<AtomicBool>,
    pub rollbacks: Arc<AtomicUsize>,
}
impl AtomicStore for RollbackRegistryStore {
    type Scope<'t> = <TestStore as AtomicStore>::Scope<'t>;
    async fn transaction<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'s, 't> FnOnce(&'s mut Self::Scope<'t>) -> BoxFuture<'s, Result<T, StoreError>>
            + Send
            + 'static,
    {
        let fail = Arc::clone(&self.fail);
        let rollbacks = Arc::clone(&self.rollbacks);
        let revision = self.revision;
        let database = database().map_err(storage)?;
        self.inner
            .transaction(move |scope| {
                Box::pin(async move {
                    let answer = run(scope).await?;
                    if fail.load(Ordering::SeqCst)
                        && registry_snapshot(scope, database)
                            .await
                            .map_err(storage)?
                            .is_some_and(|snapshot| snapshot.revision == revision)
                    {
                        rollbacks.fetch_add(1, Ordering::SeqCst);
                        return Err(StoreError::Aborted {
                            reason: "injected after real registry mutation before commit".into(),
                        });
                    }
                    Ok(answer)
                })
            })
            .await
    }
}

pub(super) fn rollback_store(
    inner: Arc<TestStore>,
    epoch: u64,
    change: u64,
) -> TestResult<Arc<RollbackRegistryStore>> {
    Ok(Arc::new(RollbackRegistryStore {
        inner,
        revision: RegistryRevision {
            epoch: Epoch::new(epoch)?,
            change: std::num::NonZeroU64::new(change).ok_or("zero rollback revision")?,
        },
        fail: Arc::new(AtomicBool::new(true)),
        rollbacks: Arc::new(AtomicUsize::new(0)),
    }))
}

pub(super) fn rollback_engine(
    store: Arc<RollbackRegistryStore>,
    host: Arc<TestHost>,
) -> TestResult<Engine<RollbackRegistryStore, TestHost>> {
    let mut engine = Engine::new(store, Arc::clone(&host));
    engine.register(RegistrationTask::<RollbackRegistryStore, Turso>::new(
        config()?,
    ))?;
    engine.register(ImportTask::<RollbackRegistryStore, Turso>::new(config()?))?;
    engine.register(RetirementTask::<RollbackRegistryStore, Turso>::new(
        config()?
    ))?;
    engine.register(HolderChangeTask::<RollbackRegistryStore, Turso>::new(
        config()?,
        host,
    ))?;
    engine.register(SettleReservationTask::<RollbackRegistryStore>::new())?;
    Ok(engine)
}
