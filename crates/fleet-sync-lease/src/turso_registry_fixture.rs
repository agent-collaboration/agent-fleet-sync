//! Real file-backed registry, copy and consumer-gate stores used by rule tests.
use crate::*;
use durable_steps::{
    AtomicStore, CallFailure, CallId, CallTarget, Engine, Host, OperationState, RequestKey, Task,
    Uuid, WakeFailure, storage::TransactionScope, turso_store::TursoStore,
};
use fleet_sync_protocol::*;
use runtime_primitives::{Clock, IdSource, WakeScheduler};
use serde::{Serialize, de::DeserializeOwned};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify};

static TEST_IDS: AtomicU64 = AtomicU64::new(10000);

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
pub(super) type TestStore = TursoStore<TestHost>;
pub(super) struct CopyScope<'a>(pub &'a mut TursoConnection);
impl TransactionScope for CopyScope<'_> {
    type Database = Turso;
    type Executor<'q>
        = &'q mut TursoConnection
    where
        Self: 'q;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
pub(super) struct TestHost {
    pub now: AtomicI64,
    pub copies: Mutex<BTreeMap<DatabaseId, TursoConnection>>,
    pub gate: TursoGate,
    pub copy_directory: PathBuf,
    pub with_schema: bool,
    pub lose_create: AtomicBool,
    pub release_unreachable: AtomicBool,
    pub lose_release: AtomicBool,
    pub release_answer: Mutex<Option<ReleaseAnswer>>,
    pub settlement_unreachable: AtomicBool,
    pub release_observed: Notify,
    pub acked_observed: Notify,
    pub hold_release: AtomicBool,
    pub hold_acked: AtomicBool,
    pub status_unreachable: AtomicBool,
    pub lose_reservation: AtomicBool,
    pub fence_unreachable: AtomicBool,
    pub lose_fence: AtomicBool,
    pub retire_unreachable: AtomicBool,
    pub lose_retire: AtomicBool,
    pub fence_gates: Mutex<BTreeMap<TransferId, Arc<crate::turso_registry_copy::FenceGate>>>,
    pub calls: Mutex<Vec<(String, CallId)>>,
}
pub(super) struct TursoGate {
    pub connection: Mutex<TursoConnection>,
    pub reject_registration: AtomicBool,
    pub reject_retirement: AtomicBool,
    pub reject_claim: AtomicBool,
    pub lose_settlement: AtomicBool,
}
impl Clock for TestHost {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.now.load(Ordering::SeqCst))
    }
}
impl IdSource for TestHost {
    fn new_id(&self) -> Uuid {
        Uuid::from_u128(u128::from(TEST_IDS.fetch_add(1, Ordering::SeqCst)))
    }
}
impl WakeScheduler for TestHost {
    async fn wake_at(&self, _: SyncTime) -> Result<(), WakeFailure> {
        Ok(())
    }
}
impl TestHost {
    pub async fn open(directory: &Path, with_schema: bool) -> TestResult<Arc<Self>> {
        let mut gate = TursoConnectOptions::new()
            .filename(directory.join("gate.db"))
            .create_if_missing(true)
            .connect()
            .await?;
        sqlx_core::raw_sql::raw_sql("CREATE TABLE IF NOT EXISTS calls(call TEXT PRIMARY KEY,request TEXT NOT NULL,answer TEXT NOT NULL); CREATE TABLE IF NOT EXISTS reservations(attempt TEXT PRIMARY KEY,change TEXT NOT NULL); CREATE TABLE IF NOT EXISTS settlements(attempt TEXT PRIMARY KEY,outcome TEXT NOT NULL); CREATE TABLE IF NOT EXISTS releases(call TEXT PRIMARY KEY,request TEXT NOT NULL,answer TEXT NOT NULL);").execute(&mut gate).await?;
        Ok(Arc::new(Self {
            now: AtomicI64::new(1000),
            copies: Mutex::new(BTreeMap::new()),
            gate: TursoGate {
                connection: Mutex::new(gate),
                reject_registration: AtomicBool::new(false),
                reject_retirement: AtomicBool::new(false),
                reject_claim: AtomicBool::new(false),
                lose_settlement: AtomicBool::new(false),
            },
            copy_directory: directory.to_path_buf(),
            with_schema,
            lose_create: AtomicBool::new(false),
            release_unreachable: AtomicBool::new(false),
            lose_release: AtomicBool::new(false),
            release_answer: Mutex::new(None),
            settlement_unreachable: AtomicBool::new(false),
            release_observed: Notify::new(),
            acked_observed: Notify::new(),
            hold_release: AtomicBool::new(false),
            hold_acked: AtomicBool::new(false),
            status_unreachable: AtomicBool::new(false),
            lose_reservation: AtomicBool::new(false),
            fence_unreachable: AtomicBool::new(false),
            lose_fence: AtomicBool::new(false),
            retire_unreachable: AtomicBool::new(false),
            lose_retire: AtomicBool::new(false),
            fence_gates: Mutex::new(BTreeMap::new()),
            calls: Mutex::new(Vec::new()),
        }))
    }
    pub fn advance(&self) {
        self.now.fetch_add(1_000_000, Ordering::SeqCst);
    }
    pub async fn copy_connection(&self, database: DatabaseId) -> TestResult<TursoConnection> {
        let mut connection = TursoConnectOptions::new()
            .filename(
                self.copy_directory
                    .join(format!("copy-{}.db", database.as_uuid())),
            )
            .create_if_missing(true)
            .foreign_keys(false)
            .connect()
            .await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut migrator = sqlx_core::migrate::Migrator::new(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../fleet-sync-receiver/migrations"
        )))
        .await?;
        migrator.dangerous_set_table_name("_fleet_sync_copy_migrations");
        migrator.run_direct(None, &mut *transaction, false).await?;
        transaction.commit().await?;
        Ok(connection)
    }
}
impl Host for TestHost {
    async fn dispatch<C: CallTarget>(
        &self,
        target: C,
        request: C::Request,
        call: CallId,
    ) -> Result<C::Answer, CallFailure> {
        self.calls.lock().await.push((C::KIND.into(), call.clone()));
        let target = serde_json::to_value(target).map_err(lost)?;
        let request = serde_json::to_value(request).map_err(lost)?;
        let answer = match C::KIND {
            "fleet-sync.registration-admission" => {
                let request: fleet_sync_protocol::RegistrationAdmission = decode(request)?;
                encode(
                    self.gate
                        .admit_registration(&request.database, &request.writer, call)
                        .await
                        .map_err(lost)?,
                )?
            }
            "fleet-sync.retirement-admission" => {
                let request: fleet_sync_protocol::RetirementAdmission = decode(request)?;
                encode(
                    self.gate
                        .admit_retirement(&request.database, call)
                        .await
                        .map_err(lost)?,
                )?
            }
            "fleet-sync.claim-reservation" => {
                let request: fleet_sync_protocol::ClaimReservation = decode(request)?;
                let answer = self
                    .gate
                    .reserve(&request.change, call)
                    .await
                    .map_err(lost)?;
                if self.lose_reservation.swap(false, Ordering::SeqCst) {
                    return Err(CallFailure::AnswerLost);
                }
                encode(answer)?
            }
            "fleet-sync.claim-settlement" => {
                if self.settlement_unreachable.load(Ordering::SeqCst) {
                    return Err(CallFailure::NotSent {
                        reason: durable_steps::NotSentReason::Unreachable { last_seen: None },
                    });
                }
                let request: fleet_sync_protocol::ClaimSettlement = decode(request)?;
                self.gate
                    .settle(&request.change, request.outcome, call)
                    .await
                    .map_err(lost)?;
                if self.gate.lose_settlement.swap(false, Ordering::SeqCst) {
                    return Err(CallFailure::AnswerLost);
                }
                encode(SettlementRecorded)?
            }
            _ => self.copy_dispatch(C::KIND, target, request, call).await?,
        };
        decode(answer)
    }
}
pub(super) fn lost(_: impl std::fmt::Display) -> CallFailure {
    CallFailure::AnswerLost
}
pub(super) fn decode<T: DeserializeOwned>(value: serde_json::Value) -> Result<T, CallFailure> {
    serde_json::from_value(value).map_err(lost)
}
pub(super) fn encode<T: Serialize>(value: T) -> Result<serde_json::Value, CallFailure> {
    serde_json::to_value(value).map_err(lost)
}
pub(super) fn storage(error: impl std::fmt::Display) -> durable_steps::StoreError {
    durable_steps::StoreError::backend(error)
}
pub(super) fn database() -> TestResult<DatabaseId> {
    Ok(DatabaseId::new(Uuid::from_u128(100))?)
}
pub(super) fn peer(value: u128) -> TestResult<PeerId> {
    Ok(PeerId::new(Uuid::from_u128(value))?)
}
pub(super) fn config() -> TestResult<RegistryConfig> {
    Ok(RegistryConfig {
        lease: LeasePolicy {
            lease_duration: Duration::from_millis(2000),
            renew_interval: Duration::from_millis(500),
        },
        authoritative_receiver: ReceiverId::new(Uuid::from_u128(101))?,
        target_schema_max: 10,
        release_unsent_for: Duration::from_millis(100),
    })
}
pub(super) fn registration(attempt: u128, writer: u128) -> TestResult<Registration> {
    Ok(Registration {
        attempt: RequestId::new(Uuid::from_u128(attempt))?,
        database: database()?,
        writer: peer(writer)?,
        origin: Origin::Empty,
    })
}
pub(super) fn import_request(attempt: u128, digest: &str) -> TestResult<Import> {
    Ok(Import {
        attempt: RequestId::new(Uuid::from_u128(attempt))?,
        database: database()?,
        digest: ImportDigest::new(digest.repeat(64))?,
    })
}
pub(super) async fn open_store(
    path: &Path,
    host: Arc<TestHost>,
    migrate: bool,
) -> TestResult<Arc<TestStore>> {
    let store = Arc::new(TestStore::open_and_migrate(path, host).await?);
    if migrate {
        store
            .transaction(|scope| {
                Box::pin(async move {
                    sqlx_core::raw_sql::raw_sql(include_str!(
                        "../migrations/0001_registry_rules.sql"
                    ))
                    .execute(scope.executor())
                    .await?;
                    Ok(())
                })
            })
            .await?;
    }
    Ok(store)
}
pub(super) fn engine(
    store: Arc<TestStore>,
    host: Arc<TestHost>,
) -> TestResult<Engine<TestStore, TestHost>> {
    let mut engine = Engine::new(store, host);
    engine.register(RegistrationTask::<TestStore, Turso>::new(config()?))?;
    engine.register(ImportTask::<TestStore, Turso>::new(config()?))?;
    engine.register(RetirementTask::<TestStore, Turso>::new(config()?))?;
    engine.register(PreviewTakeoverTask::<TestStore, Turso>::new(config()?))?;
    engine.register(HolderChangeTask::<TestStore, Turso>::new(config()?))?;
    engine.register(SettleReservationTask::<TestStore>::new())?;
    Ok(engine)
}
pub(super) async fn task_answer<T>(
    engine: &Engine<TestStore, TestHost>,
    key: &RequestKey,
    input: T::Input,
) -> TestResult<T::Output>
where
    T: Task<Store = TestStore>,
    T::Output: DeserializeOwned,
{
    let id = engine.start::<T>(key, input).await?;
    match engine.status(&id).await?.state {
        OperationState::Completed { result } => Ok(serde_json::from_slice(&result)?),
        state => Err(format!("task did not complete: {state:?}").into()),
    }
}
pub(super) async fn scalar(store: &TestStore, sql: &'static str) -> TestResult<String> {
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<Turso>(sql)
                    .fetch_one(scope.executor())
                    .await?;
                Ok(row.try_get("value")?)
            })
        })
        .await?)
}

pub(super) struct VersionedTask<T, const VERSION: u32>(pub T);
impl<T: Task, const VERSION: u32> Task for VersionedTask<T, VERSION> {
    const KIND: &'static str = T::KIND;
    const VERSION: u32 = VERSION;
    const KEEP_RESULT: bool = T::KEEP_RESULT;
    type Store = T::Store;
    type Input = T::Input;
    type Output = T::Output;
    async fn run<C: durable_steps::OperationContext<Store = Self::Store>>(
        &self,
        ctx: &mut C,
        input: &Self::Input,
    ) -> Result<Self::Output, durable_steps::OperationFailure> {
        self.0.run(ctx, input).await
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut <Self::Store as AtomicStore>::Scope<'t>,
        key: &'s RequestKey,
        failure: &'s durable_steps::OperationFailure,
    ) -> durable_steps::BoxFuture<'s, Result<(), durable_steps::StoreError>> {
        self.0.on_failed(scope, key, failure)
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut <Self::Store as AtomicStore>::Scope<'t>,
        key: &'s RequestKey,
    ) -> durable_steps::BoxFuture<
        's,
        Result<durable_steps::Resume<Self::Output>, durable_steps::StoreError>,
    > {
        self.0.before_resume(scope, key)
    }
}
