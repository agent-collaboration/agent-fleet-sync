use super::session_fixture_contract::*;
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use sqlx_turso::{
    Turso, TursoConnectOptions, TursoConnection,
    sqlx::{ConnectOptions, Connection},
};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::sync::Mutex;

static FIXTURE_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./tests/fixtures/migrations");

pub async fn open_and_migrate(path: &Path) -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    {
        let mut scope = FixtureScope {
            transaction: &mut transaction,
            state: FixtureState::default(),
            registrations: BTreeMap::new(),
            publications: Vec::new(),
            effects: Vec::new(),
        };
        fleet_sync_session::validate_legacy_rows(&mut scope).await?;
    }
    let migrations = FIXTURE_MIGRATOR
        .iter()
        .chain(SESSION_MIGRATOR.sql_migrations())
        .cloned()
        .collect();
    sqlx::migrate::Migrator::with_migrations(migrations)
        .run_direct(None, &mut *transaction, false)
        .await?;
    transaction.commit().await?;
    Ok(connection)
}

struct FixtureRow {
    record: String,
}
pub async fn read_state(connection: &mut TursoConnection) -> TestResult<FixtureState> {
    let row = sqlx_turso::query_as!(
        FixtureRow,
        r#"SELECT record AS "record!: String" FROM session_fixture_state WHERE singleton = 1"#
    )
    .fetch_optional(connection)
    .await?;
    Ok(match row {
        Some(row) => serde_json::from_str(&row.record)?,
        None => FixtureState::default(),
    })
}
pub async fn write_state(connection: &mut TursoConnection, state: &FixtureState) -> TestResult {
    let record = serde_json::to_string(state)?;
    sqlx_turso::query!("INSERT INTO session_fixture_state(singleton, record) VALUES (1, ?) ON CONFLICT(singleton) DO UPDATE SET record=excluded.record", record).execute(connection).await?;
    Ok(())
}

pub struct FixtureScope<'a> {
    pub transaction: &'a mut TursoConnection,
    pub state: FixtureState,
    pub registrations: BTreeMap<ConnectionId, ConnectionRegistration>,
    pub publications: Vec<PublishSchemaSupportInput>,
    pub effects: Vec<SessionEffect<FixtureProtocol>>,
}
fn store_error(error: impl std::fmt::Display) -> StoreError {
    StoreError::Unavailable {
        message: error.to_string(),
    }
}
impl TransactionScope for FixtureScope<'_> {
    type Database = Turso;
    type Executor<'query>
        = &'query mut TursoConnection
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.transaction
    }
}
impl SessionScope for FixtureScope<'_> {
    async fn publish_schema_support(
        &mut self,
        input: PublishSchemaSupportInput,
        _: SyncTime,
    ) -> Result<(), StoreError> {
        self.publications.push(input);
        Ok(())
    }
    fn welcome_after_commit(
        &mut self,
        connection: ConnectionId,
        peer: PeerId,
        session_id: SessionId,
    ) {
        self.effects.push(SessionEffect::Welcome {
            connection,
            peer,
            session_id,
        });
    }
    fn registration(&self, connection: &ConnectionId) -> Option<ConnectionRegistration> {
        self.registrations.get(connection).copied()
    }
    fn set_registration(
        &mut self,
        connection: &ConnectionId,
        registration: ConnectionRegistration,
    ) {
        self.registrations.insert(*connection, registration);
    }
}

pub async fn read_sessions(connection: &mut TursoConnection) -> TestResult<Vec<SessionRecord>> {
    use sqlx::Row;
    let rows=sqlx::query::<Turso>("SELECT peer,session_id,state_kind,connection_id,detached_since,end_reason,notice_head FROM _fleet_sync_sessions ORDER BY peer,session_id").fetch_all(connection).await?;
    rows.into_iter()
        .map(|row| {
            let state = match row.try_get::<String, _>("state_kind")?.as_str() {
                "active" => SessionState::Active {
                    connection: ConnectionId::try_from(row.try_get::<String, _>("connection_id")?)?,
                },
                "detached" => SessionState::Detached {
                    since: SyncTime::from_unix_millis(row.try_get::<i64, _>("detached_since")?),
                },
                "ended" => SessionState::Ended {
                    reason: match row.try_get::<String, _>("end_reason")?.as_str() {
                        "newSession" => EndReason::NewSession,
                        "peerRevoked" => EndReason::PeerRevoked,
                        _ => return Err("unknown fixture session end reason".into()),
                    },
                },
                _ => return Err("unknown fixture session state".into()),
            };
            Ok(SessionRecord {
                peer: PeerId::try_from(row.try_get::<String, _>("peer")?)?,
                session_id: SessionId::try_from(row.try_get::<String, _>("session_id")?)?,
                state,
                notice_head: NoticePosition::new(row.try_get::<String, _>("notice_head")?.parse()?),
            })
        })
        .collect()
}

#[derive(Clone)]
pub struct TursoClientStore(Arc<Mutex<TursoConnection>>);
impl TursoClientStore {
    pub async fn open_and_migrate(path: &Path) -> TestResult<Self> {
        Ok(Self(Arc::new(Mutex::new(open_and_migrate(path).await?))))
    }
}
struct ClientRow {
    record: String,
}
impl SessionClientStore for TursoClientStore {
    async fn load(&self) -> Result<Option<ClientSessionState>, StoreError> {
        let mut connection = self.0.lock().await;
        let row = sqlx_turso::query_as!(
            ClientRow,
            r#"SELECT record AS "record!: String" FROM client_fixture_state WHERE singleton = 1"#
        )
        .fetch_optional(&mut *connection)
        .await
        .map_err(store_error)?;
        row.map(|row| {
            serde_json::from_str(&row.record).map_err(|_| StoreError::Corrupt {
                field: "client session state".into(),
            })
        })
        .transpose()
    }
    async fn save(&self, state: &ClientSessionState) -> Result<(), StoreError> {
        let mut connection = self.0.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(store_error)?;
        let record = serde_json::to_string(state).map_err(store_error)?;
        sqlx_turso::query!("INSERT INTO client_fixture_state(singleton, record) VALUES (1, ?) ON CONFLICT(singleton) DO UPDATE SET record=excluded.record", record).execute(&mut *transaction).await.map_err(store_error)?;
        transaction.commit().await.map_err(store_error)
    }
}
