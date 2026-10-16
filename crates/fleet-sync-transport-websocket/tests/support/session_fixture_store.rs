use super::session_fixture_contract::*;
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use sqlx_turso::{
    TursoConnectOptions, TursoConnection,
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
    FIXTURE_MIGRATOR
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
}
fn store_error(error: impl std::fmt::Display) -> StoreError {
    StoreError::Unavailable {
        message: error.to_string(),
    }
}
impl SessionScope<FixtureProtocol> for FixtureScope<'_> {
    async fn load_session(
        &mut self,
        key: &SessionKey,
    ) -> Result<Option<SessionRecord>, StoreError> {
        Ok(self
            .state
            .sessions
            .iter()
            .find(|record| record.key() == *key)
            .cloned())
    }
    async fn save_session(&mut self, record: &SessionRecord) -> Result<(), StoreError> {
        match self
            .state
            .sessions
            .iter_mut()
            .find(|stored| stored.key() == record.key())
        {
            Some(stored) => *stored = record.clone(),
            None => self.state.sessions.push(record.clone()),
        }
        write_state(self.transaction, &self.state)
            .await
            .map_err(store_error)
    }
    async fn sessions_for_peer(&mut self, peer: &PeerId) -> Result<Vec<SessionRecord>, StoreError> {
        Ok(self
            .state
            .sessions
            .iter()
            .filter(|record| record.peer == *peer)
            .cloned()
            .collect())
    }
    async fn registration(
        &mut self,
        connection: ConnectionId,
    ) -> Result<Option<ConnectionRegistration>, StoreError> {
        Ok(self.registrations.get(&connection).copied())
    }
    async fn save_registration(
        &mut self,
        connection: ConnectionId,
        registration: ConnectionRegistration,
    ) -> Result<(), StoreError> {
        self.registrations.insert(connection, registration);
        Ok(())
    }
    async fn database_statuses(
        &mut self,
        peer: &PeerId,
    ) -> Result<Vec<DatabaseStatus>, StoreError> {
        let database = DatabaseId::new(uuid::Uuid::from_u128(10)).map_err(store_error)?;
        let epoch = Epoch::new(1).map_err(store_error)?;
        let remaining_ms = u64::try_from(
            self.state
                .lease_expires_at
                .unix_millis()
                .saturating_sub(self.state.now.unix_millis())
                .max(0),
        )
        .map_err(store_error)?;
        Ok(vec![DatabaseStatus::Ready {
            database,
            acked: ReplicationPosition::new(epoch, CommitSequence::new(self.state.value)),
            lease: LeaseStatus {
                database,
                epoch,
                holder: *peer,
                state: if remaining_ms == 0 {
                    LeaseState::Expired
                } else {
                    LeaseState::Held
                },
                expires_at: self.state.lease_expires_at,
                remaining_ms,
            },
        }])
    }
    async fn save_notice(
        &mut self,
        session: &SessionKey,
        notice: &StoredNotice<FixtureProtocol>,
    ) -> Result<(), StoreError> {
        self.state.notices.push((
            session.peer,
            session.session_id,
            notice.position,
            serde_json::to_vec(&notice.body).map_err(store_error)?,
        ));
        write_state(self.transaction, &self.state)
            .await
            .map_err(store_error)
    }
    async fn notices_after(
        &mut self,
        session: &SessionKey,
        after: NoticePosition,
    ) -> Result<Vec<StoredNotice<FixtureProtocol>>, StoreError> {
        self.state
            .notices
            .iter()
            .filter(|(peer, id, position, _)| {
                *peer == session.peer && *id == session.session_id && *position > after
            })
            .map(|(_, _, position, body)| {
                Ok(StoredNotice {
                    position: *position,
                    body: serde_json::from_slice(body).map_err(store_error)?,
                })
            })
            .collect()
    }
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
