#![allow(clippy::unwrap_used)]
use crate::*;
use fleet_sync_protocol::*;
use sqlx_core::{
    pool::{Pool, PoolOptions},
    transaction::Transaction,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::collections::BTreeMap;
use uuid::Uuid;

pub struct SqlScope {
    pub pool: Pool<Turso>,
    pub transaction: Transaction<'static, Turso>,
    pub registrations: BTreeMap<ConnectionId, ConnectionRegistration>,
}
impl SqlScope {
    pub async fn new() -> Self {
        let pool = PoolOptions::<Turso>::new()
            .max_connections(1)
            .connect_with(TursoConnectOptions::new())
            .await
            .unwrap();
        let mut transaction = pool.begin().await.unwrap();
        SESSION_MIGRATOR
            .run_direct(None, &mut *transaction, false)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let transaction = pool.begin().await.unwrap();
        Self {
            pool,
            transaction,
            registrations: BTreeMap::new(),
        }
    }
    pub async fn record(&mut self, key: &SessionKey) -> SessionRecord {
        crate::session_storage::load_session(self, key)
            .await
            .unwrap()
            .unwrap()
    }
    pub async fn records(&mut self) -> Vec<SessionRecord> {
        crate::session_storage::sessions_for_peer(self, &binding(2).peer)
            .await
            .unwrap()
    }
}
impl TransactionScope for SqlScope {
    type Database = Turso;
    type Executor<'query>
        = &'query mut TursoConnection
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        &mut self.transaction
    }
}
impl SessionScope for SqlScope {
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
pub fn now(value: i64) -> SyncTime {
    SyncTime::from_unix_millis(value)
}
pub fn session(value: u128) -> SessionId {
    SessionId::new(Uuid::from_u128(
        (value) | 0x019eef00000070008000000000000000,
    ))
    .unwrap()
}
pub fn binding(value: u128) -> ConnectionBinding {
    ConnectionBinding::new(
        ConnectionId::new(Uuid::from_u128(
            (value) | 0x019eef00000070008000000000000000,
        ))
        .unwrap(),
        VerifiedPeer {
            peer: PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000001)).unwrap(),
            expires_at: now(100),
        },
    )
}
pub fn hello(id: SessionId, max_version: u64) -> Vec<u8> {
    serde_json::to_vec(&ClientFrame::<NoConsumer>::Hello {
        session_id: id,
        schema_support: SchemaSupport { max_version },
    })
    .unwrap()
}
pub async fn attach(
    scope: &mut SqlScope,
    binding: &ConnectionBinding,
    id: SessionId,
) -> Vec<SessionEffect> {
    connection_opened::<_, NoConsumer>(scope, binding, now(1))
        .await
        .unwrap();
    frame_received(scope, binding, &hello(id, 1), now(1))
        .await
        .unwrap()
}
