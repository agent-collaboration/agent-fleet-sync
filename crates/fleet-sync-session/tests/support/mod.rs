#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Default)]
pub struct MemoryScope {
    pub sessions: BTreeMap<SessionKey, SessionRecord>,
    pub registrations: BTreeMap<ConnectionId, ConnectionRegistration>,
    pub statuses: Vec<DatabaseStatus>,
    notices: BTreeMap<(SessionKey, NoticePosition), Vec<u8>>,
}
impl SessionScope for MemoryScope {
    async fn load_session(
        &mut self,
        key: &SessionKey,
    ) -> Result<Option<SessionRecord>, StoreError> {
        Ok(self.sessions.get(key).cloned())
    }
    async fn save_session(&mut self, record: &SessionRecord) -> Result<(), StoreError> {
        self.sessions.insert(record.key(), record.clone());
        Ok(())
    }
    async fn sessions_for_peer(&mut self, peer: &PeerId) -> Result<Vec<SessionRecord>, StoreError> {
        Ok(self
            .sessions
            .values()
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
        _peer: &PeerId,
    ) -> Result<Vec<DatabaseStatus>, StoreError> {
        Ok(self.statuses.clone())
    }
    async fn save_notice(
        &mut self,
        key: &SessionKey,
        notice: &StoredNotice<NoConsumer>,
    ) -> Result<(), StoreError> {
        self.notices.insert(
            (*key, notice.position),
            serde_json::to_vec(&notice.body).unwrap(),
        );
        Ok(())
    }
    async fn notices_after(
        &mut self,
        key: &SessionKey,
        after: NoticePosition,
    ) -> Result<Vec<StoredNotice<NoConsumer>>, StoreError> {
        Ok(self
            .notices
            .iter()
            .filter(|((session, position), _)| session == key && *position > after)
            .map(|((_, position), body)| StoredNotice {
                position: *position,
                body: serde_json::from_slice(body).unwrap(),
            })
            .collect())
    }
}
pub fn now(value: i64) -> SyncTime {
    SyncTime::from_unix_millis(value)
}
pub fn session(value: u128) -> SessionId {
    SessionId::new(Uuid::from_u128(value)).unwrap()
}
pub fn binding(value: u128) -> ConnectionBinding {
    ConnectionBinding::new(
        ConnectionId::new(Uuid::from_u128(value)).unwrap(),
        VerifiedPeer {
            peer: PeerId::new(Uuid::from_u128(1)).unwrap(),
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
    scope: &mut MemoryScope,
    binding: &ConnectionBinding,
    id: SessionId,
) -> Vec<SessionEffect> {
    connection_opened(scope, binding, now(1)).await.unwrap();
    frame_received(scope, binding, &hello(id, 1), now(1))
        .await
        .unwrap()
}
