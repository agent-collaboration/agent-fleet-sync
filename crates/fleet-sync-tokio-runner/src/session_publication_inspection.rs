//! Native-only observation seams for the real session publication adapter.
use crate::{SessionHandler, SessionHost, SessionHostError};
use durable_steps::{StoreError, storage::TransactionScope};
use fleet_sync_protocol::*;
use fleet_sync_session::{ConnectionRegistration, PeerAuthenticator, schema_support_request_key};
use serde::{Deserialize, Serialize};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SchemaPublicationSnapshot {
    pub registrations: Vec<(ConnectionId, ConnectionRegistration)>,
    pub attach_steps: u64,
}
impl<P: ConsumerProtocol, A: PeerAuthenticator, D: SessionHandler<P>, H: durable_steps::Host>
    SessionHost<P, A, D, H>
{
    pub fn fail_next_schema_finalize(&self, peer: PeerId) -> Result<(), StoreError> {
        self.store.finalize_probe.arm(peer)
    }
    pub async fn wait_schema_finalize_rollback(&self) -> Result<PeerId, StoreError> {
        self.store.finalize_probe.wait_rollback().await
    }
    pub fn release_schema_finalize(&self) {
        self.store.finalize_probe.release();
    }

    pub async fn schema_publication_snapshot(
        &self,
        report: &ReportSchemaSupport,
    ) -> Result<SchemaPublicationSnapshot, SessionHostError> {
        let key = schema_support_request_key(report)?.as_str().to_owned();
        let attach_steps = self.object.rule_transaction(move |scope| Box::pin(async move {
            let row = query::<Turso>("SELECT CAST(count(*) AS TEXT) AS count FROM _fleet_sync_steps s JOIN _fleet_sync_operations o ON s.operation_id=o.operation_id WHERE o.kind='fleet-sync.publish-schema-support' AND o.request_key=?1 AND s.step_name='attach'")
                .bind(key).fetch_one(scope.executor()).await.map_err(StoreError::backend)?;
            row.try_get::<String, _>("count").map_err(StoreError::backend)?.parse::<u64>().map_err(StoreError::backend)
        })).await?;
        let runtime = self.runtime.lock().await;
        // Include orphaned registrations: filtering through live sockets would hide resurrection.
        let registrations = runtime
            .registrations
            .iter()
            .map(|(connection, registration)| (*connection, *registration))
            .collect();
        Ok(SchemaPublicationSnapshot {
            registrations,
            attach_steps,
        })
    }
    pub async fn wait_schema_connection_closed(
        &self,
        connection: ConnectionId,
    ) -> Result<(), SessionHostError> {
        let mut changed = self.registration_changes.subscribe();
        loop {
            let owned = {
                let runtime = self.runtime.lock().await;
                runtime.sockets.contains_key(&connection)
                    || runtime.registrations.contains_key(&connection)
            };
            if !owned {
                return Ok(());
            }
            changed.changed().await.map_err(SessionHostError::rule)?;
        }
    }
    pub async fn wait_schema_publication(
        &self,
        report: &ReportSchemaSupport,
    ) -> Result<(), SessionHostError> {
        let key = schema_support_request_key(report)?.as_str().to_owned();
        let mut changed = self.object.subscribe_commits();
        loop {
            let lookup = key.clone();
            let state = self.object.rule_transaction(move |scope| Box::pin(async move {
                query::<Turso>("SELECT state FROM _fleet_sync_operations WHERE kind='fleet-sync.publish-schema-support' AND request_key=?1")
                    .bind(lookup).fetch_optional(scope.executor()).await.map_err(StoreError::backend)?
                    .map(|row| row.try_get::<String, _>("state").map_err(StoreError::backend)).transpose()
            })).await?;
            match state.as_deref() {
                Some("completed") => return Ok(()),
                Some("failed") => return Err(SessionHostError::rule("schema publication failed")),
                _ => changed.changed().await.map_err(SessionHostError::rule)?,
            }
        }
    }
}
