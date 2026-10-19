//! Test-only holds and observations at the native session-to-registry boundary.
use crate::{ServiceConsumer, ServiceError, ServiceHandle};
use durable_steps::{StoreError, storage::TransactionScope};
use fleet_sync_protocol::*;
use fleet_sync_session::PeerAuthenticator;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard, watch};

struct Hold {
    peer: PeerId,
    sequence: AttachSequence,
    observed: watch::Sender<Option<ReportSchemaSupport>>,
    released: watch::Sender<bool>,
}
#[derive(Default)]
pub(crate) struct SchemaSupportProbe {
    held: Mutex<Option<Arc<Hold>>>,
}
impl SchemaSupportProbe {
    async fn arm(&self, peer: PeerId, sequence: AttachSequence) -> Result<(), ServiceError> {
        let mut held = self.held.lock().await;
        if held.is_some() {
            return Err(ServiceError::rule("schema report already held"));
        }
        *held = Some(Arc::new(Hold {
            peer,
            sequence,
            observed: watch::channel(None).0,
            released: watch::channel(false).0,
        }));
        Ok(())
    }
    pub async fn before_report(&self, report: &ReportSchemaSupport) {
        let hold = self
            .held
            .lock()
            .await
            .clone()
            .filter(|hold| hold.peer == report.peer && hold.sequence == report.attach_seq);
        if let Some(hold) = hold {
            let mut release = hold.released.subscribe();
            hold.observed.send_replace(Some(report.clone()));
            while !*release.borrow() {
                if release.changed().await.is_err() {
                    break;
                }
            }
        }
    }
    async fn wait(&self) -> Result<ReportSchemaSupport, ServiceError> {
        let held = self
            .held
            .lock()
            .await
            .clone()
            .ok_or_else(|| ServiceError::rule("no schema report hold"))?;
        let mut observed = held.observed.subscribe();
        loop {
            if let Some(report) = observed.borrow().clone() {
                return Ok(report);
            }
            observed.changed().await.map_err(ServiceError::rule)?;
        }
    }
    async fn release(&self) {
        if let Some(held) = self.held.lock().await.take() {
            held.released.send_replace(true);
        }
    }
}
impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> ServiceHandle<P, C, A>
{
    pub async fn hold_schema_report(
        &self,
        peer: PeerId,
        sequence: AttachSequence,
    ) -> Result<(), ServiceError> {
        self.state.schema_support_probe.arm(peer, sequence).await
    }
    pub async fn wait_schema_report(&self) -> Result<ReportSchemaSupport, ServiceError> {
        self.state.schema_support_probe.wait().await
    }
    pub async fn release_schema_report(&self) {
        self.state.schema_support_probe.release().await;
    }
    pub async fn accepted_schema_support(
        &self,
        peer: PeerId,
    ) -> Result<Option<ReportSchemaSupport>, ServiceError> {
        self.state
            .registry
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    fleet_sync_lease::accepted_schema_support(scope, peer)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await
            .map_err(Into::into)
    }
    pub async fn hold_registry_drive(&self) -> OwnedMutexGuard<()> {
        self.state.registry.engine_lock().lock_owned().await
    }
    pub fn fail_next_schema_finalize(&self, peer: PeerId) -> Result<(), ServiceError> {
        self.sessions
            .fail_next_schema_finalize(peer)
            .map_err(Into::into)
    }
    pub async fn wait_schema_finalize_rollback(&self) -> Result<PeerId, ServiceError> {
        self.sessions
            .wait_schema_finalize_rollback()
            .await
            .map_err(Into::into)
    }
    pub fn release_schema_finalize(&self) {
        self.sessions.release_schema_finalize();
    }
    pub async fn schema_publication_snapshot(
        &self,
        report: &ReportSchemaSupport,
    ) -> Result<fleet_sync_tokio_runner::SchemaPublicationSnapshot, ServiceError> {
        self.sessions
            .schema_publication_snapshot(report)
            .await
            .map_err(ServiceError::rule)
    }
    pub async fn wait_schema_connection_closed(
        &self,
        connection: ConnectionId,
    ) -> Result<(), ServiceError> {
        self.sessions
            .wait_schema_connection_closed(connection)
            .await
            .map_err(ServiceError::rule)
    }
    pub async fn wait_schema_publication(
        &self,
        report: &ReportSchemaSupport,
    ) -> Result<(), ServiceError> {
        self.sessions
            .wait_schema_publication(report)
            .await
            .map_err(ServiceError::rule)
    }
    pub async fn admitted_schema_for_test(
        &self,
        attempt: TransferId,
    ) -> Result<Option<SchemaSupport>, ServiceError> {
        self.state.registry.rule_transaction(move |scope| Box::pin(async move {
            let row = query::<Turso>("SELECT target_schema FROM _fleet_sync_transfer_admissions WHERE attempt=?1").bind(attempt.to_string()).fetch_one(scope.executor()).await.map_err(StoreError::backend)?;
            serde_json::from_str(&row.try_get::<String, _>("target_schema").map_err(StoreError::backend)?).map_err(StoreError::backend)
        })).await.map_err(Into::into)
    }
}
