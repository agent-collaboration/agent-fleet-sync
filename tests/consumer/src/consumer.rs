use crate::{
    ConsumerStore, HookEffect, SampleAnswer, SampleProtocol, SampleRequest, checked_queries,
};
use durable_steps::CallId;
use fleet_sync_lease::{GateFailure, HolderChangeGate, LifecycleGate};
use fleet_sync_protocol::*;
use fleet_sync_receiver::{
    AppliedPiece, FollowUpDone, FollowUpFailure, InstalledSnapshot, ReceiverHook,
};
use fleet_sync_service::{ConsumerHandler, ServiceConsumer, ServiceError, ServiceScope};
use fleet_sync_session::{PeerAuthenticator, VerifiedPeer};
use runtime_primitives::{Clock, SystemClock};
use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

pub struct Consumer {
    pub store: ConsumerStore,
    pub hook_failure: AtomicBool,
    pub lose_follow_up_answer: AtomicBool,
    hook_code: HookRefusalCode,
    pub handled_requests: AtomicU64,
}
impl Consumer {
    pub fn new(store: ConsumerStore) -> Result<Self, TextValueError> {
        Ok(Self {
            store,
            hook_failure: AtomicBool::new(false),
            lose_follow_up_answer: AtomicBool::new(false),
            hook_code: HookRefusalCode::new("consumer_hook")?,
            handled_requests: AtomicU64::new(0),
        })
    }
    async fn record_hook(
        &self,
        scope: &mut ServiceScope<'_>,
        database: DatabaseId,
        position: ReplicationPosition,
        annotation: String,
    ) -> Result<Vec<HookEffect>, HookRefusal> {
        let result: crate::TestResult<Vec<HookEffect>> = async {
            let event_id = format!(
                "{}:{}:{}:{}",
                database.as_uuid(),
                position.epoch.value(),
                position.sequence.value(),
                annotation
            );
            checked_queries::insert_hook_event(scope, event_id, annotation.clone()).await?;
            let count = checked_queries::count_hook_events(scope).await?;
            if count.count_class.as_deref() != Some("integer") {
                return Err("hook count has wrong storage class".into());
            }
            let event_count = count.event_count.ok_or("hook count missing")?.parse()?;
            if self.hook_failure.load(Ordering::SeqCst) {
                return Err("consumer refused after local hook write".into());
            }
            Ok(vec![HookEffect {
                database,
                position,
                event_count,
                annotation,
            }])
        }
        .await;
        result.map_err(|error| HookRefusal {
            code: self.hook_code.clone(),
            detail: error.to_string(),
        })
    }
}
impl ConsumerHandler<SampleProtocol> for Consumer {
    async fn request(
        &self,
        peer: PeerId,
        request: SampleRequest,
    ) -> Result<SampleAnswer, ServiceError> {
        self.handled_requests.fetch_add(1, Ordering::SeqCst);
        match request {
            SampleRequest::Echo { text } => Ok(SampleAnswer::Echo { peer, text }),
        }
    }
}
impl ServiceConsumer<SampleProtocol> for Consumer {
    type HookItem = HookEffect;
}
impl ReceiverHook<ServiceScope<'_>> for Consumer {
    type Protocol = SampleProtocol;
    type FollowUp = HookEffect;
    fn local_migrations(&self) -> &'static [sqlx_core::migrate::Migration] {
        static LOCAL: sqlx_core::migrate::Migrator = sqlx::migrate!("./hook-migrations");
        LOCAL.iter().as_slice()
    }
    async fn on_applied(
        &self,
        scope: &mut ServiceScope<'_>,
        applied: &AppliedPiece<'_, String>,
    ) -> Result<Vec<HookEffect>, HookRefusal> {
        self.record_hook(
            scope,
            applied.database,
            applied.through,
            applied.annotation.cloned().unwrap_or_default(),
        )
        .await
    }
    async fn on_snapshot_installed(
        &self,
        scope: &mut ServiceScope<'_>,
        installed: &InstalledSnapshot,
    ) -> Result<Vec<HookEffect>, HookRefusal> {
        self.record_hook(
            scope,
            installed.database,
            installed.position,
            "snapshot".into(),
        )
        .await
    }
    async fn follow_up(
        &self,
        item: &HookEffect,
        call: CallId,
    ) -> Result<FollowUpDone, FollowUpFailure> {
        self.store
            .save_effect(call, item.clone())
            .await
            .map_err(|_| FollowUpFailure::Retry)?;
        if self.lose_follow_up_answer.swap(false, Ordering::SeqCst) {
            // The external effect committed. The journey kills this process
            // while the actual durable call remains unanswered.
            std::future::pending::<()>().await;
        }
        Ok(FollowUpDone)
    }
}
impl LifecycleGate for Consumer {
    async fn admit_registration(
        &self,
        _: &DatabaseId,
        _: &PeerId,
        _: CallId,
    ) -> Result<GateAnswer, GateFailure> {
        Ok(GateAnswer::Admitted)
    }
    async fn admit_retirement(&self, _: &DatabaseId, _: CallId) -> Result<GateAnswer, GateFailure> {
        Ok(GateAnswer::Admitted)
    }
}
impl HolderChangeGate for Consumer {
    async fn reserve(&self, _: &HolderChangeRequest, _: CallId) -> Result<GateAnswer, GateFailure> {
        Ok(GateAnswer::Reserved)
    }
    async fn settle(
        &self,
        _: &HolderChangeRequest,
        _: SettleOutcome,
        _: CallId,
    ) -> Result<(), GateFailure> {
        Ok(())
    }
}
pub struct FixtureAuthenticator;
impl PeerAuthenticator for FixtureAuthenticator {
    type Material = String;
    async fn verify(&self, material: String) -> Result<VerifiedPeer, AuthRefusal> {
        let peer = material
            .strip_prefix("Bearer peer-")
            .and_then(|value| value.parse::<u128>().ok())
            .and_then(|value| PeerId::new(durable_steps::Uuid::from_u128(value)).ok())
            .ok_or(AuthRefusal::Unknown)?;
        Ok(VerifiedPeer {
            peer,
            expires_at: SystemClock.now().after(Duration::from_secs(3600)),
        })
    }
}
