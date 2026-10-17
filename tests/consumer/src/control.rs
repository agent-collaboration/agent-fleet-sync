use crate::{Consumer, EffectReceipt, FixtureAuthenticator, SampleProtocol};
use fleet_sync_protocol::*;
use fleet_sync_service::{ImportSeedRequest, ServiceHandle};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::watch;

pub type ConsumerHandle = ServiceHandle<SampleProtocol, Consumer, FixtureAuthenticator>;
#[derive(Serialize, Deserialize)]
pub enum ControlRequest {
    AttachSource {
        receiver: ReceiverId,
        endpoint: String,
        peer: u128,
    },
    InstallSnapshot {
        attempt: durable_steps::Uuid,
        database: DatabaseId,
        source: ReceiverId,
    },
    InstallRecord {
        install: durable_steps::Uuid,
    },
    WaitInstall {
        install: durable_steps::Uuid,
    },
    ReplaySnapshotReady {
        install: durable_steps::Uuid,
    },
    HoldNextInstallPageAnswer,
    WaitStagedInstallPage,
    WaitSourceUnattached {
        receiver: ReceiverId,
    },
    #[cfg(feature = "test-support")]
    DeliverySnapshot {
        mailbox: PeerId,
        origin: PeerId,
    },
    RequestCount,
    WaitAttempts {
        call: durable_steps::CallId,
        count: u64,
    },
    WaitOpening {
        peer: PeerId,
    },
    SetHookFailure {
        enabled: bool,
    },
    CheckPrivateHistoryOnApply,
    PrivateHistoryChecks,
    SetRegistrationFailure {
        enabled: bool,
    },
    LoseNextFollowUpAnswer,
    HoldNextFollowUpBeforeEffect,
    WaitFollowUpBeforeEffect,
    LoseNextClaimSettlementAnswer,
    HoldNextClaimReserve,
    WaitClaimReserve,
    WaitClaimSettlement {
        attempt: TransferId,
        count: u64,
    },
    WaitHolderChangeSettled {
        attempt: TransferId,
    },
    ClaimReceipts,
    Retire(Retirement),
    Renew {
        request: RenewRequest,
        holder: PeerId,
    },
    WaitEffects {
        count: usize,
    },
    Effects,
    Import(Import),
    SeedBegin {
        database: DatabaseId,
        attempt: durable_steps::Uuid,
    },
    SeedPage(Box<SeedPage>),
    SeedComplete(SeedComplete),
    CopyStatus {
        database: DatabaseId,
    },
    DatabaseStatuses {
        peer: PeerId,
    },
    HolderChange(HolderChangeRequest),
    PreviewTakeover {
        database: DatabaseId,
        attempt: durable_steps::Uuid,
    },
    WaitHolderChange(HolderChangeRequest),
    CancelHolderChange(fleet_sync_lease::CancelHolderChange),
    HolderChangeView {
        attempt: TransferId,
    },
    WaitHolderChangePhase {
        attempt: TransferId,
        phase: fleet_sync_lease::HolderChangePhase,
    },
    ReplayHolderRelease {
        holder: PeerId,
        request: ReleaseRequest,
        call: durable_steps::CallId,
    },
    Revoke {
        peer: PeerId,
    },
    AllowPeer {
        peer: PeerId,
    },
    HoldNextVerification {
        peer: PeerId,
    },
    WaitVerification {
        peer: PeerId,
    },
    ResumeVerification,
    Shutdown,
}
#[derive(Serialize, Deserialize)]
pub enum ControlAnswer {
    Install(fleet_sync_tokio_runner::InstallRecord),
    Staged(fleet_sync_receiver::InstallPageReceipt),
    #[cfg(feature = "test-support")]
    DeliverySnapshot(fleet_sync_service::DeliveryServiceSnapshot),
    RequestCount(u64),
    PrivateHistoryChecks(u64),
    Opening {
        connection: ConnectionId,
    },
    Ok,
    Effects(Vec<EffectReceipt>),
    Effect(EffectReceipt),
    FollowUpCall(durable_steps::CallId),
    ClaimReserveCall(durable_steps::CallId),
    ClaimReceipt(crate::ClaimGateReceipt),
    ClaimReceipts(Vec<crate::ClaimGateReceipt>),
    Retirement(RetirementAnswer),
    Renewal(RenewAnswer),
    Import(ImportAnswer),
    Replication(Box<ReplicationAnswer>),
    CopyStatus(CopyStatusAnswer),
    DatabaseStatuses(Vec<DatabaseStatus>),
    HolderChange(HolderChangeAnswer),
    TakeoverPreview(TakeoverPreview),
    Cancel(fleet_sync_lease::CancelAnswer),
    HolderChangeView(Option<fleet_sync_lease::HolderChangeView>),
    Release(ReleaseAnswer),
    Error {
        message: String,
    },
}
pub(crate) struct ControlState {
    pub consumer: Arc<Consumer>,
    pub authenticator: Arc<FixtureAuthenticator>,
    pub service: ConsumerHandle,
    pub shutdown: watch::Sender<bool>,
    pub source_events:
        tokio::sync::mpsc::Sender<fleet_sync_service::AttachedSource<SampleProtocol>>,
}
impl ControlState {
    pub async fn dispatch(&self, request: ControlRequest) -> ControlAnswer {
        let result: crate::TestResult<ControlAnswer> = async {
            Ok(match request {
                ControlRequest::AttachSource {
                    receiver,
                    endpoint,
                    peer,
                } => {
                    let client = fleet_sync_session::SessionClient::<_, _, SampleProtocol>::load(
                        fleet_sync_transport_websocket::WebSocketTransport::default(),
                        self.consumer.store.clone(),
                        &runtime_primitives::UuidV7Ids,
                        SchemaSupport { max_version: 100 },
                        runtime_primitives::Backoff {
                            initial: Duration::from_millis(10),
                            max: Duration::from_secs(1),
                            factor: 2,
                        },
                    )
                    .await?;
                    let attached = self
                        .service
                        .attach_source(
                            receiver,
                            client,
                            fleet_sync_session::Endpoint::new(&endpoint)?,
                            fleet_sync_session::PeerCredential::new(format!("Bearer peer-{peer}"))?,
                        )
                        .await?;
                    self.source_events.send(attached).await?;
                    ControlAnswer::Ok
                }
                ControlRequest::InstallSnapshot {
                    attempt,
                    database,
                    source,
                } => ControlAnswer::Install(
                    self.service
                        .install_snapshot(attempt, database, source)
                        .await?
                        .status()
                        .await?,
                ),
                ControlRequest::InstallRecord { install } => {
                    ControlAnswer::Install(self.service.install_record(install).await?)
                }
                ControlRequest::ReplaySnapshotReady { install } => {
                    self.service.replay_snapshot_ready(install).await?;
                    ControlAnswer::Ok
                }
                ControlRequest::WaitInstall { install } => ControlAnswer::Install(
                    tokio::time::timeout(
                        Duration::from_secs(60),
                        self.service.wait_install(install),
                    )
                    .await??,
                ),
                ControlRequest::HoldNextInstallPageAnswer => {
                    self.service.hold_next_install_page_answer();
                    ControlAnswer::Ok
                }
                ControlRequest::WaitSourceUnattached { receiver } => {
                    tokio::time::timeout(
                        Duration::from_secs(50),
                        self.service.wait_source_unattached(receiver),
                    )
                    .await??;
                    ControlAnswer::Ok
                }
                ControlRequest::WaitStagedInstallPage => ControlAnswer::Staged(
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        self.service.wait_staged_install_page(),
                    )
                    .await??,
                ),
                #[cfg(feature = "test-support")]
                ControlRequest::DeliverySnapshot { mailbox, origin } => {
                    ControlAnswer::DeliverySnapshot(
                        self.service.delivery_snapshot(mailbox, origin).await?,
                    )
                }
                ControlRequest::RequestCount => ControlAnswer::RequestCount(
                    self.consumer.handled_requests.load(Ordering::SeqCst),
                ),
                ControlRequest::WaitAttempts { call, count } => ControlAnswer::Effect(
                    tokio::time::timeout(
                        // Engine drive ownership survives a process kill for
                        // its 30-second lease; wait on the committed receipt.
                        Duration::from_secs(45),
                        self.consumer.store.wait_attempts(call, count),
                    )
                    .await??,
                ),
                ControlRequest::WaitOpening { peer } => ControlAnswer::Opening {
                    connection: tokio::time::timeout(
                        Duration::from_secs(10),
                        self.service.wait_opening(peer),
                    )
                    .await??,
                },
                ControlRequest::SetHookFailure { enabled } => {
                    self.consumer.hook_failure.store(enabled, Ordering::SeqCst);
                    ControlAnswer::Ok
                }
                ControlRequest::CheckPrivateHistoryOnApply => {
                    self.consumer
                        .private_history_checks_enabled
                        .store(true, Ordering::SeqCst);
                    ControlAnswer::Ok
                }
                ControlRequest::PrivateHistoryChecks => ControlAnswer::PrivateHistoryChecks(
                    self.consumer.private_history_checks.load(Ordering::SeqCst),
                ),
                ControlRequest::SetRegistrationFailure { enabled } => {
                    self.consumer
                        .registration_failure
                        .store(enabled, Ordering::SeqCst);
                    ControlAnswer::Ok
                }
                ControlRequest::LoseNextFollowUpAnswer => {
                    self.consumer
                        .lose_follow_up_answer
                        .store(true, Ordering::SeqCst);
                    ControlAnswer::Ok
                }
                ControlRequest::HoldNextFollowUpBeforeEffect => {
                    self.consumer.follow_up_before_effect.send_replace(None);
                    self.consumer
                        .hold_follow_up_before_effect
                        .store(true, Ordering::SeqCst);
                    ControlAnswer::Ok
                }
                ControlRequest::WaitFollowUpBeforeEffect => {
                    let mut observed = self.consumer.follow_up_before_effect.subscribe();
                    let call = loop {
                        if let Some(call) = observed.borrow().clone() {
                            break call;
                        }
                        observed.changed().await?;
                    };
                    ControlAnswer::FollowUpCall(call)
                }
                ControlRequest::LoseNextClaimSettlementAnswer => {
                    self.consumer
                        .lose_claim_settlement_answer
                        .store(true, Ordering::SeqCst);
                    ControlAnswer::Ok
                }
                ControlRequest::HoldNextClaimReserve => {
                    self.consumer.claim_reserve_observed.send_replace(None);
                    self.consumer
                        .hold_claim_reserve
                        .store(true, Ordering::SeqCst);
                    ControlAnswer::Ok
                }
                ControlRequest::WaitClaimReserve => {
                    let mut observed = self.consumer.claim_reserve_observed.subscribe();
                    let call = loop {
                        if let Some(call) = observed.borrow().clone() {
                            break call;
                        }
                        observed.changed().await?;
                    };
                    ControlAnswer::ClaimReserveCall(call)
                }
                ControlRequest::WaitClaimSettlement { attempt, count } => {
                    ControlAnswer::ClaimReceipt(
                        self.consumer
                            .store
                            .wait_claim_settlement(attempt, count)
                            .await?,
                    )
                }
                ControlRequest::WaitHolderChangeSettled { attempt } => {
                    ControlAnswer::HolderChangeView(Some(
                        self.service.wait_holder_change_settled(attempt).await?,
                    ))
                }
                ControlRequest::ClaimReceipts => {
                    ControlAnswer::ClaimReceipts(self.consumer.store.claim_receipts().await?)
                }
                ControlRequest::Retire(request) => {
                    ControlAnswer::Retirement(self.service.retire(request).await?)
                }
                ControlRequest::Renew { request, holder } => {
                    ControlAnswer::Renewal(self.service.renew(request, holder).await?)
                }
                ControlRequest::Effects => {
                    ControlAnswer::Effects(self.consumer.store.effects().await?)
                }
                ControlRequest::WaitEffects { count } => ControlAnswer::Effects(
                    tokio::time::timeout(
                        Duration::from_secs(15),
                        self.consumer.store.wait_effects(count),
                    )
                    .await??,
                ),
                ControlRequest::Import(request) => {
                    ControlAnswer::Import(self.service.import(request).await?)
                }
                ControlRequest::SeedBegin { database, attempt } => {
                    ControlAnswer::Replication(Box::new(
                        self.service
                            .seed_import(ImportSeedRequest::Begin { database, attempt })
                            .await?,
                    ))
                }
                ControlRequest::SeedPage(page) => ControlAnswer::Replication(Box::new(
                    self.service
                        .seed_import(ImportSeedRequest::Page(*page))
                        .await?,
                )),
                ControlRequest::SeedComplete(done) => ControlAnswer::Replication(Box::new(
                    self.service
                        .seed_import(ImportSeedRequest::Complete(done))
                        .await?,
                )),
                ControlRequest::CopyStatus { database } => {
                    ControlAnswer::CopyStatus(self.service.copy_status(database).await?)
                }
                ControlRequest::DatabaseStatuses { peer } => {
                    ControlAnswer::DatabaseStatuses(self.service.database_statuses(peer).await?)
                }
                ControlRequest::HolderChange(request) => ControlAnswer::HolderChange(
                    self.service
                        .holder_change(request, ActorRef::new("consumer-control")?)
                        .await?,
                ),
                ControlRequest::CancelHolderChange(cancel) => {
                    ControlAnswer::Cancel(self.service.cancel_holder_change(cancel).await?)
                }
                ControlRequest::PreviewTakeover { database, attempt } => {
                    ControlAnswer::TakeoverPreview(
                        self.service
                            .preview_takeover(
                                durable_steps::RequestKey::new(attempt.to_string())?,
                                database,
                            )
                            .await?,
                    )
                }
                ControlRequest::HolderChangeView { attempt } => {
                    ControlAnswer::HolderChangeView(self.service.holder_change_view(attempt).await?)
                }
                ControlRequest::WaitHolderChangePhase { attempt, phase } => {
                    ControlAnswer::HolderChangeView(Some(
                        self.service
                            .wait_holder_change_phase(attempt, phase)
                            .await?,
                    ))
                }
                ControlRequest::ReplayHolderRelease {
                    holder,
                    request,
                    call,
                } => ControlAnswer::Release(
                    self.service
                        .replay_holder_release(holder, request, call)
                        .await?,
                ),
                ControlRequest::WaitHolderChange(request) => {
                    ControlAnswer::HolderChange(self.service.wait_holder_change(request).await?)
                }
                ControlRequest::Revoke { peer } => {
                    self.authenticator.set_allowed(peer, false).await;
                    self.service.revoke(peer).await?;
                    ControlAnswer::Ok
                }
                ControlRequest::AllowPeer { peer } => {
                    self.authenticator.set_allowed(peer, true).await;
                    ControlAnswer::Ok
                }
                ControlRequest::HoldNextVerification { peer } => {
                    self.authenticator.hold_next(peer).await;
                    ControlAnswer::Ok
                }
                ControlRequest::WaitVerification { peer } => {
                    self.authenticator.wait_verification(peer).await?;
                    ControlAnswer::Ok
                }
                ControlRequest::ResumeVerification => {
                    self.authenticator.resume_verification();
                    ControlAnswer::Ok
                }
                ControlRequest::Shutdown => {
                    self.shutdown.send_replace(true);
                    ControlAnswer::Ok
                }
            })
        }
        .await;
        result.unwrap_or_else(|error| ControlAnswer::Error {
            message: error.to_string(),
        })
    }
}
