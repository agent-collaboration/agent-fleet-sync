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
    LoseNextFollowUpAnswer,
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
    HolderChange(HolderChangeRequest),
    Revoke {
        peer: PeerId,
    },
    Shutdown,
}
#[derive(Serialize, Deserialize)]
pub enum ControlAnswer {
    RequestCount(u64),
    Opening { connection: ConnectionId },
    Ok,
    Effects(Vec<EffectReceipt>),
    Effect(EffectReceipt),
    Import(ImportAnswer),
    Replication(Box<ReplicationAnswer>),
    CopyStatus(CopyStatusAnswer),
    HolderChange(HolderChangeAnswer),
    Error { message: String },
}
pub(crate) struct ControlState {
    pub consumer: Arc<Consumer>,
    pub service: ConsumerHandle,
    pub shutdown: watch::Sender<bool>,
}
impl ControlState {
    pub async fn dispatch(&self, request: ControlRequest) -> ControlAnswer {
        let result: crate::TestResult<ControlAnswer> = async {
            Ok(match request {
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
                ControlRequest::LoseNextFollowUpAnswer => {
                    self.consumer
                        .lose_follow_up_answer
                        .store(true, Ordering::SeqCst);
                    ControlAnswer::Ok
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
                ControlRequest::HolderChange(request) => {
                    ControlAnswer::HolderChange(self.service.holder_change(request).await?)
                }
                ControlRequest::Revoke { peer } => {
                    self.service.revoke(peer).await?;
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
