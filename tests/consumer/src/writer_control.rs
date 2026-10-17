use crate::{SampleProtocol, SampleSchema, TestResult, insert_sample};
use fleet_sync_protocol::*;
use fleet_sync_tokio_runner::{NativeWriterRuntime, SeedPageObservation};
use fleet_sync_writer::{ReplicatedDatabase, WriteRefusal, WriterRegistrationOutcome};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Serialize, Deserialize)]
pub enum WriterCommand {
    Register,
    WaitReady,
    WaitPeriodicRenewal,
    Position,
    State,
    HoldSeedPage,
    HoldSeedPageAt {
        page: u32,
    },
    WaitSeedPage,
    ReleaseSeedPage,
    TryWrite {
        label: String,
    },
    WaitAcked {
        through: ReplicationPosition,
    },
    RejectRegistration,
    JournalSteps {
        operation: durable_steps::OperationId,
    },
    Shutdown,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum WriterReply {
    Ok,
    Operation(durable_steps::OperationId),
    Registration(WriterRegistrationOutcome),
    Position(ReplicationPosition),
    State(fleet_sync_lease::WriterState),
    SeedPage(SeedPageObservation),
    WriteRefused { seeding: bool, message: String },
    LateRejectionRefused { already_granted: bool },
    JournalSteps(Vec<durable_steps::storage::StepRecord>),
    Error { message: String },
}
pub(crate) struct WriterControl {
    pub database: Arc<ReplicatedDatabase<SampleSchema>>,
    pub runtime: Arc<NativeWriterRuntime<SampleSchema, SampleProtocol>>,
    pub acked: watch::Receiver<Option<ReplicationPosition>>,
    pub sender: Arc<crate::writer_actor::WriterSender>,
    pub receiver: ReceiverId,
}
impl WriterControl {
    pub async fn dispatch(&self, command: WriterCommand) -> WriterReply {
        let result: TestResult<WriterReply> = async {
            Ok(match command {
                WriterCommand::Register => {
                    WriterReply::Operation(self.runtime.start_registration().await?)
                }
                WriterCommand::JournalSteps { operation } => {
                    WriterReply::JournalSteps(self.runtime.journal_steps(operation).await?)
                }
                WriterCommand::WaitReady => {
                    WriterReply::Registration(self.runtime.wait_registration().await?)
                }
                WriterCommand::WaitPeriodicRenewal => {
                    self.runtime.transport().wait_periodic_renewal().await?;
                    WriterReply::Ok
                }
                WriterCommand::Position => {
                    WriterReply::Position(self.database.snapshot().await?.position())
                }
                WriterCommand::State => WriterReply::State(self.database.writer_state().await?),
                WriterCommand::HoldSeedPage => {
                    self.runtime.transport().hold_next_seed_page_answer();
                    WriterReply::Ok
                }
                WriterCommand::HoldSeedPageAt { page } => {
                    self.runtime.transport().hold_seed_page_answer(page);
                    WriterReply::Ok
                }
                WriterCommand::WaitSeedPage => {
                    WriterReply::SeedPage(self.runtime.transport().wait_seed_page().await?)
                }
                WriterCommand::ReleaseSeedPage => {
                    self.runtime.transport().release_seed_page_answer();
                    WriterReply::Ok
                }
                WriterCommand::TryWrite { label } => match self.database.begin_write().await {
                    Ok(mut write) => {
                        insert_sample(
                            &mut write,
                            durable_steps::Uuid::now_v7().to_string(),
                            label,
                            9_007_199_254_740_993,
                            vec![0, 255, 128],
                        )
                        .await?;
                        write.commit().await?;
                        WriterReply::Position(self.database.snapshot().await?.position())
                    }
                    Err(error) => WriterReply::WriteRefused {
                        seeding: matches!(error, WriteRefusal::Seeding),
                        message: error.to_string(),
                    },
                },
                WriterCommand::WaitAcked { through } => {
                    let mut acked = self.acked.clone();
                    loop {
                        let saved = self
                            .sender
                            .copies()
                            .await?
                            .into_iter()
                            .find(|copy| copy.copy == self.receiver);
                        if saved.is_some_and(|copy| copy.acked >= through) {
                            break;
                        }
                        acked.changed().await?;
                    }
                    WriterReply::Ok
                }
                WriterCommand::RejectRegistration => {
                    let reason = GateReason {
                        code: GateReasonCode::new("late")?,
                        detail: "late fixture admission refusal".into(),
                    };
                    let result = self.database.reject_registration(reason).await;
                    WriterReply::LateRejectionRefused {
                        already_granted: matches!(
                            result,
                            Err(fleet_sync_writer::OpenError::Holder {
                                error: fleet_sync_lease::HolderError::AlreadyGranted
                            })
                        ),
                    }
                }
                WriterCommand::Shutdown => WriterReply::Ok,
            })
        }
        .await;
        result.unwrap_or_else(|error| WriterReply::Error {
            message: error.to_string(),
        })
    }
}
