use crate::{ControlAnswer, TestResult, control::ControlState};
use fleet_sync_protocol::DatabaseId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum CopyFaultRequest {
    HoldCall {
        database: DatabaseId,
        kind: fleet_sync_service::CopyCallKind,
    },
    WaitCall {
        database: DatabaseId,
    },
    WaitCopyWaiter {
        database: DatabaseId,
    },
    ReleaseCall {
        database: DatabaseId,
    },
    HoldDrive {
        database: DatabaseId,
    },
    ReleaseDrive {
        database: DatabaseId,
    },
    ReplaceMarker {
        database: DatabaseId,
        replacement: String,
    },
    WaitHolderChange {
        attempt: fleet_sync_protocol::TransferId,
    },
    LoseCommitReply {
        database: DatabaseId,
    },
    LoseRollbackReply {
        database: DatabaseId,
    },
    FailNextWake {
        database: DatabaseId,
    },
    WakeFailures {
        database: DatabaseId,
    },
    Wake {
        database: DatabaseId,
    },
}
impl ControlState {
    pub async fn copy_fault(&self, request: CopyFaultRequest) -> TestResult<ControlAnswer> {
        match request {
            CopyFaultRequest::HoldCall { database, kind } => {
                self.service.hold_copy_call_for_test(database, kind).await?;
                Ok(ControlAnswer::Ok)
            }
            CopyFaultRequest::WaitCall { database } => Ok(ControlAnswer::CopyCall(
                tokio::time::timeout(
                    std::time::Duration::from_secs(15),
                    self.service.wait_copy_call_for_test(database),
                )
                .await??,
            )),
            CopyFaultRequest::ReleaseCall { database } => {
                self.service.release_copy_call_for_test(database).await?;
                Ok(ControlAnswer::Ok)
            }
            CopyFaultRequest::WaitCopyWaiter { database } => Ok(ControlAnswer::CopyWaiter(
                tokio::time::timeout(
                    std::time::Duration::from_secs(15),
                    self.service.wait_copy_waiter_for_test(database),
                )
                .await??,
            )),
            CopyFaultRequest::HoldDrive { database } => {
                let mut held = self.copy_drives.lock().await;
                if held.contains_key(&database) {
                    return Err("copy drive already held".into());
                }
                held.insert(
                    database,
                    self.service.hold_copy_drive_for_test(database).await?,
                );
                Ok(ControlAnswer::Ok)
            }
            CopyFaultRequest::ReleaseDrive { database } => {
                self.copy_drives
                    .lock()
                    .await
                    .remove(&database)
                    .ok_or("copy drive not held")?;
                Ok(ControlAnswer::Ok)
            }
            CopyFaultRequest::ReplaceMarker {
                database,
                replacement,
            } => Ok(ControlAnswer::CopyMarker(
                self.service
                    .replace_copy_marker_for_test(database, replacement)
                    .await?,
            )),
            CopyFaultRequest::WaitHolderChange { attempt } => {
                Ok(ControlAnswer::HolderChangeView(Some(
                    tokio::time::timeout(
                        std::time::Duration::from_secs(15),
                        self.service.wait_holder_change_copy_for_test(attempt),
                    )
                    .await??,
                )))
            }
            CopyFaultRequest::LoseCommitReply { database } => {
                self.service
                    .lose_copy_transaction_reply_for_test(
                        database,
                        fleet_sync_tokio_runner::TransactionReplyFault::Commit,
                    )
                    .await?;
                Ok(ControlAnswer::Ok)
            }
            CopyFaultRequest::LoseRollbackReply { database } => {
                self.service
                    .lose_copy_transaction_reply_for_test(
                        database,
                        fleet_sync_tokio_runner::TransactionReplyFault::Rollback,
                    )
                    .await?;
                Ok(ControlAnswer::Ok)
            }
            CopyFaultRequest::FailNextWake { database } => {
                self.service.fail_next_copy_wake_for_test(database).await?;
                Ok(ControlAnswer::Ok)
            }
            CopyFaultRequest::WakeFailures { database } => Ok(ControlAnswer::CopyWakeFailures(
                self.service.copy_wake_failures_for_test(database).await?,
            )),
            CopyFaultRequest::Wake { database } => {
                self.service.wake_copy_for_test(database).await?;
                Ok(ControlAnswer::Ok)
            }
        }
    }
}
