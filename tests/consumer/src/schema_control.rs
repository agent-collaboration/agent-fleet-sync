//! Controls for native schema publication and admission-window journeys.
use crate::{ControlAnswer, TestResult, control::ControlState};
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum SchemaControlRequest {
    HoldReport {
        peer: PeerId,
        attach_seq: AttachSequence,
    },
    WaitReport,
    ReleaseReport,
    Projection {
        peer: PeerId,
    },
    HoldRegistryDrive,
    ReleaseRegistryDrive,
    Admission {
        attempt: TransferId,
    },
    FailFinalize {
        peer: PeerId,
    },
    WaitFinalizeRollback,
    ReleaseFinalize,
    Snapshot(ReportSchemaSupport),
    WaitClosed {
        connection: ConnectionId,
    },
    WaitPublication(ReportSchemaSupport),
}
#[derive(Serialize, Deserialize)]
pub enum SchemaControlAnswer {
    Ok,
    Report(ReportSchemaSupport),
    Projection(Option<ReportSchemaSupport>),
    AdmittedTarget { schema: Option<SchemaSupport> },
    RolledBack { peer: PeerId },
    Snapshot(fleet_sync_tokio_runner::SchemaPublicationSnapshot),
}
impl ControlState {
    pub async fn schema_control(&self, request: SchemaControlRequest) -> TestResult<ControlAnswer> {
        let answer = match request {
            SchemaControlRequest::HoldReport { peer, attach_seq } => {
                self.service.hold_schema_report(peer, attach_seq).await?;
                SchemaControlAnswer::Ok
            }
            SchemaControlRequest::WaitReport => {
                SchemaControlAnswer::Report(self.service.wait_schema_report().await?)
            }
            SchemaControlRequest::ReleaseReport => {
                self.service.release_schema_report().await;
                SchemaControlAnswer::Ok
            }
            SchemaControlRequest::Projection { peer } => {
                SchemaControlAnswer::Projection(self.service.accepted_schema_support(peer).await?)
            }
            SchemaControlRequest::HoldRegistryDrive => {
                let mut held = self.schema_drive.lock().await;
                if held.is_some() {
                    return Err("registry drive already held".into());
                }
                *held = Some(self.service.hold_registry_drive().await);
                SchemaControlAnswer::Ok
            }
            SchemaControlRequest::ReleaseRegistryDrive => {
                self.schema_drive
                    .lock()
                    .await
                    .take()
                    .ok_or("registry drive not held")?;
                SchemaControlAnswer::Ok
            }
            SchemaControlRequest::Admission { attempt } => SchemaControlAnswer::AdmittedTarget {
                schema: self.service.admitted_schema_for_test(attempt).await?,
            },
            SchemaControlRequest::FailFinalize { peer } => {
                self.service.fail_next_schema_finalize(peer)?;
                SchemaControlAnswer::Ok
            }
            SchemaControlRequest::WaitFinalizeRollback => SchemaControlAnswer::RolledBack {
                peer: self.service.wait_schema_finalize_rollback().await?,
            },
            SchemaControlRequest::ReleaseFinalize => {
                self.service.release_schema_finalize();
                SchemaControlAnswer::Ok
            }
            SchemaControlRequest::Snapshot(report) => SchemaControlAnswer::Snapshot(
                self.service.schema_publication_snapshot(&report).await?,
            ),
            SchemaControlRequest::WaitClosed { connection } => {
                self.service
                    .wait_schema_connection_closed(connection)
                    .await?;
                SchemaControlAnswer::Ok
            }
            SchemaControlRequest::WaitPublication(report) => {
                self.service.wait_schema_publication(&report).await?;
                SchemaControlAnswer::Ok
            }
        };
        Ok(ControlAnswer::Schema(answer))
    }
}
