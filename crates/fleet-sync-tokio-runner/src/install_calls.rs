use crate::{InstallFileAnswer, StagedPage};
use durable_steps::{CallTarget, Uuid};
use fleet_sync_protocol::{
    CopyStatus, DatabaseId, ReceiverId, ReplicationPosition, SnapshotCursor,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallSource {
    pub receiver: ReceiverId,
    pub database: DatabaseId,
}
impl CallTarget for InstallSource {
    const KIND: &'static str = "fleet.snapshot-source";
    type Request = ();
    type Answer = Result<CopyStatus, InstallRefusal>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallFile {
    pub install_id: Uuid,
}
impl CallTarget for InstallFile {
    const KIND: &'static str = "fleet.snapshot-file";
    type Request = InstallFileRequest;
    type Answer = Result<InstallFileResult, InstallRefusal>;
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InstallFileRequest {
    Prepare {
        source: CopyStatus,
    },
    Page {
        page: u32,
        cursor: Option<SnapshotCursor>,
    },
    Verify,
    Publish,
    Ready,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InstallFileResult {
    Prepared,
    Page(StagedPage),
    Verified,
    Moved { position: ReplicationPosition },
    Outcome(InstallFileAnswer),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InstallRefusal {
    pub code: String,
    pub detail: String,
}
impl From<InstallRefusal> for durable_steps::OperationFailure {
    fn from(refusal: InstallRefusal) -> Self {
        Self::Refused {
            code: refusal.code,
            detail: refusal.detail,
        }
    }
}
