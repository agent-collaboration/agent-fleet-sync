use durable_steps::{CallTarget, OperationFailure, Uuid};
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterDatabaseInput {
    pub request: Registration,
    pub receiver: ReceiverId,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WriterRegistrationOutcome {
    Pending,
    Ready { lease: LeaseStatus },
    Refused { reason: RegistrationRefusal },
    Failed { reason: OperationFailure },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryRegistration {
    pub receiver: ReceiverId,
}
impl CallTarget for RegistryRegistration {
    const KIND: &'static str = "fleet.writer-register";
    type Request = Registration;
    type Answer = RegistrationAnswer;
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryRenewal {
    pub receiver: ReceiverId,
    pub cause: RenewalCause,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RenewalCause {
    Periodic,
    Notice,
    Seeding,
}
impl CallTarget for RegistryRenewal {
    const KIND: &'static str = "fleet.writer-renew";
    type Request = RenewRequest;
    type Answer = RenewAnswer;
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedCopyInput {
    pub receiver: ReceiverId,
    pub position: ReplicationPosition,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedFile {
    pub receiver: ReceiverId,
}
impl CallTarget for SeedFile {
    const KIND: &'static str = "fleet.writer-seed-file";
    type Request = SeedFileRequest;
    type Answer = Result<SeedFileAnswer, SeedCopyRefusal>;
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SeedFileRequest {
    Open {
        generation: Uuid,
        position: ReplicationPosition,
    },
    Page {
        generation: Uuid,
        seed: SeedId,
        page: u32,
        cursor: Option<SnapshotCursor>,
    },
    Complete {
        generation: Uuid,
        request: SeedComplete,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SeedFileAnswer {
    Opened {
        seed: SeedId,
        database: DatabaseId,
        position: ReplicationPosition,
        schema: SchemaIdentity,
    },
    Page {
        digest: PageDigest,
        rows: u64,
        next: Option<SnapshotCursor>,
        completed: Option<TableManifest>,
    },
    Published {
        position: ReplicationPosition,
    },
    SourceLost,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SeedCopyRefusal {
    pub code: String,
    pub detail: String,
}
impl From<SeedCopyRefusal> for OperationFailure {
    fn from(refusal: SeedCopyRefusal) -> Self {
        Self::Refused {
            code: refusal.code,
            detail: refusal.detail,
        }
    }
}
