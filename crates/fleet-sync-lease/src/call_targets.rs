use durable_steps::CallTarget;
use fleet_sync_protocol as wire;
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HolderRelease {
    pub holder: PeerId,
    pub database: DatabaseId,
}
impl CallTarget for HolderRelease {
    const KIND: &'static str = "fleet-sync.holder-release";
    type Request = ReleaseRequest;
    type Answer = ReleaseAnswer;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HolderWithdrawal {
    pub holder: PeerId,
}
impl CallTarget for HolderWithdrawal {
    const KIND: &'static str = "fleet-sync.holder-withdrawal";
    type Request = WithdrawRelease;
    type Answer = WithdrawAnswer;
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CopyFence {
    pub copy: ReceiverId,
}
impl CallTarget for CopyFence {
    const KIND: &'static str = "fleet-sync.copy-fence";
    type Request = FenceRequest;
    type Answer = FenceResult;
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CopyCreate {
    pub copy: ReceiverId,
}
impl CallTarget for CopyCreate {
    const KIND: &'static str = "fleet-sync.copy-create";
    type Request = CreateCopy;
    type Answer = CreateCopyAnswer;
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CopyRetire {
    pub copy: ReceiverId,
}
impl CallTarget for CopyRetire {
    const KIND: &'static str = "fleet-sync.copy-retire";
    type Request = RetireCopy;
    type Answer = RetireCopyAnswer;
}

/// Host-owned lifecycle and claim gates. Their calls are journaled like every other target.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegistrationAdmission;
impl CallTarget for RegistrationAdmission {
    const KIND: &'static str = "fleet-sync.registration-admission";
    type Request = wire::RegistrationAdmission;
    type Answer = GateAnswer;
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetirementAdmission;
impl CallTarget for RetirementAdmission {
    const KIND: &'static str = "fleet-sync.retirement-admission";
    type Request = wire::RetirementAdmission;
    type Answer = GateAnswer;
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClaimReservation;
impl CallTarget for ClaimReservation {
    const KIND: &'static str = "fleet-sync.claim-reservation";
    type Request = wire::ClaimReservation;
    type Answer = GateAnswer;
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClaimSettlement;
impl CallTarget for ClaimSettlement {
    const KIND: &'static str = "fleet-sync.claim-settlement";
    type Request = wire::ClaimSettlement;
    type Answer = SettlementRecorded;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CopyAcknowledged {
    pub copy: ReceiverId,
    pub database: DatabaseId,
}
impl CallTarget for CopyAcknowledged {
    const KIND: &'static str = "fleet-sync.copy-acknowledged";
    type Request = AwaitAcked;
    type Answer = AwaitAckedAnswer;
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CopyRead {
    pub copy: ReceiverId,
    pub database: DatabaseId,
}
impl CallTarget for CopyRead {
    const KIND: &'static str = "fleet-sync.copy-status";
    type Request = ();
    type Answer = CopyStatusAnswer;
}
