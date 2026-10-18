use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DatabaseLifecycle {
    Registered {
        origin: Origin,
        imported_from: Option<ImportOrigin>,
    },
    Unheld {
        import_digest: ImportDigest,
    },
    Retiring {
        attempt: RequestId,
    },
    Retired {
        at: SyncTime,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportOrigin {
    pub digest: ImportDigest,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatabaseRecord {
    pub database: DatabaseId,
    pub lifecycle: DatabaseLifecycle,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegistryLease {
    pub database: DatabaseId,
    pub holder: PeerId,
    pub epoch: Epoch,
    pub expires_at: SyncTime,
    pub last_renewed_at: SyncTime,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EpochHistory {
    pub database: DatabaseId,
    pub epoch: Epoch,
    pub holder: PeerId,
    pub granted_at: SyncTime,
    pub ended_at: Option<ReplicationPosition>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RenewalAttempt {
    pub request: RenewRequest,
    pub holder: PeerId,
    pub answer: RenewAnswer,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransferIntent {
    pub request: HolderChangeRequest,
    pub new_epoch: Option<Epoch>,
    pub target_schema: Option<SchemaSupport>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransferRecord {
    pub intent: TransferIntent,
    pub outcome: Option<HolderChangeAnswer>,
    pub phase: crate::HolderChangePhase,
    pub phase_history: Vec<crate::PhaseEntry>,
    pub cleanup_failed: Option<CleanupFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CleanupFailure {
    pub reason: FailureCode,
    pub at: SyncTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransferAdmission {
    pub operation: durable_steps::OperationId,
    pub request: HolderChangeRequest,
    pub requested_by: ActorRef,
    pub admitted_at: SyncTime,
    pub target_schema: Option<SchemaSupport>,
    pub follow_up: Option<durable_steps::OperationId>,
}

impl TransferRecord {
    pub(crate) fn requested(intent: TransferIntent, at: SyncTime) -> Self {
        Self {
            intent,
            outcome: None,
            phase: crate::HolderChangePhase::Requested,
            phase_history: vec![crate::PhaseEntry {
                phase: crate::HolderChangePhase::Requested,
                entered_at: at,
            }],
            cleanup_failed: None,
        }
    }
    pub(crate) fn enter(
        &mut self,
        phase: crate::HolderChangePhase,
        at: SyncTime,
    ) -> Result<(), crate::LeaseError> {
        if self.phase == phase {
            return Ok(());
        }
        if !self.allows(&self.phase, &phase) {
            return Err(crate::LeaseError::Corrupt {
                field: "holder-change phase transition".into(),
            });
        }
        let at = self
            .phase_history
            .last()
            .map_or(at, |entry| at.max(entry.entered_at));
        self.phase_history.push(crate::PhaseEntry {
            phase: phase.clone(),
            entered_at: at,
        });
        self.phase = phase;
        self.validate()
    }
    fn allows(&self, before: &crate::HolderChangePhase, after: &crate::HolderChangePhase) -> bool {
        use crate::HolderChangePhase as P;
        match (before, after) {
            (P::Ended, _) => false,
            (_, P::Ended) => true,
            (P::Requested, P::Releasing) | (P::Releasing, P::Released { .. }) => {
                matches!(self.intent.request, HolderChangeRequest::Move { .. })
            }
            (P::Requested, P::Reserved) => {
                matches!(self.intent.request, HolderChangeRequest::Claim { .. })
            }
            (P::Requested, P::Fencing) => {
                matches!(self.intent.request, HolderChangeRequest::Takeover { .. })
            }
            (P::Released { .. } | P::Reserved, P::Fencing) => true,
            (phase, P::Cancelling) => {
                phase.cancellable()
                    && matches!(self.intent.request, HolderChangeRequest::Move { .. })
            }
            _ => false,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), crate::LeaseError> {
        use crate::HolderChangePhase as P;
        let history = &self.phase_history;
        let valid = !history.is_empty()
            && history.len() <= 8
            && history
                .first()
                .is_some_and(|entry| entry.phase == P::Requested)
            && history
                .last()
                .is_some_and(|entry| entry.phase == self.phase)
            && history.windows(2).all(|pair| matches!(pair,[before,after] if before.entered_at<=after.entered_at && self.allows(&before.phase,&after.phase)))
            && (self.phase != P::Ended
                || self
                    .outcome
                    .as_ref()
                    .is_some_and(|answer| !matches!(answer, HolderChangeAnswer::Pending { .. })))
            && (self.outcome.is_none() || self.phase == P::Ended)
            && self.cleanup_failed.as_ref().is_none_or(|failure| {
                matches!(self.intent.request, HolderChangeRequest::Move { .. })
                    && (self.phase == P::Cancelling
                        || matches!(
                            self.outcome,
                            Some(HolderChangeAnswer::Refused(
                                HolderChangeRefusal::Superseded { .. }
                            ))
                        ))
                    && history
                        .last()
                        .is_some_and(|entry| failure.at >= entry.entered_at || self.phase == P::Ended)
            })
            && (!matches!(self.phase, P::Cancelling) || self.outcome.is_none());
        if !valid {
            return Err(crate::LeaseError::Corrupt {
                field: "holder-change phase history".into(),
            });
        }
        Ok(())
    }
}
impl TransferIntent {
    pub(crate) fn database(&self) -> DatabaseId {
        match self.request {
            HolderChangeRequest::Move { database, .. }
            | HolderChangeRequest::Takeover { database, .. }
            | HolderChangeRequest::Claim { database, .. } => database,
        }
    }
    pub(crate) fn attempt(&self) -> TransferId {
        match self.request {
            HolderChangeRequest::Move { attempt, .. }
            | HolderChangeRequest::Takeover { attempt, .. }
            | HolderChangeRequest::Claim { attempt, .. } => attempt,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "request",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum LifecycleIntent {
    Register(Registration),
    Import(Import),
    Retire(Retirement),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "answer",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum LifecycleOutcome {
    Registration(RegistrationAnswer),
    Import(ImportAnswer),
    Retirement(RetirementAnswer),
    Failed { reason: FailureCode },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleAttempt {
    pub intent: LifecycleIntent,
    pub outcome: Option<LifecycleOutcome>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreviewRecord {
    pub database: DatabaseId,
    pub preview: TakeoverPreview,
}
impl LifecycleIntent {
    pub(crate) fn database(&self) -> DatabaseId {
        match self {
            Self::Register(request) => request.database,
            Self::Import(request) => request.database,
            Self::Retire(request) => request.database,
        }
    }
    pub(crate) fn attempt(&self) -> RequestId {
        match self {
            Self::Register(request) => request.attempt,
            Self::Import(request) => request.attempt,
            Self::Retire(request) => request.attempt,
        }
    }
}
