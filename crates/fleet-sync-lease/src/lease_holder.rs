use crate::{LeasePolicy, WriterState};
use fleet_sync_protocol::{
    DatabaseId, Epoch, GateReason, LeaseRefusal, LeaseState, LeaseStatus, PeerId, RenewAnswer,
    ReplicationPosition, SyncTime, TransferId,
};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HolderError {
    #[error("lease answer belongs to another database or writer")]
    LeaseMismatch,
    #[error("registry refused the writer: {0:?}")]
    Refused(LeaseRefusal),
    #[error("registration rejected: {reason:?}")]
    RegistrationRejected { reason: GateReason },
    #[error("writer cannot make this transition from its current state")]
    InvalidTransition,
}

#[derive(Clone, Debug)]
struct CachedLease {
    epoch: Epoch,
    deadline: SyncTime,
    renew_at: SyncTime,
}

#[derive(Clone, Debug)]
enum WriteActivity {
    Seeding,
    Writing,
    Draining { transfer: TransferId },
}

#[derive(Clone, Debug)]
enum HolderAuthority {
    Provisional,
    Seeding {
        epoch: Epoch,
    },
    Lease {
        cached: CachedLease,
        activity: WriteActivity,
    },
    Released {
        epoch: Epoch,
        final_position: ReplicationPosition,
    },
    Superseded {
        current: Epoch,
    },
    Refused(LeaseRefusal),
    Rejected {
        reason: GateReason,
    },
}

/// Mutated under the database's writer guard. Notices only trigger renewal;
/// this owner changes authority only from answers or committed local release state.
#[derive(Clone, Debug)]
pub struct LeaseHolder {
    database: DatabaseId,
    peer: PeerId,
    policy: LeasePolicy,
    authority: HolderAuthority,
}

impl LeaseHolder {
    pub fn provisional(database: DatabaseId, peer: PeerId, policy: LeasePolicy) -> Self {
        Self {
            database,
            peer,
            policy,
            authority: HolderAuthority::Provisional,
        }
    }

    pub fn seeding(database: DatabaseId, peer: PeerId, epoch: Epoch, policy: LeasePolicy) -> Self {
        Self {
            database,
            peer,
            policy,
            authority: HolderAuthority::Seeding { epoch },
        }
    }

    pub const fn database(&self) -> DatabaseId {
        self.database
    }
    pub const fn peer(&self) -> PeerId {
        self.peer
    }

    pub fn writer_state(&self, now: SyncTime) -> Result<WriterState, HolderError> {
        Ok(match &self.authority {
            HolderAuthority::Provisional => WriterState::Provisional,
            HolderAuthority::Seeding { epoch } => WriterState::Seeding { epoch: *epoch },
            HolderAuthority::Lease { cached, activity } => match activity {
                WriteActivity::Seeding => WriterState::Seeding {
                    epoch: cached.epoch,
                },
                WriteActivity::Draining { transfer } => WriterState::Draining {
                    epoch: cached.epoch,
                    transfer: *transfer,
                },
                WriteActivity::Writing if now < cached.deadline => WriterState::Writing {
                    epoch: cached.epoch,
                },
                WriteActivity::Writing => WriterState::Paused {
                    epoch: cached.epoch,
                    expired_at: cached.deadline,
                },
            },
            HolderAuthority::Released {
                epoch,
                final_position,
            } => WriterState::Released {
                epoch: *epoch,
                final_position: *final_position,
            },
            HolderAuthority::Superseded { current } => {
                WriterState::Superseded { current: *current }
            }
            HolderAuthority::Refused(refusal) => return Err(HolderError::Refused(refusal.clone())),
            HolderAuthority::Rejected { reason } => {
                return Err(HolderError::RegistrationRejected {
                    reason: reason.clone(),
                });
            }
        })
    }

    pub fn renewal_at(&self) -> Option<SyncTime> {
        match &self.authority {
            HolderAuthority::Lease { cached, .. } => Some(cached.renew_at),
            _ => None,
        }
    }

    /// `request_started` bounds the deadline conservatively: response transit
    /// never adds time to the registry's remaining duration. Replayed `recorded`
    /// grants are deliberately ignored; only `current` supplies authority.
    pub fn observe_renewal(
        &mut self,
        answer: RenewAnswer,
        request_started: SyncTime,
    ) -> Result<(), HolderError> {
        if matches!(
            self.authority,
            HolderAuthority::Released { .. }
                | HolderAuthority::Superseded { .. }
                | HolderAuthority::Rejected { .. }
                | HolderAuthority::Refused(LeaseRefusal::Retired)
        ) {
            return Ok(());
        }
        match answer {
            RenewAnswer::Renewed { current, .. } => self.observe_current(current, request_started),
            RenewAnswer::Refused(LeaseRefusal::StaleEpoch { current_epoch, .. }) => {
                if self.known_epoch().is_none_or(|known| current_epoch > known) {
                    self.authority = HolderAuthority::Superseded {
                        current: current_epoch,
                    };
                }
                Ok(())
            }
            RenewAnswer::Refused(refusal) => {
                self.authority = HolderAuthority::Refused(refusal);
                Ok(())
            }
        }
    }

    pub fn observe_registration(
        &mut self,
        current: LeaseStatus,
        request_started: SyncTime,
    ) -> Result<(), HolderError> {
        if !matches!(
            self.authority,
            HolderAuthority::Provisional | HolderAuthority::Seeding { .. }
        ) {
            return Err(HolderError::InvalidTransition);
        }
        self.observe_current(current, request_started)
    }

    fn observe_current(
        &mut self,
        current: LeaseStatus,
        request_started: SyncTime,
    ) -> Result<(), HolderError> {
        if current.database != self.database {
            return Err(HolderError::LeaseMismatch);
        }
        if let Some(known) = self.known_epoch() {
            if current.epoch < known {
                return Ok(());
            }
            if current.epoch > known {
                self.authority = HolderAuthority::Superseded {
                    current: current.epoch,
                };
                return Ok(());
            }
        }
        if current.holder != self.peer {
            self.authority = HolderAuthority::Refused(LeaseRefusal::NotWriter {
                current: Some(current.epoch),
            });
            return Ok(());
        }
        let activity = match &self.authority {
            HolderAuthority::Seeding { .. } => WriteActivity::Seeding,
            HolderAuthority::Lease { activity, .. } => activity.clone(),
            _ => WriteActivity::Writing,
        };
        let remaining = if current.state == LeaseState::Held {
            current.remaining_ms
        } else {
            0
        };
        let deadline = request_started.after(Duration::from_millis(remaining));
        self.authority = HolderAuthority::Lease {
            cached: CachedLease {
                epoch: current.epoch,
                deadline,
                renew_at: request_started.after(self.policy.renew_interval),
            },
            activity,
        };
        Ok(())
    }

    fn known_epoch(&self) -> Option<Epoch> {
        match &self.authority {
            HolderAuthority::Seeding { epoch } | HolderAuthority::Released { epoch, .. } => {
                Some(*epoch)
            }
            HolderAuthority::Lease { cached, .. } => Some(cached.epoch),
            HolderAuthority::Superseded { current } => Some(*current),
            HolderAuthority::Refused(LeaseRefusal::NotWriter {
                current: Some(current),
            }) => Some(*current),
            HolderAuthority::Provisional
            | HolderAuthority::Refused(_)
            | HolderAuthority::Rejected { .. } => None,
        }
    }

    pub fn reject_registration(&mut self, reason: GateReason) -> Result<(), HolderError> {
        if !matches!(self.authority, HolderAuthority::Provisional) {
            return Err(HolderError::InvalidTransition);
        }
        self.authority = HolderAuthority::Rejected { reason };
        Ok(())
    }

    /// Call only after the installed file's verified marker is published and ready.
    pub fn complete_seed(&mut self, epoch: Epoch) -> Result<(), HolderError> {
        match &mut self.authority {
            HolderAuthority::Lease { cached, activity }
                if cached.epoch == epoch && matches!(activity, WriteActivity::Seeding) =>
            {
                *activity = WriteActivity::Writing;
                Ok(())
            }
            _ => Err(HolderError::InvalidTransition),
        }
    }

    /// The database holds its writer guard across the release record's commit
    /// and this transition, so no admission can observe only one of them.
    pub fn begin_draining(&mut self, transfer: TransferId) -> Result<(), HolderError> {
        match &mut self.authority {
            HolderAuthority::Lease { activity, .. }
                if matches!(activity, WriteActivity::Writing) =>
            {
                *activity = WriteActivity::Draining { transfer };
                Ok(())
            }
            HolderAuthority::Lease {
                activity: WriteActivity::Draining { transfer: existing },
                ..
            } if *existing == transfer => Ok(()),
            _ => Err(HolderError::InvalidTransition),
        }
    }

    pub fn refuse_release(&mut self, transfer: TransferId) -> Result<(), HolderError> {
        match &mut self.authority {
            HolderAuthority::Lease { activity, .. } if matches!(&*activity,WriteActivity::Draining {transfer:existing} if *existing==transfer) =>
            {
                *activity = WriteActivity::Writing;
                Ok(())
            }
            _ => Err(HolderError::InvalidTransition),
        }
    }

    pub fn complete_release(
        &mut self,
        transfer: TransferId,
        final_position: ReplicationPosition,
    ) -> Result<(), HolderError> {
        match &self.authority {
            HolderAuthority::Lease {
                cached,
                activity: WriteActivity::Draining { transfer: existing },
            } if *existing == transfer && cached.epoch == final_position.epoch => {
                self.authority = HolderAuthority::Released {
                    epoch: cached.epoch,
                    final_position,
                };
                Ok(())
            }
            HolderAuthority::Released {
                final_position: existing,
                ..
            } if *existing == final_position => Ok(()),
            _ => Err(HolderError::InvalidTransition),
        }
    }
}
