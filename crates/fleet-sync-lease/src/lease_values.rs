use fleet_sync_protocol::{Epoch, ReceiverId, ReplicationPosition, SyncTime, TransferId};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Consumer-selected lease duration and the holder's ordinary renewal cadence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeasePolicy {
    pub lease_duration: Duration,
    pub renew_interval: Duration,
}

/// Derived from the current lease, local clock and persisted release record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum WriterState {
    Provisional,
    Seeding {
        epoch: Epoch,
    },
    Writing {
        epoch: Epoch,
    },
    Paused {
        epoch: Epoch,
        expired_at: SyncTime,
    },
    Draining {
        epoch: Epoch,
        transfer: TransferId,
    },
    Released {
        epoch: Epoch,
        final_position: ReplicationPosition,
    },
    Superseded {
        current: Epoch,
    },
}

/// Host configuration for the registry's authoritative copy and call policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegistryConfig {
    pub lease: LeasePolicy,
    pub authoritative_receiver: ReceiverId,
    pub target_schema_max: u64,
    pub release_unsent_for: Duration,
    pub move_needs_renewal_within: Option<Duration>,
}
