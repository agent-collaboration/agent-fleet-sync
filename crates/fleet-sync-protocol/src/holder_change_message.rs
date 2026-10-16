use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FenceExpectation {
    pub epoch: Epoch,
    pub acked: ReplicationPosition,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FenceRequest {
    pub database: DatabaseId,
    pub transfer_id: TransferId,
    pub new_epoch: Epoch,
    pub expect: FenceExpectation,
    pub target_schema_max: u64,
    pub holder: PeerId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum FenceResult {
    Fenced {
        old_epoch: Epoch,
        new_epoch: Epoch,
        start: ReplicationPosition,
    },
    FenceNotAdvanced {
        current_epoch: Epoch,
    },
    PreviewStale {
        acked: ReplicationPosition,
    },
    SchemaTooNew {
        store: SchemaIdentity,
        supported: u64,
    },
    NotInitialized,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReleaseRequest {
    pub database: DatabaseId,
    pub epoch: Epoch,
    pub transfer: TransferId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MigrationState {
    Pending,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ReleaseRefusal {
    MigrationPending {
        migration: u64,
        state: MigrationState,
    },
    SchemaTooNew {
        store: SchemaIdentity,
        supported: Option<u64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ReleaseAnswer {
    Released {
        #[serde(rename = "final")]
        final_position: ReplicationPosition,
    },
    ReleaseRefused(ReleaseRefusal),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AwaitAcked {
    pub through: ReplicationPosition,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum AwaitAckedAnswer {
    Acked { through: ReplicationPosition },
    NotInitialized,
    Retired,
}
