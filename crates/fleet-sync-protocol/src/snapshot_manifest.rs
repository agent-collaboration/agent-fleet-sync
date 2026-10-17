use crate::{DatabaseId, ReplicationPosition, SchemaIdentity, TableDigest, TableName};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TableManifest {
    pub table: TableName,
    pub row_count: u64,
    pub digest: TableDigest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotManifest {
    pub database: DatabaseId,
    pub position: ReplicationPosition,
    pub schema: SchemaIdentity,
    pub tables: Vec<TableManifest>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SnapshotManifestAnswer {
    Manifest(SnapshotManifest),
    Moved { position: ReplicationPosition },
    NotInitialized,
    Retired,
}
