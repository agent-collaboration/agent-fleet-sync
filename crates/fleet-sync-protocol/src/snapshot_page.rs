use serde::{Deserialize, Serialize};

use crate::{DatabaseId, ReplicationPosition, SqlValue};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchemaSupport {
    pub max_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "RawSchemaIdentity")]
pub struct SchemaIdentity {
    pub version: u64,
    pub fingerprint: String,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("schema fingerprint must not be empty")]
pub struct SchemaIdentityError;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawSchemaIdentity {
    version: u64,
    fingerprint: String,
}

impl TryFrom<RawSchemaIdentity> for SchemaIdentity {
    type Error = SchemaIdentityError;

    fn try_from(raw: RawSchemaIdentity) -> Result<Self, Self::Error> {
        Self::new(raw.version, raw.fingerprint)
    }
}

impl SchemaIdentity {
    pub fn new(version: u64, fingerprint: String) -> Result<Self, SchemaIdentityError> {
        if fingerprint.is_empty() {
            return Err(SchemaIdentityError);
        }
        Ok(Self {
            version,
            fingerprint,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotCursor {
    pub position: ReplicationPosition,
    pub table: String,
    pub after_key: Vec<SqlValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotPage {
    pub database: DatabaseId,
    pub position: ReplicationPosition,
    pub schema: SchemaIdentity,
    pub schema_steps: Vec<String>,
    pub table: String,
    pub rows: Vec<Vec<SqlValue>>,
    pub next: Option<SnapshotCursor>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum SnapshotRead {
    Page(Box<SnapshotPage>),
    Moved { position: ReplicationPosition },
}
