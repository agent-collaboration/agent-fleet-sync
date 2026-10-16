use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::DigestError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaObjectKind {
    Index,
    Table,
    Trigger,
    View,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown schema object kind")]
pub struct SchemaObjectKindError;

impl TryFrom<&str> for SchemaObjectKind {
    type Error = SchemaObjectKindError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "index" => Ok(Self::Index),
            "table" => Ok(Self::Table),
            "trigger" => Ok(Self::Trigger),
            "view" => Ok(Self::View),
            _ => Err(SchemaObjectKindError),
        }
    }
}

impl SchemaObjectKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Index => "index",
            Self::Table => "table",
            Self::Trigger => "trigger",
            Self::View => "view",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaEntry {
    pub kind: SchemaObjectKind,
    pub name: String,
    pub sql: String,
}

pub fn schema_fingerprint(entries: &[SchemaEntry]) -> Result<String, DigestError> {
    let mut normalized: Vec<_> = entries
        .iter()
        .filter(|entry| !crate::reserved_namespace(&entry.name))
        .map(|entry| {
            Ok((
                entry.kind.as_str(),
                entry.name.as_str(),
                crate::schema_sql::canonical_schema_sql(&entry.sql)?,
            ))
        })
        .collect::<Result<_, DigestError>>()?;
    normalized.sort_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
    let encoded = serde_json::to_vec(&normalized)
        .map_err(|error| DigestError::Encoding(error.to_string()))?;
    let hash = Sha256::digest(encoded);
    Ok(format!("{hash:x}"))
}
