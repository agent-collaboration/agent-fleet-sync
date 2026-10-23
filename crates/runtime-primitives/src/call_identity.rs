use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{field} must not be empty")]
pub struct InvalidName {
    pub field: &'static str,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct StepName(String);

impl StepName {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidName> {
        let value = value.into();
        if value.is_empty() {
            return Err(InvalidName { field: "step_name" });
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for StepName {
    type Error = InvalidName;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<StepName> for String {
    fn from(value: StepName) -> Self {
        value.0
    }
}

/// Semantic validation for identities minted inside an owning host.
pub fn uuid_v7(value: Uuid) -> bool {
    value.get_version_num() == 7 && value.get_variant() == uuid::Variant::RFC4122
}

/// Text must be checked before parsing can normalize its spelling.
pub fn canonical_uuid_v7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|id| uuid_v7(id) && id.hyphenated().to_string() == value)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("operation identity must be a canonical UUIDv7")]
pub struct InvalidOperationId;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperationId(Uuid);

impl OperationId {
    pub fn new(value: Uuid) -> Result<Self, InvalidOperationId> {
        uuid_v7(value)
            .then_some(Self(value))
            .ok_or(InvalidOperationId)
    }
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}
impl TryFrom<String> for OperationId {
    type Error = InvalidOperationId;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if !canonical_uuid_v7(&value) {
            return Err(InvalidOperationId);
        }
        Self::new(Uuid::parse_str(&value).map_err(|_| InvalidOperationId)?)
    }
}
impl From<OperationId> for String {
    fn from(value: OperationId) -> Self {
        value.0.to_string()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallId {
    pub operation: OperationId,
    pub step: StepName,
}
