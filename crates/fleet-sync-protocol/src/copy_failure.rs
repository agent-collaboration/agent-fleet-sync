use crate::CopyUnavailableReason;
use serde::{Deserialize, Serialize};

/// Bounded human-readable context. Consumers branch on the reason, never this text.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DiagnosticText(String);

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("diagnostic text must contain 1 to 1024 characters")]
pub struct DiagnosticTextError;

impl DiagnosticText {
    pub fn new(value: impl Into<String>) -> Result<Self, DiagnosticTextError> {
        let value = value.into();
        if value.is_empty() || value.chars().count() > 1024 {
            return Err(DiagnosticTextError);
        }
        Ok(Self(value))
    }
    /// Host-side formatting cannot create an invalid or unbounded wire value.
    pub fn from_error(error: impl std::fmt::Display) -> Self {
        let value: String = error.to_string().chars().take(1024).collect();
        Self(if value.is_empty() {
            "store boundary failed".into()
        } else {
            value
        })
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for DiagnosticText {
    type Error = DiagnosticTextError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<DiagnosticText> for String {
    fn from(value: DiagnosticText) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum OutcomeUnknownReason {
    StoreBoundary { detail: DiagnosticText },
}

#[derive(Debug, thiserror::Error)]
#[error("unknown or malformed copy retry reason")]
pub struct CopyRetryCodecError;

impl CopyUnavailableReason {
    /// The sole codec for the entire reason in a generic durable call failure.
    pub fn encode_retry(&self) -> (String, String) {
        match self {
            Self::Uninitialized => ("copy_uninitialized".into(), String::new()),
            Self::StorageFull => ("copy_storage_full".into(), String::new()),
            Self::MigrationPending { version } => {
                ("copy_migration_pending".into(), version.to_string())
            }
            Self::Damaged { detail } => ("copy_damaged".into(), detail.clone()),
        }
    }
    pub fn decode_retry(code: &str, detail: &str) -> Result<Self, CopyRetryCodecError> {
        match code {
            "copy_uninitialized" if detail.is_empty() => Ok(Self::Uninitialized),
            "copy_storage_full" if detail.is_empty() => Ok(Self::StorageFull),
            "copy_migration_pending" => {
                let version = detail.parse::<u64>().map_err(|_| CopyRetryCodecError)?;
                if version.to_string() != detail {
                    return Err(CopyRetryCodecError);
                }
                Ok(Self::MigrationPending { version })
            }
            "copy_damaged" => Ok(Self::Damaged {
                detail: detail.into(),
            }),
            _ => Err(CopyRetryCodecError),
        }
    }
}
