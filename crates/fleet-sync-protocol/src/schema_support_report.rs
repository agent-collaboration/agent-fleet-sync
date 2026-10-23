use crate::{PeerId, SchemaSupport, SessionId};
use serde::{Deserialize, Serialize};

/// Order assigned by the session owner to an accepted attach, independent of time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct AttachSequence(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("attach sequence must be at least 1")]
pub struct AttachSequenceError;
impl AttachSequence {
    pub fn new(value: u64) -> Result<Self, AttachSequenceError> {
        (value > 0)
            .then_some(Self(value))
            .ok_or(AttachSequenceError)
    }
    pub const fn value(self) -> u64 {
        self.0
    }
}
impl TryFrom<u64> for AttachSequence {
    type Error = AttachSequenceError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<AttachSequence> for u64 {
    fn from(value: AttachSequence) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReportSchemaSupport {
    pub peer: PeerId,
    pub session_id: SessionId,
    pub attach_seq: AttachSequence,
    pub support: SchemaSupport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SchemaSupportRecorded;
