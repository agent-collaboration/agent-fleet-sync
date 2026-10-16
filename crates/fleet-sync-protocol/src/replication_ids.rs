use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{kind} must be a non-nil UUID")]
pub struct IdentityError {
    kind: &'static str,
}

macro_rules! uuid_id {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
        #[serde(try_from = "Uuid", into = "Uuid")]
        pub struct $name(Uuid);

        impl $name {
            pub fn new(value: Uuid) -> Result<Self, IdentityError> {
                if value.is_nil() {
                    return Err(IdentityError { kind: stringify!($name) });
                }
                Ok(Self(value))
            }
            pub const fn as_uuid(self) -> Uuid { self.0 }
        }
        impl TryFrom<Uuid> for $name {
            type Error = IdentityError;
            fn try_from(value: Uuid) -> Result<Self, Self::Error> { Self::new(value) }
        }
        impl From<$name> for Uuid {
            fn from(value: $name) -> Uuid { value.0 }
        }
    )+};
}

uuid_id!(
    DatabaseId,
    PeerId,
    ReceiverId,
    SessionId,
    ConnectionId,
    FrameId,
    RequestId,
    TransferId,
    SeedId
);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("epoch must be at least 1")]
pub struct EpochError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct Epoch(u64);

impl Epoch {
    pub fn new(value: u64) -> Result<Self, EpochError> {
        (value > 0).then_some(Self(value)).ok_or(EpochError)
    }
    pub const fn value(self) -> u64 {
        self.0
    }
}
impl TryFrom<u64> for Epoch {
    type Error = EpochError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<Epoch> for u64 {
    fn from(value: Epoch) -> Self {
        value.0
    }
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct CommitSequence(u64);

impl CommitSequence {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct NoticePosition(u64);

impl NoticePosition {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    pub const fn value(self) -> u64 {
        self.0
    }
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}
