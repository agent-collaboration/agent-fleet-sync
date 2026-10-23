use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{kind} must be a canonical UUIDv7 (correlation identities must be non-nil)")]
pub struct IdentityError {
    kind: &'static str,
}

macro_rules! uuid_id {
    ($valid:expr, $text_valid:expr; $($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(Uuid);

        impl $name {
            pub fn new(value: Uuid) -> Result<Self, IdentityError> {
                if !($valid)(value) {
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
        impl TryFrom<String> for $name {
            type Error = IdentityError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                if !($text_valid)(&value) {
                    return Err(IdentityError { kind: stringify!($name) });
                }
                let parsed = Uuid::parse_str(&value)
                    .map_err(|_| IdentityError { kind: stringify!($name) })?;
                Self::new(parsed)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self { value.0.to_string() }
        }
        impl std::str::FromStr for $name {
            type Err = IdentityError;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::try_from(value.to_owned())
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }
    )+};
}

uuid_id!(runtime_primitives::uuid_v7, runtime_primitives::canonical_uuid_v7;
    DatabaseId,
    PeerId,
    ReceiverId,
    SessionId,
    RequestId,
    TransferId,
    SeedAttemptId,
    InstallId,
    InstallAttemptId
);
uuid_id!(|value: Uuid| !value.is_nil(), |_: &str| true; ConnectionId, FrameId);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SeedId {
    pub generation: u64,
}

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
