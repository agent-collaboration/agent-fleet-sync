use crate::*;
use serde::{Deserialize, Serialize};

macro_rules! validated_text {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);
        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, TextValueError> {
                let value = value.into();
                if value.is_empty() { return Err(TextValueError { kind: stringify!($name) }); }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str { &self.0 }
        }
        impl TryFrom<String> for $name {
            type Error = TextValueError;
            fn try_from(value: String) -> Result<Self, Self::Error> { Self::new(value) }
        }
        impl From<$name> for String { fn from(value: $name) -> String { value.0 } }
    )+};
}
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{kind} must not be empty")]
pub struct TextValueError {
    kind: &'static str,
}
validated_text!(TableName, GateReasonCode, HookRefusalCode, FailureCode);

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PreviewToken(String);

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("preview token must be a canonical UUIDv7")]
pub struct PreviewTokenError;

impl PreviewToken {
    pub fn new(value: impl Into<String>) -> Result<Self, PreviewTokenError> {
        let value = value.into();
        canonical_uuid_v7(&value)
            .then_some(Self(value))
            .ok_or(PreviewTokenError)
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for PreviewToken {
    type Error = PreviewTokenError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<PreviewToken> for String {
    fn from(value: PreviewToken) -> Self {
        value.0
    }
}

impl From<crate::DeliveryFailureKind> for FailureCode {
    fn from(value: crate::DeliveryFailureKind) -> Self {
        Self(value.as_str().to_owned())
    }
}

macro_rules! digest_value {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(PieceDigest);
        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, DigestError> { PieceDigest::try_from(value.into()).map(Self) }
            pub fn of_canonical<T: Serialize>(value: &T) -> Result<Self, DigestError> { PieceDigest::of_canonical(value).map(Self) }
            pub fn as_str(&self) -> &str { self.0.as_str() }
        }
    )+};
}
digest_value!(PageDigest, TableDigest, ImportDigest);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SeedPage {
    pub seed: SeedId,
    pub page: u32,
    pub contents: SnapshotPage,
    pub digest: PageDigest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SeedComplete {
    pub database: DatabaseId,
    pub seed: SeedId,
    pub position: ReplicationPosition,
    pub pages: u32,
    pub schema: SchemaIdentity,
    pub table_digests: Vec<(TableName, TableDigest)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum SeedPageAnswer {
    Staged,
    AlreadyStaged,
    SeedConflict,
    NotSeedable,
    Retired,
    StaleSeed,
    UnitTooLarge,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SeedAnswer {
    IntegrityViolation {
        table: TableName,
        references: TableName,
    },
    Published {
        position: ReplicationPosition,
    },
    PositionMismatch {
        expected: ReplicationPosition,
        offered: ReplicationPosition,
    },
    Incomplete {
        missing: Vec<u32>,
    },
    DigestMismatch {
        table: TableName,
    },
    SchemaMismatch,
    Retired,
    StaleSeed,
    Hook(HookRefusal),
}

impl PageDigest {
    pub fn of(page: &SnapshotPage) -> Result<Self, DigestError> {
        Self::of_canonical(page)
    }
}

/// Hash rows in declared primary-key order, retaining each row's declared column order.
/// Streaming uses the same canonical JSON array encoding as `PieceDigest`.
pub struct TableDigestBuilder {
    hasher: sha2::Sha256,
    has_rows: bool,
}
impl TableDigestBuilder {
    pub fn new() -> Self {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(b"[");
        Self {
            hasher,
            has_rows: false,
        }
    }
    pub fn push_row(&mut self, row: &[SqlValue]) -> Result<(), DigestError> {
        use sha2::Digest;
        let encoded =
            serde_json::to_vec(row).map_err(|error| DigestError::Encoding(error.to_string()))?;
        if self.has_rows {
            self.hasher.update(b",");
        }
        self.hasher.update(encoded);
        self.has_rows = true;
        Ok(())
    }
    pub fn finish(mut self) -> TableDigest {
        use sha2::Digest;
        self.hasher.update(b"]");
        TableDigest(PieceDigest::from_sha256(self.hasher.finalize()))
    }
}
impl Default for TableDigestBuilder {
    fn default() -> Self {
        Self::new()
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
pub enum Origin {
    Empty,
    Seeded { position: ReplicationPosition },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum CopyOrigin {
    Empty,
    AwaitingSeed {
        expected_position: Option<ReplicationPosition>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateCopy {
    pub database: DatabaseId,
    pub epoch: Epoch,
    pub holder: Option<PeerId>,
    pub origin: CopyOrigin,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum CreateCopyAnswer {
    CopyCreated,
    AlreadyCreated,
    Retired,
    CreationConflict { existing: CreateCopy },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetireCopy {
    pub database: DatabaseId,
    pub expect_fence: Epoch,
    pub expect_acked: ReplicationPosition,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RetireCopyAnswer {
    Retired,
    NotInitialized,
    Mismatch {
        fence: Epoch,
        acked: ReplicationPosition,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GateReason {
    pub code: GateReasonCode,
    pub detail: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Registration {
    pub attempt: RequestId,
    pub database: DatabaseId,
    pub writer: PeerId,
    pub origin: Origin,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RegistrationRefusal {
    DatabaseRejected { reason: GateReason },
    NotWriter { current: Option<Epoch> },
    Retired,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RegistrationAnswer {
    Registered { lease: LeaseStatus, replayed: bool },
    Refused(RegistrationRefusal),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Import {
    pub attempt: RequestId,
    pub database: DatabaseId,
    pub digest: ImportDigest,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ImportAnswer {
    Imported,
    AlreadyImported,
    ImportConflict {
        existing: Option<ImportDigest>,
        offered: ImportDigest,
    },
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Retirement {
    pub attempt: RequestId,
    pub database: DatabaseId,
    pub expect_fence: Epoch,
    pub expect_acked: ReplicationPosition,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RetirementRefusal {
    CopyNotInitialized,
    LeaseHeld,
    TransferInProgress,
    GateRefused {
        reason: GateReason,
    },
    CopyMismatch {
        fence: Epoch,
        acked: ReplicationPosition,
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
pub enum RetirementAnswer {
    Retired { at: SyncTime },
    Refused(RetirementRefusal),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreviewTakeover {
    pub database: DatabaseId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CopyRole {
    Authoritative,
    Follower,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CopyStatus {
    pub database: DatabaseId,
    pub role: CopyRole,
    pub fence: Epoch,
    pub holder: Option<PeerId>,
    pub start: ReplicationPosition,
    pub acked: ReplicationPosition,
    pub schema: SchemaIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum CopyStatusAnswer {
    Status(CopyStatus),
    NotInitialized,
    Retired,
}
