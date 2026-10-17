use fleet_sync_protocol::{Epoch, GateReason, SyncTime, TableName, TransferId};

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SchemaForm {
    #[error("primary-key column {column} of {table:?} permits NULL")]
    NullablePrimaryKey { table: TableName, column: String },
    #[error("migration accesses undeclared table {table:?}")]
    UndeclaredTable { table: TableName },
    #[error("local migration changes replicated table {table:?}")]
    ReplicatedTableInLocalMigration { table: TableName },
    #[error("schema version {store} is newer than supported version {supported}")]
    SchemaTooNew { store: u64, supported: u64 },
    #[error("migration statement is not admitted: {statement}: {detail}")]
    InvalidMigration { statement: String, detail: String },
    #[error("migration statement calls nondeterministic function {function}: {statement}")]
    NonDeterministicMigration { statement: String, function: String },
    #[error("replicated table {table:?} has no primary key")]
    NoPrimaryKey { table: TableName },
    #[error("replicated table {table:?} is missing")]
    MissingTable { table: TableName },
    #[error("replicated table has trigger {name}")]
    Trigger { name: String },
    #[error("virtual table {table:?} cannot replicate")]
    VirtualTable { table: TableName },
    #[error("foreign key of {table:?} uses an unsupported action: {constraint}")]
    ForeignKeyAction {
        table: TableName,
        constraint: String,
    },
    #[error("foreign-key metadata for {table:?} cannot be resolved: {detail}")]
    ForeignKeyMetadata { table: TableName, detail: String },
    #[error("table {table:?} contains an orphan referencing {parent:?}")]
    ForeignKeyOrphan { table: TableName, parent: TableName },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdoptionHistory {
    #[error("the file has no SQLx migration history")]
    Missing,
    #[error("migration {version} is not the next bundle version {expected:?}")]
    NonPrefix { version: i64, expected: Option<i64> },
    #[error("migration {version} has a different checksum")]
    Checksum { version: i64 },
    #[error("migration {version} did not succeed")]
    Failed { version: i64 },
    #[error("migration history has an invalid {field}")]
    Malformed { field: String },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OpenError {
    #[error("cannot adopt migration history: {reason}")]
    AdoptionHistory { reason: AdoptionHistory },
    #[error("writer authority refused: {error}")]
    Holder {
        error: fleet_sync_lease::HolderError,
    },
    #[error("captured transaction is invalid: {error}")]
    Capture { error: crate::CaptureError },
    #[error("migration write refused: {refusal}")]
    WriteRefused { refusal: WriteRefusal },
    #[error("existing unmanaged database needs explicit schema-checked adoption")]
    UnmanagedFile,
    #[error("unsupported replicated schema: {form}")]
    SchemaUnsupported { form: SchemaForm },
    #[error("reserved replicated table {table:?}")]
    ReservedTableName { table: TableName },
    #[error("invalid stored field: {field}")]
    Corrupt { field: String },
    #[error("writer storage failed: {message}")]
    Storage { message: String },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WriteRefusal {
    #[error("database has not registered")]
    Provisional,
    #[error("registration rejected: {reason:?}")]
    Rejected { reason: GateReason },
    #[error("statement refused: {form}")]
    StatementRefused { form: crate::StatementForm },
    #[error("writer is being seeded")]
    Seeding,
    #[error("lease {epoch:?} expired at {expired_at:?}")]
    LeaseExpired { epoch: Epoch, expired_at: SyncTime },
    #[error("writer is draining for {transfer:?}")]
    Draining { transfer: TransferId },
    #[error("writer released its lease")]
    Released,
    #[error("writer superseded by epoch {current:?}")]
    Superseded { current: Epoch },
    #[error("peer is not the writer")]
    NotWriter,
    #[error("writer is closed")]
    Closed,
    #[error("writer storage failed: {message}")]
    Storage { message: String },
}

impl From<crate::StatementRefusal> for sqlx_core::Error {
    fn from(refusal: crate::StatementRefusal) -> Self {
        Self::Configuration(Box::new(WriteRefusal::StatementRefused {
            form: refusal.form,
        }))
    }
}

impl WriteRefusal {
    pub fn from_sqlx(error: sqlx_core::Error) -> Self {
        if let sqlx_core::Error::Configuration(source) = &error
            && let Some(refusal) = source.downcast_ref::<Self>()
        {
            return refusal.clone();
        }
        Self::Storage {
            message: error.to_string(),
        }
    }
}

impl From<sqlx_core::Error> for OpenError {
    fn from(error: sqlx_core::Error) -> Self {
        Self::Storage {
            message: error.to_string(),
        }
    }
}

impl OpenError {
    pub(crate) fn corrupt(field: impl Into<String>) -> Self {
        Self::Corrupt {
            field: field.into(),
        }
    }
}
