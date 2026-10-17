use durable_steps::CallId;
use fleet_sync_protocol::*;
use fleet_sync_writer::{Migration, ReplicatedSchema};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

pub struct SampleProtocol;
impl ConsumerProtocol for SampleProtocol {
    type Delivery = crate::SampleDelivery;
    type Request = SampleRequest;
    type Answer = SampleAnswer;
    type ServerRequest = String;
    type ClientAnswer = String;
    type Notice = String;
    type Annotation = String;
}
#[derive(Debug, Serialize, Deserialize)]
pub enum SampleRequest {
    Echo { text: String },
}
#[derive(Debug, Serialize, Deserialize)]
pub enum SampleAnswer {
    Echo { peer: PeerId, text: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HookEffect {
    pub database: DatabaseId,
    pub position: ReplicationPosition,
    pub event_count: u64,
    pub annotation: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SavedEffect {
    pub call: CallId,
    pub effect: HookEffect,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EffectReceipt {
    pub saved: SavedEffect,
    pub attempts: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClaimSettlementReceipt {
    pub call: CallId,
    pub outcome: SettleOutcome,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClaimGateReceipt {
    pub request: HolderChangeRequest,
    pub reserved_by: Option<CallId>,
    pub settlement: Option<ClaimSettlementReceipt>,
    pub reserve_attempts: u64,
    pub settle_attempts: u64,
}

static MIGRATIONS: sqlx_core::migrate::Migrator = sqlx::migrate!("./replicated-migrations");
static LATER_MIGRATIONS: sqlx_core::migrate::Migrator = sqlx::migrate!("./later-migrations");
static EXTENDED_MIGRATIONS: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    MIGRATIONS
        .iter()
        .chain(LATER_MIGRATIONS.iter())
        .cloned()
        .collect()
});
static TABLES: LazyLock<Result<Vec<TableName>, TextValueError>> = LazyLock::new(|| {
    ["samples", "readings", "samples_by_source"]
        .into_iter()
        .map(TableName::new)
        .collect()
});
pub struct SampleSchema {
    tables: &'static [TableName],
    migrations: &'static [Migration],
}
impl SampleSchema {
    pub fn new() -> Result<Self, TextValueError> {
        Ok(Self {
            tables: TABLES.as_ref().map(Vec::as_slice).map_err(Clone::clone)?,
            migrations: MIGRATIONS.iter().as_slice(),
        })
    }
    pub fn extended() -> Result<Self, TextValueError> {
        Ok(Self {
            migrations: &EXTENDED_MIGRATIONS,
            ..Self::new()?
        })
    }
    pub fn rename_prefix() -> Result<Self, TextValueError> {
        Self::renaming(false)
    }
    pub fn rename_bundle() -> Result<Self, TextValueError> {
        Self::renaming(true)
    }
    fn renaming(complete: bool) -> Result<Self, TextValueError> {
        static MIGRATIONS: LazyLock<Vec<Migration>> = LazyLock::new(|| {
            use sqlx_core::{migrate::MigrationType, sql_str::SqlSafeStr};
            [
                "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,value TEXT); INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001','before')",
                "ALTER TABLE records RENAME TO tasks",
            ].into_iter().zip(1_i64..).map(|(sql, version)| Migration::new(version,"rename".into(),MigrationType::Simple,sql.into_sql_str(),false)).collect()
        });
        static TABLES: LazyLock<Result<[Vec<TableName>; 2], TextValueError>> =
            LazyLock::new(|| {
                Ok([
                    vec![TableName::new("records")?],
                    vec![TableName::new("tasks")?],
                ])
            });
        let index = usize::from(complete);
        Ok(Self {
            tables: &TABLES.as_ref().map_err(Clone::clone)?[index],
            migrations: &MIGRATIONS[..index + 1],
        })
    }
}
impl ReplicatedSchema for SampleSchema {
    fn migrations(&self) -> &'static [Migration] {
        self.migrations
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
