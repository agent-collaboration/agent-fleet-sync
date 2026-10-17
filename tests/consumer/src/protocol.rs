use durable_steps::CallId;
use fleet_sync_protocol::*;
use fleet_sync_writer::{Migration, ReplicatedSchema};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

pub struct SampleProtocol;
impl ConsumerProtocol for SampleProtocol {
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
    extended: bool,
}
impl SampleSchema {
    pub fn new() -> Result<Self, TextValueError> {
        Ok(Self {
            tables: TABLES.as_ref().map(Vec::as_slice).map_err(Clone::clone)?,
            extended: false,
        })
    }
    pub fn extended() -> Result<Self, TextValueError> {
        Ok(Self {
            extended: true,
            ..Self::new()?
        })
    }
}
impl ReplicatedSchema for SampleSchema {
    fn migrations(&self) -> &'static [Migration] {
        if self.extended {
            &EXTENDED_MIGRATIONS
        } else {
            MIGRATIONS.iter().as_slice()
        }
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
