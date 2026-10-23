//! Read-only operations facts. This projection does not decide whether a copy is readable.
use crate::{
    ReceiverError, StoreError, checked_queries,
    copy_contract::CopyRecord,
    copy_storage::{CopyStorage, decode_record},
    schema_reads::{SchemaReads, required_text},
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{CopyStatus, SyncTime, TransactionKey};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CopyStage {
    Starting,
    Seeding,
    Following,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StagedPart {
    pub index: u32,
    pub staged_at: SyncTime,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StagedTransaction {
    pub transaction_key: TransactionKey,
    pub parts: Vec<StagedPart>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum CopyInspection {
    Uninitialized,
    Damaged {
        detail: String,
    },
    Live {
        stage: CopyStage,
        status: CopyStatus,
        fingerprint: String,
        staged_parts: Vec<StagedTransaction>,
    },
    Retired {
        retired_at: SyncTime,
    },
}

fn staged_transactions(
    rows: Vec<checked_queries::InspectionPartRow>,
) -> Result<Vec<StagedTransaction>, StoreError> {
    let mut transactions: BTreeMap<TransactionKey, BTreeMap<u32, SyncTime>> = BTreeMap::new();
    for row in rows {
        let key = required_text(row.transaction_key, row.key_class, "staged transaction key")?;
        let transaction_key: TransactionKey = decode_record(Some(key), "staged transaction key")?;
        if row.index_class.as_deref() != Some("integer") {
            return Err(StoreError::Corrupt {
                field: "staged part index".into(),
            });
        }
        let index = row
            .part_index
            .and_then(|index| index.parse::<u32>().ok())
            .ok_or_else(|| StoreError::Corrupt {
                field: "staged part index".into(),
            })?;
        let staged_at = required_text(row.staged_at, row.time_class, "staged part time")?
            .parse::<i64>()
            .map_err(|_| StoreError::Corrupt {
                field: "staged part time".into(),
            })?;
        if transactions
            .entry(transaction_key)
            .or_default()
            .insert(index, SyncTime::from_unix_millis(staged_at))
            .is_some()
        {
            return Err(StoreError::Corrupt {
                field: "duplicate staged part".into(),
            });
        }
    }
    Ok(transactions
        .into_iter()
        .map(|(transaction_key, parts)| StagedTransaction {
            transaction_key,
            parts: parts
                .into_iter()
                .map(|(index, staged_at)| StagedPart { index, staged_at })
                .collect(),
        })
        .collect())
}

/// Inspect the current receiver corpus without creating tables or changing rows.
/// Host migration history stays outside this projection; Live is not a readiness claim.
#[allow(clippy::manual_async_fn)]
pub fn inspect_copy<'scope, S>(
    scope: &'scope mut S,
) -> impl std::future::Future<Output = CopyInspection> + Send + 'scope
where
    S: TransactionScope + 'scope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        let result: Result<CopyInspection, ReceiverError> = async {
            if !crate::inspection_corpus::current_corpus_present(scope).await? {
                return Ok(CopyInspection::Uninitialized);
            }
            let Some(record) = CopyStorage(scope).load().await? else {
                return Ok(CopyInspection::Uninitialized);
            };
            let (stage, status) = match record {
                CopyRecord::Retired { retired_at, .. } => {
                    return Ok(CopyInspection::Retired { retired_at });
                }
                CopyRecord::Starting { status, .. } => (CopyStage::Starting, status),
                CopyRecord::Seeding { status, .. } => (CopyStage::Seeding, status),
                CopyRecord::Following(status) => (CopyStage::Following, status),
            };
            let fingerprint = SchemaReads(scope).fingerprint().await?;
            let rows = checked_queries::read_inspection_parts(scope)
                .await
                .map_err(crate::sql_transport::sql_error)?;
            Ok(CopyInspection::Live {
                stage,
                status,
                fingerprint,
                staged_parts: staged_transactions(rows)?,
            })
        }
        .await;
        match result {
            Ok(inspection) => inspection,
            Err(error) => CopyInspection::Damaged {
                detail: error.to_string(),
            },
        }
    }
}
