//! Read-only proof that a Copy's retained receiver records decode through the
//! receiver's own readers and validators, unchanged. For a host that must show
//! storage it didn't write is readable before acting on it (a migration's
//! candidate, before its first write). Compiled only with the `inspect` feature.
//!
//! Each family goes through the reader the receiver itself uses for it:
//! the Copy record and creation request; every fence receipt at its own
//! address, by its transfer (`fence_receipt`) and by the epoch it ended
//! (`epoch_end`), each agreeing with the row's own request, plus the carried
//! fence set a follower hands to a snapshot install (`read_old_fence`); the seed
//! record and its pages; each seed attempt by its id; the install marker and its
//! page receipts; and each staged transaction's parts as apply reads them
//! (`CopyStorage::parts`: payload, part metadata, index, transaction, digest).
//! The answer counts what each family's readers decoded, so the host can compare
//! it with the rows it holds: a row the receiver never reads is never counted.

use crate::{
    CopyInspection, ReceiverError, SnapshotInstallRules, StoreError, checked_inspect_queries,
    checked_queries,
    copy_contract::CopyRecord,
    copy_storage::{CopyStorage, decode_record, encode_record},
    inspect_copy,
    seed_storage::SeedStorage,
    snapshot_install_storage::InstallStorage,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{CopyRole, Epoch, FenceRequest, PeerId, SeedAttemptId};
use serde::{Serialize, de::DeserializeOwned};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// One receiver record family, named by the table that holds it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetainedFamily {
    Copy,
    Creation,
    FenceReceipts,
    Seed,
    SeedPages,
    SeedAttempts,
    Install,
    InstallPages,
    StagedParts,
}

impl RetainedFamily {
    pub const fn table(self) -> &'static str {
        match self {
            Self::Copy => "_fleet_sync_copy",
            Self::Creation => "_fleet_sync_creation",
            Self::FenceReceipts => "_fleet_sync_fence_receipts",
            Self::Seed => "_fleet_sync_seed",
            Self::SeedPages => "_fleet_sync_seed_pages",
            Self::SeedAttempts => "_fleet_sync_seed_attempts",
            Self::Install => "_fleet_sync_install",
            Self::InstallPages => "_fleet_sync_install_pages",
            Self::StagedParts => "_fleet_sync_staged_parts",
        }
    }
}

/// How many records of each family the receiver's readers decoded.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RetainedRecords {
    pub creation: usize,
    pub fence_receipts: usize,
    pub seed: usize,
    pub seed_pages: usize,
    pub seed_attempts: usize,
    pub install: usize,
    pub install_pages: usize,
    pub staged_parts: usize,
}

/// The first record a receiver reader refused: its family, the row as the
/// reader addresses it, and the reader's own error.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{table} {row}: {detail}", table = .family.table())]
pub struct RetainedRecordRefusal {
    pub family: RetainedFamily,
    pub row: String,
    pub detail: String,
}

fn refusal(
    family: RetainedFamily,
    row: impl Into<String>,
) -> impl FnOnce(String) -> RetainedRecordRefusal {
    let row = row.into();
    move |detail| RetainedRecordRefusal {
        family,
        row,
        detail,
    }
}

fn corrupt(field: &str) -> String {
    StoreError::Corrupt {
        field: field.into(),
    }
    .to_string()
}

/// Reads every retained receiver record of the Copy in `scope` and writes
/// nothing. `A` is the consumer annotation apply reads staged parts with.
pub async fn inspect_retained_records<S, A>(
    scope: &mut S,
) -> Result<RetainedRecords, RetainedRecordRefusal>
where
    S: TransactionScope + Send,
    A: Serialize + DeserializeOwned,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> f64: Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Vec<u8>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    use RetainedFamily as Family;
    let copy = CopyStorage(scope)
        .load()
        .await
        .map_err(|error| refusal(Family::Copy, "record")(error.to_string()))?
        .ok_or_else(|| {
            refusal(Family::Copy, "record")(ReceiverError::NotInitialized.to_string())
        })?;
    let mut records = RetainedRecords {
        creation: CopyStorage(scope)
            .creation()
            .await
            .map_err(|error| refusal(Family::Creation, "singleton")(error.to_string()))?
            .map_or(0, |_| 1),
        ..RetainedRecords::default()
    };

    let fences = checked_queries::read_all_fences(scope)
        .await
        .map_err(|error| refusal(Family::FenceReceipts, "all")(error.to_string()))?;
    for (index, row) in fences.into_iter().enumerate() {
        let label = row
            .transfer
            .clone()
            .unwrap_or_else(|| format!("row {index}"));
        let refuse = |detail: String| RetainedRecordRefusal {
            family: Family::FenceReceipts,
            row: label.clone(),
            detail,
        };
        // Each row at its own address, as the carried-fence reader addresses
        // it: its own transfer and its own ended epoch, both agreeing with the
        // request it holds, so one row's request can't stand in for another's.
        let request: FenceRequest = decode_record(row.request, "fence receipt request")
            .map_err(|error| refuse(error.to_string()))?;
        let old_epoch = row
            .old_epoch
            .as_deref()
            .and_then(|value| value.parse::<u64>().ok())
            .and_then(|epoch| Epoch::new(epoch).ok())
            .ok_or_else(|| refuse(corrupt("fence receipt epoch")))?;
        let old_holder: Option<PeerId> = row
            .old_holder
            .map(|value| decode_record(Some(value), "fence receipt holder"))
            .transpose()
            .map_err(|error| refuse(error.to_string()))?;
        let transfer =
            encode_record(&request.transfer_id).map_err(|error| refuse(error.to_string()))?;
        if row.transfer.as_deref() != Some(transfer.as_str()) || old_epoch != request.expect.epoch {
            return Err(refuse(corrupt("fence receipt identity")));
        }
        let result = CopyStorage(scope)
            .fence_receipt(&request)
            .await
            .map_err(|error| refuse(error.to_string()))?
            .ok_or_else(|| refuse(corrupt("fence receipt")))?;
        let result = encode_record(&result).map_err(|error| refuse(error.to_string()))?;
        if row.result.as_deref() != Some(result.as_str()) {
            return Err(refuse(corrupt("fence receipt identity")));
        }
        if CopyStorage(scope)
            .epoch_end(old_epoch)
            .await
            .map_err(|error| refuse(error.to_string()))?
            != Some((old_holder, request.expect.acked))
        {
            return Err(refuse(corrupt("epoch fence")));
        }
        records.fence_receipts += 1;
    }
    let follower = match &copy {
        CopyRecord::Following(status) | CopyRecord::Starting { status, .. } => {
            status.role != CopyRole::Authoritative
        }
        CopyRecord::Seeding { .. } | CopyRecord::Retired { .. } => false,
    };
    if follower {
        SnapshotInstallRules(&mut *scope)
            .read_old_fence()
            .await
            .map_err(|error| refusal(Family::FenceReceipts, "carried fences")(error.to_string()))?;
    }

    if let Some(seed) = SeedStorage(scope)
        .load(copy.database())
        .await
        .map_err(|error| refusal(Family::Seed, "singleton")(error.to_string()))?
    {
        records.seed = 1;
        records.seed_pages = SeedStorage(scope)
            .pages(seed.seed)
            .await
            .map_err(|error| {
                refusal(
                    Family::SeedPages,
                    format!("generation {}", seed.seed.generation),
                )(error.to_string())
            })?
            .len();
    }
    let attempts = checked_inspect_queries::read_seed_attempt_ids(scope)
        .await
        .map_err(|error| refusal(Family::SeedAttempts, "all")(error.to_string()))?;
    for (index, row) in attempts.into_iter().enumerate() {
        let label = row
            .attempt
            .clone()
            .unwrap_or_else(|| format!("row {index}"));
        let attempt = row
            .attempt
            .as_deref()
            .ok_or_else(|| corrupt("seed attempt id"))
            .and_then(|text| {
                text.parse::<SeedAttemptId>()
                    .map_err(|error| error.to_string())
            })
            .map_err(refusal(Family::SeedAttempts, label.as_str()))?;
        SeedStorage(scope)
            .attempt(attempt)
            .await
            .map_err(|error| refusal(Family::SeedAttempts, label.as_str())(error.to_string()))?
            .ok_or_else(|| {
                refusal(Family::SeedAttempts, label.as_str())(corrupt("seed attempt"))
            })?;
        records.seed_attempts += 1;
    }

    if let Some(marker) = InstallStorage(scope)
        .optional()
        .await
        .map_err(|error| refusal(Family::Install, "singleton")(error.to_string()))?
    {
        records.install = 1;
        let install = marker.install.install_id;
        records.install_pages = InstallStorage(scope)
            .pages(install)
            .await
            .map_err(|error| refusal(Family::InstallPages, install.to_string())(error.to_string()))?
            .len();
    }

    let transactions = match inspect_copy(scope).await {
        CopyInspection::Live { staged_parts, .. } => staged_parts,
        CopyInspection::Retired { .. } => Vec::new(),
        CopyInspection::Damaged { detail } => {
            return Err(refusal(Family::StagedParts, "inspection")(detail));
        }
        CopyInspection::Uninitialized => {
            return Err(refusal(Family::Copy, "inspection")(
                ReceiverError::NotInitialized.to_string(),
            ));
        }
    };
    for staged in transactions {
        let key = staged.transaction_key;
        let label = encode_record(&key).unwrap_or_else(|_| format!("{key:?}"));
        records.staged_parts += CopyStorage(scope)
            .parts::<A>(key)
            .await
            .map_err(|error| refusal(Family::StagedParts, label.as_str())(error.to_string()))?
            .len();
    }
    Ok(records)
}
