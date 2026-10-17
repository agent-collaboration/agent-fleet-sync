use crate::{
    SnapshotInstall, SnapshotInstallError, SnapshotInstallRules, StoreError, checked_queries,
    copy_contract::CopyRecord,
    copy_storage::{CopyStorage, decode_record, encode_record},
    snapshot_install::{check_identity, starting_status},
    snapshot_install_storage::InstallStorage,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{
    CopyRole, CopyStatus, CreateCopy, Epoch, FenceRequest, FenceResult, PeerId,
};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Only the target copy's owner reads this carrier, immediately before publish.
/// The replication position and rows come from the verified snapshot instead.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotFence {
    status: CopyStatus,
    creation: Option<CreateCopy>,
    receipts: Vec<StoredFence>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredFence {
    request: FenceRequest,
    old_holder: Option<PeerId>,
    result: FenceResult,
}

impl<S> SnapshotInstallRules<'_, S>
where
    S: TransactionScope + Send,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'a> String: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Option<String>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> f64: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Vec<u8>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub async fn read_old_fence(&mut self) -> Result<Option<SnapshotFence>, SnapshotInstallError> {
        let status = match CopyStorage(self.0).load().await? {
            None => return Ok(None),
            Some(CopyRecord::Following(status)) | Some(CopyRecord::Starting { status, .. }) => {
                status
            }
            Some(CopyRecord::Retired { .. }) => return Err(SnapshotInstallError::Retired),
            Some(CopyRecord::Seeding { .. }) => {
                return Err(SnapshotInstallError::AuthoritativeCopy);
            }
        };
        if status.role == CopyRole::Authoritative {
            return Err(SnapshotInstallError::AuthoritativeCopy);
        }
        let creation = CopyStorage(self.0).creation().await?;
        let mut receipts = Vec::new();
        for row in checked_queries::read_all_fences(self.0).await? {
            let request: FenceRequest = decode_record(row.request, "carried fence request")?;
            let old_epoch = row
                .old_epoch
                .and_then(|value| value.parse::<u64>().ok())
                .and_then(|epoch| Epoch::new(epoch).ok())
                .ok_or_else(|| StoreError::Corrupt {
                    field: "carried fence epoch".into(),
                })?;
            let old_holder = row
                .old_holder
                .map(|value| decode_record(Some(value), "carried fence holder"))
                .transpose()?;
            let result = CopyStorage(self.0)
                .fence_receipt(&request)
                .await?
                .ok_or_else(|| StoreError::Corrupt {
                    field: "carried fence receipt".into(),
                })?;
            if row.transfer.as_deref() != Some(encode_record(&request.transfer_id)?.as_str())
                || row.result.as_deref() != Some(encode_record(&result)?.as_str())
                || old_epoch != request.expect.epoch
                || CopyStorage(self.0).epoch_end(old_epoch).await?
                    != Some((old_holder, request.expect.acked))
            {
                return Err(StoreError::Corrupt {
                    field: "carried fence identity".into(),
                }
                .into());
            }
            let carried = StoredFence {
                request,
                old_holder,
                result,
            };
            validate_receipt(&status, &carried)?;
            receipts.push(carried);
        }
        Ok(Some(SnapshotFence {
            status,
            creation,
            receipts,
        }))
    }
    pub async fn merge_old_fence(
        &mut self,
        install: &SnapshotInstall,
        old: Option<&SnapshotFence>,
    ) -> Result<(), SnapshotInstallError> {
        let marker = InstallStorage(self.0).load().await?;
        check_identity(&marker, install)?;
        if marker.verified.is_none() {
            return Err(SnapshotInstallError::NotVerified);
        }
        if marker.ready {
            return Err(SnapshotInstallError::IdentityConflict);
        }
        let mut status = starting_status(self.0, install).await?;
        if let Some(old) = old {
            crate::copy_contract::validate_copy_status(&old.status)?;
            if old.status.role == CopyRole::Authoritative {
                return Err(SnapshotInstallError::AuthoritativeCopy);
            }
            if old.status.database != install.database {
                return Err(SnapshotInstallError::IdentityConflict);
            }
            if install.position < old.status.start {
                return Err(SnapshotInstallError::BelowFence {
                    offered: install.position,
                    minimum: old.status.start,
                });
            }
            for receipt in &old.receipts {
                validate_receipt(&old.status, receipt)?;
            }
            if old.status.fence == status.fence
                && (old.status.holder != status.holder || old.status.start != status.start)
            {
                return Err(SnapshotInstallError::FenceConflict);
            }
            if old.status.fence > status.fence {
                status.fence = old.status.fence;
                status.holder = old.status.holder;
                status.start = old.status.start;
            }
            checked_queries::erase_fence_receipts(self.0).await?;
            for receipt in &old.receipts {
                CopyStorage(self.0)
                    .save_fence(&receipt.request, receipt.old_holder, &receipt.result)
                    .await?;
            }
            checked_queries::erase_creation(self.0).await?;
            if let Some(creation) = &old.creation {
                if creation.database != install.database {
                    return Err(SnapshotInstallError::IdentityConflict);
                }
                CopyStorage(self.0).save_creation(creation).await?;
            }
        }
        CopyStorage(self.0)
            .save(&CopyRecord::Starting {
                status,
                install_id: install.install_id,
            })
            .await?;
        Ok(())
    }
}
fn validate_receipt(
    status: &CopyStatus,
    receipt: &StoredFence,
) -> Result<(), SnapshotInstallError> {
    let FenceResult::Fenced {
        old_epoch,
        new_epoch,
        start,
    } = &receipt.result
    else {
        return Err(StoreError::Corrupt {
            field: "carried successful fence".into(),
        }
        .into());
    };
    if receipt.request.database != status.database
        || *old_epoch != receipt.request.expect.epoch
        || *new_epoch != receipt.request.new_epoch
        || *start != receipt.request.expect.acked
        || *new_epoch <= *old_epoch
        || start.epoch > *old_epoch
        || status.fence < *new_epoch
        || (status.fence == *new_epoch
            && (status.holder != Some(receipt.request.holder) || status.start != *start))
    {
        return Err(StoreError::Corrupt {
            field: "carried fence contents".into(),
        }
        .into());
    }
    Ok(())
}

/// Native carry excludes these tables. Their replacement/merge stays in rules;
/// the journal, hook tables and hook migration history remain target-owned.
pub const SNAPSHOT_REPLACED_TABLES: &[&str] = &[
    "_fleet_sync_copy",
    "_fleet_sync_receipts",
    "_fleet_sync_staged_parts",
    "_fleet_sync_fence_receipts",
    "_fleet_sync_schema_steps",
    "_fleet_sync_seed",
    "_fleet_sync_seed_pages",
    "_fleet_sync_seed_attempts",
    "_fleet_sync_creation",
    "_fleet_sync_copy_tables",
    "_fleet_sync_install",
    "_fleet_sync_install_pages",
];
