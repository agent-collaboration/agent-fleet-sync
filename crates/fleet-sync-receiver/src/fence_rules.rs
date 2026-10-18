use crate::{
    CopyStatus, ReceiverError, StoreError, copy_contract::CopyRecord, copy_storage::CopyStorage,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{FenceRequest, FenceResult};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

// Explicit Send is required by borrowed-scope host dispatch.
#[allow(clippy::manual_async_fn)]
pub fn fence<'scope, S>(
    scope: &'scope mut S,
    request: &'scope FenceRequest,
) -> impl std::future::Future<Output = Result<FenceResult, ReceiverError>> + Send + 'scope
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
        let mut storage = CopyStorage(scope);
        let Some(record) = storage.load().await? else {
            return Ok(FenceResult::NotInitialized);
        };
        let mut status = match record {
            CopyRecord::Following(status) => status,
            CopyRecord::Starting { .. } | CopyRecord::Seeding { .. } => {
                return Ok(FenceResult::NotInitialized);
            }
            CopyRecord::Retired { .. } => return Ok(FenceResult::Retired),
        };
        if status.database != request.database {
            return Ok(FenceResult::NotInitialized);
        }
        if let Some(receipt) = storage.fence_receipt(request).await? {
            validate_replayed_fence(&status, request, &receipt)?;
            return Ok(receipt);
        }
        if request.new_epoch <= status.fence {
            return Ok(FenceResult::FenceNotAdvanced {
                current_epoch: status.fence,
            });
        }
        if request.expect.epoch != status.fence || request.expect.acked != status.acked {
            return Ok(FenceResult::PreviewStale {
                acked: status.acked,
            });
        }
        let supported = request.target_schema.map(|support| support.max_version);
        if supported.is_none_or(|maximum| status.schema.version > maximum) {
            return Ok(FenceResult::SchemaTooNew {
                store: status.schema,
                supported,
            });
        }
        let result = FenceResult::Fenced {
            old_epoch: status.fence,
            new_epoch: request.new_epoch,
            start: status.acked,
        };
        let old_holder = status.holder;
        status.fence = request.new_epoch;
        status.holder = Some(request.holder);
        status.start = status.acked;
        storage.save(&CopyRecord::Following(status)).await?;
        storage.save_fence(request, old_holder, &result).await?;
        Ok(result)
    }
}

fn validate_replayed_fence(
    status: &CopyStatus,
    request: &FenceRequest,
    result: &FenceResult,
) -> Result<(), StoreError> {
    // CopyStorage validates the saved request/result tuple before this live-state check.
    let FenceResult::Fenced { new_epoch, .. } = result else {
        return Err(StoreError::Corrupt {
            field: "fence receipt".into(),
        });
    };
    if *new_epoch != request.new_epoch
        || status.fence < *new_epoch
        || (status.fence == *new_epoch && status.holder != Some(request.holder))
    {
        return Err(StoreError::Corrupt {
            field: "fence receipt state".into(),
        });
    }
    Ok(())
}
