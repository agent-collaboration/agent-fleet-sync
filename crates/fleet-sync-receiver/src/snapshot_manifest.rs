//! Position-pinned row counts and digests over the same pages used for snapshots.
use crate::{ReceiverError, StoreError, copy_lifecycle::following_status, snapshot_page};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{
    ReplicationPosition, SnapshotManifest, SnapshotManifestAnswer, SnapshotRead,
    TableDigestBuilder, TableManifest, TableName,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Read the replicated tables' manifest inside the host's read transaction.
#[allow(clippy::manual_async_fn)]
pub fn snapshot_manifest<'scope, S>(
    scope: &'scope mut S,
    position: ReplicationPosition,
) -> impl std::future::Future<Output = Result<SnapshotManifestAnswer, ReceiverError>> + Send + 'scope
where
    S: TransactionScope + 'scope,
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
    async move {
        let status = match following_status(scope).await {
            Ok(status) => status,
            Err(ReceiverError::NotInitialized) => {
                return Ok(SnapshotManifestAnswer::NotInitialized);
            }
            Err(ReceiverError::Retired) => return Ok(SnapshotManifestAnswer::Retired),
            Err(error) => return Err(error),
        };
        if status.acked != position {
            return Ok(SnapshotManifestAnswer::Moved {
                position: status.acked,
            });
        }
        let mut tables = Vec::new();
        let mut cursor = None;
        let mut digest = TableDigestBuilder::new();
        let mut row_count = 0u64;
        loop {
            let SnapshotRead::Page(page) = snapshot_page(scope, cursor).await? else {
                return Err(StoreError::Corrupt {
                    field: "snapshot moved within manifest transaction".into(),
                }
                .into());
            };
            if page.table.is_empty() {
                break;
            }
            for row in &page.rows {
                digest
                    .push_row(row)
                    .map_err(|error| ReceiverError::InvalidInput {
                        field: error.to_string(),
                    })?;
                row_count = row_count
                    .checked_add(1)
                    .ok_or_else(|| StoreError::Corrupt {
                        field: "snapshot row count overflow".into(),
                    })?;
            }
            if page
                .next
                .as_ref()
                .is_none_or(|next| next.table != page.table)
            {
                tables.push(TableManifest {
                    table: TableName::new(&page.table).map_err(|error| {
                        ReceiverError::InvalidInput {
                            field: error.to_string(),
                        }
                    })?,
                    row_count,
                    digest: digest.finish(),
                });
                digest = TableDigestBuilder::new();
                row_count = 0;
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        Ok(SnapshotManifestAnswer::Manifest(SnapshotManifest {
            database: status.database,
            position,
            schema: status.schema,
            tables,
        }))
    }
}
