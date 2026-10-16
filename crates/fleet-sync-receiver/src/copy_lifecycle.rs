use crate::{
    BoundStatement, CopyRole, CopyStatus, ReceiverError, bound_sql::quoted_identifier,
    copy_contract::CopyRecord, copy_storage::CopyStorage, schema_reads::SchemaReads,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{
    CommitSequence, CopyOrigin, CreateCopy, CreateCopyAnswer, ReplicationPosition, RetireCopy,
    RetireCopyAnswer, SchemaIdentity,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Create the authoritative copy before registration grants its first lease.
// Explicit Send is required by borrowed-scope host dispatch.
#[allow(clippy::manual_async_fn)]
pub fn create_copy<'scope, S>(
    scope: &'scope mut S,
    request: &'scope CreateCopy,
) -> impl std::future::Future<Output = Result<CreateCopyAnswer, ReceiverError>> + Send + 'scope
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
        if let Some(copy) = CopyStorage(scope).load().await? {
            if matches!(copy, CopyRecord::Retired { .. }) {
                return Ok(CreateCopyAnswer::Retired);
            }
            if copy.database() != request.database {
                return Err(ReceiverError::InvalidInput {
                    field: "copy already belongs to another database".into(),
                });
            }
            let original =
                CopyStorage(scope)
                    .creation()
                    .await?
                    .ok_or_else(|| crate::StoreError::Corrupt {
                        field: "original copy creation".into(),
                    })?;
            if original != *request {
                return Ok(CreateCopyAnswer::CreationConflict { existing: original });
            }
            return Ok(CreateCopyAnswer::AlreadyCreated);
        }
        let position = match request.origin {
            CopyOrigin::Empty => ReplicationPosition::new(request.epoch, CommitSequence::new(0)),
            CopyOrigin::AwaitingSeed { expected_position } => expected_position.unwrap_or(
                ReplicationPosition::new(request.epoch, CommitSequence::new(0)),
            ),
        };
        let status = CopyStatus {
            database: request.database,
            role: CopyRole::Authoritative,
            fence: request.epoch,
            holder: request.holder,
            start: position,
            acked: position,
            schema: SchemaIdentity::new(0, SchemaReads(scope).fingerprint().await?).map_err(
                |_| ReceiverError::InvalidInput {
                    field: "initial schema".into(),
                },
            )?,
        };
        let record = match request.origin {
            CopyOrigin::Empty => CopyRecord::Following(status),
            CopyOrigin::AwaitingSeed { expected_position } => CopyRecord::Seeding {
                status,
                expected_position,
            },
        };
        CopyStorage(scope).save_creation(request).await?;
        CopyStorage(scope).save(&record).await?;
        Ok(CreateCopyAnswer::CopyCreated)
    }
}

/// Read copy authority and position. A seed becomes readable only after publication.
// Explicit Send is required by borrowed-scope host dispatch.
#[allow(clippy::manual_async_fn)]
pub(crate) fn following_status<'scope, S>(
    scope: &'scope mut S,
) -> impl std::future::Future<Output = Result<CopyStatus, ReceiverError>> + Send + 'scope
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
        match CopyStorage(scope).load().await? {
            Some(CopyRecord::Following(status)) => Ok(status),
            Some(CopyRecord::Retired { .. }) => Err(ReceiverError::Retired),
            _ => Err(ReceiverError::NotInitialized),
        }
    }
}

/// Return terminal domain answers to the host without turning them into transport failures.
// Explicit Send is required by borrowed-scope host dispatch.
#[allow(clippy::manual_async_fn)]
pub fn copy_status<'scope, S>(
    scope: &'scope mut S,
) -> impl std::future::Future<Output = Result<fleet_sync_protocol::CopyStatusAnswer, ReceiverError>>
+ Send
+ 'scope
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
        use fleet_sync_protocol::CopyStatusAnswer;
        match following_status(scope).await {
            Ok(status) => Ok(CopyStatusAnswer::Status(status)),
            Err(ReceiverError::NotInitialized) => Ok(CopyStatusAnswer::NotInitialized),
            Err(ReceiverError::Retired) => Ok(CopyStatusAnswer::Retired),
            Err(error) => Err(error),
        }
    }
}

/// Delete user records and retain the permanent retired marker in the same transaction.
// Explicit Send is required by borrowed-scope host dispatch.
#[allow(clippy::manual_async_fn)]
pub fn retire_copy<'scope, S>(
    scope: &'scope mut S,
    request: &'scope RetireCopy,
    now: fleet_sync_protocol::SyncTime,
) -> impl std::future::Future<Output = Result<RetireCopyAnswer, ReceiverError>> + Send + 'scope
where
    S: TransactionScope + 'scope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> f64: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Vec<u8>: Type<S::Database> + Encode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        let Some(copy) = CopyStorage(scope).load().await? else {
            return Ok(RetireCopyAnswer::NotInitialized);
        };
        let status = match copy {
            CopyRecord::Retired { .. } => return Ok(RetireCopyAnswer::Retired),
            CopyRecord::Seeding { status, .. } | CopyRecord::Following(status) => status,
        };
        if status.database != request.database {
            return Ok(RetireCopyAnswer::NotInitialized);
        }
        if status.fence != request.expect_fence || status.acked != request.expect_acked {
            return Ok(RetireCopyAnswer::Mismatch {
                fence: status.fence,
                acked: status.acked,
            });
        }
        for table in SchemaReads(scope).table_names().await? {
            BoundStatement::new(
                format!("DELETE FROM {}", quoted_identifier(&table)),
                Vec::new(),
            )
            .execute(scope)
            .await?;
        }
        CopyStorage(scope).erase_records().await?;
        CopyStorage(scope)
            .save(&CopyRecord::Retired {
                database: request.database,
                retired_at: now,
            })
            .await?;
        Ok(RetireCopyAnswer::Retired)
    }
}
