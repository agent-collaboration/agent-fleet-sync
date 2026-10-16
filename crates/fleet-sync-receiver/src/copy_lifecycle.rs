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
pub async fn create_copy<S>(
    scope: &mut S,
    request: &CreateCopy,
) -> Result<CreateCopyAnswer, ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    if let Some(copy) = CopyStorage(scope).load().await? {
        if matches!(copy, CopyRecord::Retired(_)) {
            return Err(ReceiverError::Retired);
        }
        if copy.status().database != request.database {
            return Err(ReceiverError::InvalidInput {
                field: "copy already belongs to another database".into(),
            });
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
        schema: SchemaIdentity::new(0, SchemaReads(scope).fingerprint().await?).map_err(|_| {
            ReceiverError::InvalidInput {
                field: "initial schema".into(),
            }
        })?,
    };
    let record = match request.origin {
        CopyOrigin::Empty => CopyRecord::Following(status),
        CopyOrigin::AwaitingSeed { expected_position } => CopyRecord::Seeding {
            status,
            expected_position,
        },
    };
    CopyStorage(scope).save(&record).await?;
    Ok(CreateCopyAnswer::CopyCreated)
}

/// Read copy authority and position. A seed becomes readable only after publication.
pub async fn copy_status<S>(scope: &mut S) -> Result<CopyStatus, ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    match CopyStorage(scope).load().await? {
        Some(CopyRecord::Following(status)) => Ok(status),
        Some(CopyRecord::Retired(_)) => Err(ReceiverError::Retired),
        _ => Err(ReceiverError::NotInitialized),
    }
}

/// Delete user records and retain the permanent retired marker in the same transaction.
pub async fn retire_copy<S>(
    scope: &mut S,
    request: &RetireCopy,
) -> Result<RetireCopyAnswer, ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> f64: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Vec<u8>: Type<S::Database> + Encode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let copy = CopyStorage(scope)
        .load()
        .await?
        .ok_or(ReceiverError::NotInitialized)?;
    if copy.status().database != request.database {
        return Err(ReceiverError::NotInitialized);
    }
    if matches!(copy, CopyRecord::Retired(_)) {
        return Ok(RetireCopyAnswer::Retired);
    }
    let status = copy.status();
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
    CopyStorage(scope)
        .save(&CopyRecord::Retired(status.clone()))
        .await?;
    Ok(RetireCopyAnswer::Retired)
}
