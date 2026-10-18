//! Registry projection of declarations ordered by the session owner's attach sequence.
use crate::{
    LeaseError, checked_queries,
    registry_storage::{decode, encode},
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub async fn accepted_schema_support<S>(
    scope: &mut S,
    peer: PeerId,
) -> Result<Option<ReportSchemaSupport>, LeaseError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database> + Decode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Encode<'v, S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    let Some(row) = checked_queries::read_schema_support(scope, peer.to_string()).await? else {
        return Ok(None);
    };
    let report: ReportSchemaSupport = decode(row.record, "schema support report")?;
    if report.peer != peer || row.peer.as_deref() != Some(peer.to_string().as_str()) {
        return Err(LeaseError::Corrupt {
            field: "schema support peer".into(),
        });
    }
    Ok(Some(report))
}

pub async fn report_schema_support<S>(
    scope: &mut S,
    report: &ReportSchemaSupport,
) -> Result<SchemaSupportRecorded, LeaseError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database> + Decode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Encode<'v, S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    if let Some(existing) = accepted_schema_support(scope, report.peer).await? {
        if existing.attach_seq > report.attach_seq {
            return Ok(SchemaSupportRecorded);
        }
        if existing.attach_seq == report.attach_seq {
            if existing != *report {
                return Err(LeaseError::InvalidInput {
                    field: "attach sequence has different schema report".into(),
                });
            }
            return Ok(SchemaSupportRecorded);
        }
    }
    checked_queries::write_schema_support(scope, report.peer.to_string(), encode(report)?).await?;
    Ok(SchemaSupportRecorded)
}
