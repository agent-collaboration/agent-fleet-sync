//! Decode historical records with the same domain types and invariants as current reads.
use crate::{
    LeaseMigrationError, checked_legacy_queries as legacy,
    lease_migration_error::invalid,
    registry_records::*,
    registry_row::{stored_epoch, stored_scalar},
};
use durable_steps::{OperationId, storage::TransactionScope};
use fleet_sync_protocol::*;
use serde::de::DeserializeOwned;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::collections::BTreeMap;
fn label(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("<missing>")
}
fn decode<T: DeserializeOwned>(
    value: Option<String>,
    table: &'static str,
    key: &str,
    field: &'static str,
) -> Result<T, LeaseMigrationError> {
    let text = value.ok_or_else(|| invalid(table, key, field))?;
    serde_json::from_str(&text).map_err(|_| invalid(table, key, field))
}
fn identity<T: DeserializeOwned>(
    value: Option<String>,
    table: &'static str,
    key: &str,
    field: &'static str,
) -> Result<T, LeaseMigrationError> {
    stored_scalar(value, field).map_err(|_| invalid(table, key, field))
}
async fn old_table<S>(
    scope: &mut S,
    table: &'static str,
    old: &str,
    current: &str,
) -> Result<bool, LeaseMigrationError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    let columns = legacy::legacy_columns(scope, table.into()).await?;
    let has_old = columns
        .iter()
        .any(|column| column.column_name.as_deref() == Some(old));
    let has_current = columns
        .iter()
        .any(|column| column.column_name.as_deref() == Some(current));
    if !columns.is_empty() && (has_old == has_current) {
        return Err(invalid(table, "<schema>", "layout"));
    }
    Ok(has_old)
}

/// Validate inside the corpus transaction, after prefix checks and before typed DDL.
#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send preserves the borrowed-scope/GAT migration boundary"
)]
pub fn validate_legacy_rows<'a, S>(
    scope: &'a mut S,
) -> impl std::future::Future<Output = Result<(), LeaseMigrationError>> + Send + 'a
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        let mut databases = BTreeMap::new();
        let mut leases = BTreeMap::new();
        let databases_old =
            old_table(scope, "_fleet_sync_databases", "record", "lifecycle_kind").await?;
        if databases_old {
            for row in legacy::legacy_databases(scope).await? {
                let table = "_fleet_sync_databases";
                let key = format!(
                    "{} [rowid {}]",
                    label(&row.database),
                    label(&row.legacy_rowid)
                );
                let database: DatabaseId = identity(row.database, table, &key, "database")?;
                let record: DatabaseRecord = decode(row.record, table, &key, "record")?;
                if record.database != database {
                    return Err(invalid(table, &key, "identity"));
                }
                databases.insert(database, (key, record));
            }
        }
        let leases_old = old_table(scope, "_fleet_sync_leases", "record", "holder").await?;
        if leases_old {
            for row in legacy::legacy_leases(scope).await? {
                let table = "_fleet_sync_leases";
                let key = format!(
                    "{} [rowid {}]",
                    label(&row.database),
                    label(&row.legacy_rowid)
                );
                let database: DatabaseId = identity(row.database, table, &key, "database")?;
                let record: RegistryLease = decode(row.record, table, &key, "record")?;
                if record.database != database {
                    return Err(invalid(table, &key, "identity"));
                }
                if record.expires_at < record.last_renewed_at {
                    return Err(invalid(table, &key, "expires_at"));
                }
                leases.insert(database, record);
            }
        }
        for (database, (key, record)) in &databases {
            record
                .validate_grant(leases.get(database))
                .map_err(|_| invalid("_fleet_sync_databases", key, "registry revision grant"))?;
        }
        if databases_old || leases_old {
            for row in legacy::legacy_floor(scope).await? {
                let table = "_fleet_sync_epoch_floor";
                let key = format!(
                    "{} [rowid {}]",
                    label(&row.database),
                    label(&row.legacy_rowid)
                );
                let _: DatabaseId = identity(row.database, table, &key, "database")?;
                stored_epoch(row.epoch, "epoch").map_err(|_| invalid(table, &key, "epoch"))?;
            }
        }
        if old_table(scope, "_fleet_sync_epoch_history", "record", "granted_at").await? {
            for row in legacy::legacy_history(scope).await? {
                let table = "_fleet_sync_epoch_history";
                let key = format!(
                    "{}/{} [rowid {}]",
                    label(&row.database),
                    label(&row.epoch),
                    label(&row.legacy_rowid)
                );
                let database: DatabaseId = identity(row.database, table, &key, "database")?;
                let epoch =
                    stored_epoch(row.epoch, "epoch").map_err(|_| invalid(table, &key, "epoch"))?;
                let record: EpochHistory = decode(row.record, table, &key, "record")?;
                crate::registry_storage::decode_history(database, epoch, record)
                    .map_err(|_| invalid(table, &key, "identity or end position"))?;
            }
        }
        if old_table(
            scope,
            "_fleet_sync_renewal_attempts",
            "record",
            "answer_kind",
        )
        .await?
        {
            for row in legacy::legacy_renewals(scope).await? {
                let table = "_fleet_sync_renewal_attempts";
                let key = format!(
                    "{}/{} [rowid {}]",
                    label(&row.database),
                    label(&row.attempt),
                    label(&row.legacy_rowid)
                );
                let database: DatabaseId = identity(row.database, table, &key, "database")?;
                let attempt: RequestId = identity(row.attempt, table, &key, "attempt")?;
                let record: RenewalAttempt = decode(row.record, table, &key, "record")?;
                if record.request.database != database || record.request.attempt != attempt {
                    return Err(invalid(table, &key, "identity"));
                }
            }
        }
        if old_table(scope, "_fleet_sync_takeover_previews", "record", "fence").await? {
            for row in legacy::legacy_previews(scope).await? {
                let table = "_fleet_sync_takeover_previews";
                let key = format!("{} [rowid {}]", label(&row.token), label(&row.legacy_rowid));
                let token: PreviewToken = identity(row.token, table, &key, "token")?;
                let database: DatabaseId = identity(row.database, table, &key, "database")?;
                let record: PreviewRecord = decode(row.record, table, &key, "record")?;
                if record.preview.token != token || record.database != database {
                    return Err(invalid(table, &key, "identity"));
                }
            }
        }
        if old_table(
            scope,
            "_fleet_sync_lifecycle_attempts",
            "intent",
            "intent_kind",
        )
        .await?
        {
            for row in legacy::legacy_lifecycle(scope).await? {
                let table = "_fleet_sync_lifecycle_attempts";
                let key = format!(
                    "{} [rowid {}]",
                    label(&row.attempt),
                    label(&row.legacy_rowid)
                );
                let attempt: RequestId = identity(row.attempt, table, &key, "attempt")?;
                let database: DatabaseId = identity(row.database, table, &key, "database")?;
                let intent: LifecycleIntent = decode(row.intent, table, &key, "intent")?;
                if intent.attempt() != attempt || intent.database() != database {
                    return Err(invalid(table, &key, "identity"));
                }
                let outcome = row
                    .outcome
                    .map(|value| decode(Some(value), table, &key, "outcome"))
                    .transpose()?;
                let record = LifecycleAttempt { intent, outcome };
                record
                    .validate()
                    .map_err(|_| invalid(table, &key, "outcome kind"))?;
            }
        }
        let admissions_old = old_table(
            scope,
            "_fleet_sync_transfer_admissions",
            "request",
            "request_kind",
        )
        .await?;
        let mut admissions = BTreeMap::new();
        if admissions_old {
            for row in legacy::legacy_admissions(scope).await? {
                let table = "_fleet_sync_transfer_admissions";
                let key = format!(
                    "{} [rowid {}]",
                    label(&row.attempt),
                    label(&row.legacy_rowid)
                );
                let attempt: TransferId = identity(row.attempt, table, &key, "attempt")?;
                let operation: OperationId = decode(row.operation, table, &key, "operation")?;
                let request: HolderChangeRequest = decode(row.request, table, &key, "request")?;
                let _: ActorRef = decode(row.requested_by, table, &key, "requested_by")?;
                let _: SyncTime = decode(row.admitted_at, table, &key, "admitted_at")?;
                let target_schema: Option<SchemaSupport> =
                    decode(row.target_schema, table, &key, "target_schema")?;
                let follow_up: Option<OperationId> = row
                    .follow_up
                    .map(|value| decode(Some(value), table, &key, "follow_up"))
                    .transpose()?;
                if crate::change_checks::attempt(&request) != attempt
                    || follow_up == Some(operation)
                {
                    return Err(invalid(table, &key, "identity"));
                }
                admissions.insert(attempt, (request, target_schema));
            }
        }
        if old_table(scope, "_fleet_sync_transfers", "intent", "new_epoch").await? {
            for row in legacy::legacy_transfers(scope).await? {
                let table = "_fleet_sync_transfers";
                let key = format!(
                    "{} [rowid {}]",
                    label(&row.attempt),
                    label(&row.legacy_rowid)
                );
                let attempt: TransferId = identity(row.attempt, table, &key, "attempt")?;
                let database: DatabaseId = identity(row.database, table, &key, "database")?;
                let (request, target_schema) = admissions
                    .get(&attempt)
                    .ok_or_else(|| invalid(table, &key, "admission"))?;
                let intent = TransferIntent {
                    request: request.clone(),
                    new_epoch: decode(row.intent, table, &key, "reserved epoch")?,
                    target_schema: *target_schema,
                };
                if intent.database() != database {
                    return Err(invalid(table, &key, "identity"));
                }
                let record = TransferRecord {
                    intent,
                    outcome: row
                        .outcome
                        .map(|value| decode(Some(value), table, &key, "outcome"))
                        .transpose()?,
                    phase: decode(row.phase, table, &key, "phase")?,
                    phase_history: decode(row.phase_history, table, &key, "phase_history")?,
                    cleanup_failed: row
                        .cleanup_failed
                        .map(|value| decode(Some(value), table, &key, "cleanup_failed"))
                        .transpose()?,
                };
                record
                    .validate()
                    .map_err(|_| invalid(table, &key, "phase history"))?;
            }
        }
        if old_table(
            scope,
            "_fleet_sync_transfer_cancels",
            "record",
            "cancelled_by",
        )
        .await?
        {
            for row in legacy::legacy_cancels(scope).await? {
                let table = "_fleet_sync_transfer_cancels";
                let key = format!(
                    "{} [rowid {}]",
                    label(&row.attempt),
                    label(&row.legacy_rowid)
                );
                let _: TransferId = identity(row.attempt, table, &key, "attempt")?;
                let _: crate::CancelRecord = decode(row.record, table, &key, "record")?;
            }
        }
        if old_table(scope, "_fleet_sync_schema_support", "record", "attach_seq").await? {
            for row in legacy::legacy_support(scope).await? {
                let table = "_fleet_sync_schema_support";
                let key = format!("{} [rowid {}]", label(&row.peer), label(&row.legacy_rowid));
                let peer: PeerId = identity(row.peer, table, &key, "peer")?;
                let record: ReportSchemaSupport = decode(row.record, table, &key, "record")?;
                if record.peer != peer {
                    return Err(invalid(table, &key, "identity"));
                }
            }
        }
        Ok(())
    }
}
