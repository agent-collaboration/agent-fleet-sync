use crate::DeliveryError;
use durable_steps::storage::TransactionScope;
use sqlx_core::{arguments::IntoArguments, database::Database};

/// Retire tables unused by a relay host in the caller's startup transaction.
/// Machine hosts retain their full schema and must not invoke this operation.
#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send preserves higher-ranked borrowed transaction futures"
)]
pub fn retire_relay_host_tables<'scope, S>(
    scope: &'scope mut S,
) -> impl std::future::Future<Output = Result<(), DeliveryError>> + Send + 'scope
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
{
    async move {
        for statement in [
            "DROP TABLE IF EXISTS _fleet_sync_mail_cancel_links",
            "DROP TABLE IF EXISTS _fleet_sync_mail_controls",
            "DROP TABLE IF EXISTS _fleet_sync_mail_items",
            "DROP TABLE IF EXISTS _fleet_sync_mail_state",
            "DROP TABLE IF EXISTS _fleet_sync_delivery_receipts",
            "DROP TABLE IF EXISTS _fleet_sync_inbox_state",
            "DROP TABLE IF EXISTS _fleet_sync_outbox_status",
            "DROP TABLE IF EXISTS _fleet_sync_reports",
            "DROP TABLE IF EXISTS _fleet_sync_inbox_cancel_links",
            "DROP TABLE IF EXISTS _fleet_sync_inbox_items",
        ] {
            sqlx_core::query::query::<S::Database>(statement)
                .execute(scope.executor())
                .await
                .map_err(crate::custody_record::sql_error)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "relay_schema_test.rs"]
mod tests;
