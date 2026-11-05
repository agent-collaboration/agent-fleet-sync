use crate::{
    DeliveryError, ItemId, SyncTime, custody_record::*, delivery_storage_rows as queries,
    task_support::*,
};
use durable_steps::{
    Admitted, AtomicStore, DriveLease, JournalScope, OperationContext, OperationFailure, Task,
    storage::{JournalStorage, OperationExecution, TransactionScope},
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{marker::PhantomData, time::Duration};

pub const DELIVERY_RETENTION_MILLIS: i64 = 60 * 24 * 60 * 60 * 1000;
pub fn removal_deadline(item: ItemId) -> SyncTime {
    SyncTime::from_unix_millis(
        created_at(item)
            .unix_millis()
            .saturating_add(DELIVERY_RETENTION_MILLIS),
    )
}
pub fn item_is_too_old(item: ItemId, now: SyncTime) -> bool {
    now > removal_deadline(item)
}

/// Admit removal for pre-existing custody after migrations, in the owner's startup transaction.
/// Only canonical item identities are read: missing or corrupt content cannot block cleanup.
/// Return the number of distinct items newly scheduled; existing admissions do not count.
#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send preserves borrowed journal transaction futures"
)]
pub fn admit_existing_record_removals<'scope, S, T>(
    scope: &'scope mut S,
    role: crate::DeliveryHostRole,
    now: SyncTime,
) -> impl std::future::Future<Output = Result<usize, DeliveryError>> + Send + 'scope
where
    S: TransactionScope + JournalScope + JournalStorage,
    T: Task<Input = ItemId>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        if crate::backfill_state::completed(scope, "record-removals").await? {
            return Ok(0);
        }
        let rows = match role {
            crate::DeliveryHostRole::Machine => queries::read_existing_custody(scope).await,
            crate::DeliveryHostRole::Relay => queries::read_relay_existing_custody(scope).await,
        }
        .map_err(sql_error)?;
        let mut items = Vec::with_capacity(rows.len());
        // Validate every key before making the first journal mutation.
        for row in rows {
            let table = match row.source.as_deref() {
                Some("outbox") => "_fleet_sync_outbox_items",
                Some("inbox") => "_fleet_sync_inbox_items",
                _ => {
                    return Err(DeliveryError::Corrupt {
                        field: "existing custody source",
                    });
                }
            };
            let key = row.row_key.ok_or(DeliveryError::Corrupt {
                field: "existing custody row key",
            })?;
            let item = row
                .item_id
                .and_then(|value| ItemId::try_from(value).ok())
                .ok_or(DeliveryError::CorruptRow {
                    table,
                    key,
                    field: "item_id",
                })?;
            items.push(item);
        }
        items.sort_unstable_by_key(|item| item.as_uuid());
        items.dedup();
        let mut admitted = 0;
        for item in items {
            admitted += usize::from(admit_record_removal_once::<_, T>(scope, item).await?);
        }
        crate::backfill_state::complete(scope, "record-removals", now).await?;
        Ok(admitted)
    }
}

#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send avoids async-fn/GAT lifetime generality errors in journal transactions"
)]
pub fn admit_record_removal<'scope, S, T>(
    scope: &'scope mut S,
    item: ItemId,
) -> impl std::future::Future<Output = Result<(), DeliveryError>> + Send + 'scope
where
    S: TransactionScope + JournalScope + JournalStorage,
    T: Task<Input = ItemId>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        admit_record_removal_once::<_, T>(scope, item)
            .await
            .map(|_| ())
    }
}

#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send preserves borrowed journal transaction futures"
)]
fn admit_record_removal_once<'scope, S, T>(
    scope: &'scope mut S,
    item: ItemId,
) -> impl std::future::Future<Output = Result<bool, DeliveryError>> + Send + 'scope
where
    S: TransactionScope + JournalScope + JournalStorage,
    T: Task<Input = ItemId>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        let admitted = scope
            .admit::<T>(&crate::delivery_request_key(&item)?, &item)
            .await
            .map_err(|error| DeliveryError::Storage(error.to_string()))?;
        if let Admitted::New(operation) = admitted {
            let record = scope
                .operation(operation)
                .await?
                .ok_or(DeliveryError::Corrupt {
                    field: "new removal operation",
                })?;
            let OperationExecution::Running {
                next_attempt_at, ..
            } = record.execution
            else {
                return Err(DeliveryError::Corrupt {
                    field: "new removal is not running",
                });
            };
            // This UUID was just minted by the owning injected IdSource.
            // The initial token is private and retry clears it before commit.
            let token = operation.as_uuid();
            // The new row is still private to this admission transaction. Use
            // the journal's guarded APIs to schedule it without an eager drive.
            if scope
                .claim(
                    operation,
                    next_attempt_at,
                    DriveLease {
                        token,
                        until: next_attempt_at.after(Duration::from_secs(1)),
                    },
                )
                .await?
                .is_none()
            {
                return Err(DeliveryError::Corrupt {
                    field: "new removal operation was not claimable",
                });
            }
            scope
                .retry(operation, token, removal_deadline(item), 0)
                .await?;
            scope.ensure_wake(removal_deadline(item)).await?;
            return Ok(true);
        }
        Ok(false)
    }
}

pub struct RemoveDeliveryRecord<S, Db> {
    role: crate::DeliveryHostRole,
    store: PhantomData<fn() -> (S, Db)>,
}
impl<S, Db> RemoveDeliveryRecord<S, Db> {
    pub const fn new(role: crate::DeliveryHostRole) -> Self {
        Self {
            role,
            store: PhantomData,
        }
    }
}
impl<S, Db> Task for RemoveDeliveryRecord<S, Db>
where
    S: AtomicStore,
    Db: Database,
    for<'t> S::Scope<'t>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'v> String: Type<Db> + Encode<'v, Db>,
{
    const KIND: &'static str = "fleet-sync.delivery.remove-record";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = ItemId;
    type Output = ();
    async fn run<O: OperationContext<Store = S>>(
        &self,
        ctx: &mut O,
        item: &ItemId,
    ) -> Result<(), OperationFailure> {
        let item = *item;
        let now = ctx.now();
        let deadline = removal_deadline(item);
        let role = self.role;
        ctx.local(step("remove_at_creation_deadline")?, move |scope| {
            Box::pin(async move {
                if now < deadline {
                    return Err(durable_steps::StepError::Retry {
                        after: Duration::from_millis(
                            deadline
                                .unix_millis()
                                .saturating_sub(now.unix_millis())
                                .unsigned_abs(),
                        ),
                    });
                }
                queries::remove_retained_status(scope, item_key(item))
                    .await
                    .map_err(sql_error)
                    .map_err(step_error)?;
                queries::remove_retained_events(scope, item_key(item))
                    .await
                    .map_err(sql_error)
                    .map_err(step_error)?;
                if role == crate::DeliveryHostRole::Machine {
                    queries::drop_inbox_reports(scope, item_key(item))
                        .await
                        .map_err(sql_error)
                        .map_err(step_error)?;
                    queries::remove_retained_inbox_links(scope, item_key(item))
                        .await
                        .map_err(sql_error)
                        .map_err(step_error)?;
                }
                queries::remove_retained_outbox_links(scope, item_key(item))
                    .await
                    .map_err(sql_error)
                    .map_err(step_error)?;
                if role == crate::DeliveryHostRole::Machine {
                    queries::drop_inbox_item(scope, item_key(item))
                        .await
                        .map_err(sql_error)
                        .map_err(step_error)?;
                }
                queries::remove_retained_outbox(scope, item_key(item))
                    .await
                    .map_err(sql_error)
                    .map_err(step_error)?;
                Ok(())
            })
        })
        .await?;
        Ok(())
    }
}
