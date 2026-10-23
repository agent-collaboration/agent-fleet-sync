use crate::{
    ReceiverError, StoreError,
    copy_contract::CopyRecord,
    copy_storage::CopyStorage,
    seed_publication::publish_seed,
    seed_storage::{SeedPageWrite, SeedRecord, SeedStorage},
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::SeedAttemptId;
use fleet_sync_protocol::{
    DatabaseId, PageDigest, SeedAnswer, SeedComplete, SeedId, SeedPage, SeedPageAnswer,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Allocate a copy-owned generation. A repeated attempt returns its original generation.
// Explicit Send is required by borrowed-scope host dispatch.
#[allow(clippy::manual_async_fn)]
pub fn seed_begin<'scope, S>(
    scope: &'scope mut S,
    database: DatabaseId,
    attempt: SeedAttemptId,
    authority: &'scope crate::SeedAuthority,
) -> impl std::future::Future<Output = Result<SeedId, ReceiverError>> + Send + 'scope
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
        let copy = CopyStorage(scope)
            .load()
            .await?
            .ok_or(ReceiverError::NotInitialized)?;
        if matches!(copy, CopyRecord::Retired { .. }) {
            return Err(ReceiverError::Retired);
        }
        if copy.database() != database {
            return Err(ReceiverError::NotInitialized);
        }
        authorize_seed(scope, &copy, authority).await?;
        let previous = SeedStorage(scope).load(database).await?;
        if let Some(seed) = SeedStorage(scope).attempt(attempt).await? {
            return Ok(seed);
        }
        if !matches!(copy, CopyRecord::Seeding { .. }) {
            return Err(ReceiverError::InvalidInput {
                field: "copy is not seedable".into(),
            });
        }
        let generation = previous
            .as_ref()
            .map_or(0, |record| record.seed.generation)
            .checked_add(1)
            .ok_or_else(|| StoreError::Corrupt {
                field: "seed generation exhausted".into(),
            })?;
        let seed = SeedId { generation };
        SeedStorage(scope).drop_pages().await?;
        SeedStorage(scope)
            .save(&SeedRecord {
                database,
                seed,
                published: None,
            })
            .await?;
        SeedStorage(scope).save_attempt(attempt, seed).await?;
        Ok(seed)
    }
}

/// Persist a verified page without changing live rows or making the copy readable.
// Explicit Send is required by borrowed-scope host dispatch.
#[allow(clippy::manual_async_fn)]
pub fn seed_page<'scope, S>(
    scope: &'scope mut S,
    page: &'scope SeedPage,
    authority: &'scope crate::SeedAuthority,
) -> impl std::future::Future<Output = Result<SeedPageAnswer, ReceiverError>> + Send + 'scope
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
        let record = match CopyStorage(scope).load().await? {
            Some(CopyRecord::Retired { .. }) => return Ok(SeedPageAnswer::Retired),
            Some(record) => record,
            None => return Ok(SeedPageAnswer::NotSeedable),
        };
        authorize_seed(scope, &record, authority).await?;
        let CopyRecord::Seeding { status: copy, .. } = record else {
            return Ok(SeedPageAnswer::NotSeedable);
        };
        let Some(record) = SeedStorage(scope).load(copy.database).await? else {
            return Ok(SeedPageAnswer::NotSeedable);
        };
        if page.seed < record.seed {
            return Ok(SeedPageAnswer::StaleSeed);
        }
        if page.seed != record.seed || page.contents.database != copy.database {
            return Ok(SeedPageAnswer::NotSeedable);
        }
        if PageDigest::of(&page.contents).ok().as_ref() != Some(&page.digest) {
            return Ok(SeedPageAnswer::SeedConflict);
        }
        if let Some(stored) = SeedStorage(scope).page(page).await? {
            return Ok(if stored == *page {
                SeedPageAnswer::AlreadyStaged
            } else {
                SeedPageAnswer::SeedConflict
            });
        }
        Ok(match SeedStorage(scope).save_page(page).await? {
            SeedPageWrite::Staged => SeedPageAnswer::Staged,
            SeedPageWrite::UnitTooLarge => SeedPageAnswer::UnitTooLarge,
        })
    }
}

/// Publish a complete seed atomically. A SeedRefused error requires host rollback.
// Explicit Send is required by borrowed-scope host dispatch.
#[allow(clippy::manual_async_fn)]
pub fn seed_complete<'scope, S, H>(
    scope: &'scope mut S,
    done: &'scope SeedComplete,
    hook: &'scope H,
    authority: &'scope crate::SeedAuthority,
) -> impl std::future::Future<Output = Result<crate::SeedDecision<H::FollowUp>, ReceiverError>>
+ Send
+ 'scope
where
    S: TransactionScope + 'scope,
    H: crate::ReceiverRules<S>,
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
        let copy = CopyStorage(scope)
            .load()
            .await?
            .ok_or(ReceiverError::NotInitialized)?;
        if matches!(copy, CopyRecord::Retired { .. }) {
            return Ok(crate::SeedDecision::Answer(SeedAnswer::Retired));
        }
        if copy.database() != done.database {
            return Err(ReceiverError::NotInitialized);
        }
        authorize_seed(scope, &copy, authority).await?;
        let Some(mut seed) = SeedStorage(scope).load(done.database).await? else {
            return Ok(crate::SeedDecision::Answer(SeedAnswer::StaleSeed));
        };
        if seed.seed != done.seed {
            return Ok(crate::SeedDecision::Answer(SeedAnswer::StaleSeed));
        }
        if let Some(published) = seed.published {
            if published != *done {
                return Err(ReceiverError::InvalidInput {
                    field: "completed seed identity has different contents".into(),
                });
            }
            return Ok(crate::SeedDecision::Answer(SeedAnswer::Published {
                position: published.position,
            }));
        }
        let CopyRecord::Seeding {
            mut status,
            expected_position,
        } = copy
        else {
            return Err(ReceiverError::InvalidInput {
                field: "copy is not seedable".into(),
            });
        };
        if let Some(expected) = expected_position
            && expected != done.position
        {
            return Ok(crate::SeedDecision::Answer(SeedAnswer::PositionMismatch {
                expected,
                offered: done.position,
            }));
        }
        if done.position.epoch != status.fence {
            return Err(ReceiverError::InvalidInput {
                field: "seed position must belong to the copy's initial epoch".into(),
            });
        }
        let pages = SeedStorage(scope).pages(done.seed).await?;
        let missing = (0..done.pages)
            .filter(|index| !pages.iter().any(|page| page.page == *index))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Ok(crate::SeedDecision::Answer(SeedAnswer::Incomplete {
                missing,
            }));
        }
        if pages.len()
            != usize::try_from(done.pages).map_err(|_| ReceiverError::InvalidInput {
                field: "seed page count".into(),
            })?
            || pages.is_empty()
        {
            return Err(ReceiverError::InvalidInput {
                field: "seed page count".into(),
            });
        }
        publish_seed(scope, done, &pages).await?;
        status.start = done.position;
        status.acked = done.position;
        status.schema = done.schema.clone();
        CopyStorage(scope)
            .save(&CopyRecord::Following(status))
            .await?;
        let installed = crate::InstalledSnapshot {
            database: done.database,
            position: done.position,
            schema: done.schema.clone(),
        };
        let follow_ups = crate::run_snapshot_hook(scope, hook, &installed)
            .await
            .map_err(|error| match error {
                crate::HookError::Refused(refusal) => ReceiverError::SeedRefused {
                    answer: SeedAnswer::Hook(refusal),
                },
                crate::HookError::Storage(error) => ReceiverError::Storage(error),
            })?;
        seed.published = Some(done.clone());
        SeedStorage(scope).save(&seed).await?;
        SeedStorage(scope).drop_pages().await?;
        Ok(crate::SeedDecision::Published {
            position: done.position,
            follow_ups,
        })
    }
}

async fn authorize_seed<S>(
    scope: &mut S,
    copy: &CopyRecord,
    authority: &crate::SeedAuthority,
) -> Result<(), ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let status = match copy {
        CopyRecord::Starting { status, .. }
        | CopyRecord::Seeding { status, .. }
        | CopyRecord::Following(status) => status,
        CopyRecord::Retired { .. } => return Err(ReceiverError::Retired),
    };
    if status.role != fleet_sync_protocol::CopyRole::Authoritative {
        return Err(ReceiverError::NotWriter {
            current: Some(status.fence),
        });
    }
    let original = CopyStorage(scope)
        .creation()
        .await?
        .ok_or_else(|| StoreError::Corrupt {
            field: "seed authority requires original copy creation".into(),
        })?;
    if original.database != status.database {
        return Err(StoreError::Corrupt {
            field: "original copy creation database".into(),
        }
        .into());
    }
    let allowed = match authority {
        crate::SeedAuthority::Writer(sender) => original.holder.as_ref() == Some(sender),
        crate::SeedAuthority::Admin => {
            original.holder.is_none()
                && matches!(
                    original.origin,
                    fleet_sync_protocol::CopyOrigin::AwaitingSeed { .. }
                )
        }
    };
    if !allowed {
        return Err(ReceiverError::NotWriter {
            current: Some(status.fence),
        });
    }
    Ok(())
}
