use crate::{
    ReceiverError, StoreError,
    copy_contract::CopyRecord,
    copy_storage::CopyStorage,
    seed_publication::publish_seed,
    seed_storage::{SeedRecord, SeedStorage},
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{
    DatabaseId, PageDigest, SeedAnswer, SeedComplete, SeedId, SeedPage, SeedPageAnswer,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use uuid::Uuid;

/// Allocate a copy-owned generation. A repeated attempt returns its original generation.
pub async fn seed_begin<S>(
    scope: &mut S,
    database: DatabaseId,
    attempt: Uuid,
) -> Result<SeedId, ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    if attempt.is_nil() {
        return Err(ReceiverError::InvalidInput {
            field: "seed attempt must be non-nil".into(),
        });
    }
    let copy = CopyStorage(scope)
        .load()
        .await?
        .ok_or(ReceiverError::NotInitialized)?;
    if matches!(copy, CopyRecord::Retired(_)) {
        return Err(ReceiverError::Retired);
    }
    if copy.status().database != database {
        return Err(ReceiverError::NotInitialized);
    }
    if let Some(seed) = SeedStorage(scope).attempt(attempt).await? {
        return Ok(seed);
    }
    if !matches!(copy, CopyRecord::Seeding { .. }) {
        return Err(ReceiverError::InvalidInput {
            field: "copy is not seedable".into(),
        });
    }
    let previous = SeedStorage(scope).load().await?;
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

/// Persist a verified page without changing live rows or making the copy readable.
pub async fn seed_page<S>(scope: &mut S, page: &SeedPage) -> Result<SeedPageAnswer, ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let copy = match CopyStorage(scope).load().await? {
        Some(CopyRecord::Retired(_)) => return Ok(SeedPageAnswer::Retired),
        Some(CopyRecord::Seeding { status, .. }) => status,
        _ => return Ok(SeedPageAnswer::NotSeedable),
    };
    let Some(record) = SeedStorage(scope).load().await? else {
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
    SeedStorage(scope).save_page(page).await?;
    Ok(SeedPageAnswer::Staged)
}

/// Publish a complete seed atomically. A SeedRefused error requires host rollback.
pub async fn seed_complete<S>(
    scope: &mut S,
    done: &SeedComplete,
) -> Result<SeedAnswer, ReceiverError>
where
    S: TransactionScope,
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
    let copy = CopyStorage(scope)
        .load()
        .await?
        .ok_or(ReceiverError::NotInitialized)?;
    if matches!(copy, CopyRecord::Retired(_)) {
        return Ok(SeedAnswer::Retired);
    }
    if copy.status().database != done.database {
        return Err(ReceiverError::NotInitialized);
    }
    let Some(mut seed) = SeedStorage(scope).load().await? else {
        return Ok(SeedAnswer::StaleSeed);
    };
    if seed.seed != done.seed {
        return Ok(SeedAnswer::StaleSeed);
    }
    if let Some(published) = seed.published {
        if published != *done {
            return Err(ReceiverError::InvalidInput {
                field: "completed seed identity has different contents".into(),
            });
        }
        return Ok(SeedAnswer::Published {
            position: published.position,
        });
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
        return Ok(SeedAnswer::PositionMismatch {
            expected,
            offered: done.position,
        });
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
        return Ok(SeedAnswer::Incomplete { missing });
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
    seed.published = Some(done.clone());
    SeedStorage(scope).save(&seed).await?;
    SeedStorage(scope).drop_pages().await?;
    Ok(SeedAnswer::Published {
        position: done.position,
    })
}
