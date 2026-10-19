use crate::{
    EpochGrant, HolderChangeAnswer, HolderChangePhase, HolderChangeRequest, LeaseError,
    change_checks, checked_queries,
    registry_records::{EpochHistory, TransferRecord},
    registry_storage::{RegistryStorage, decode, decode_history, encode},
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{DatabaseId, Epoch, ReplicationPosition};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// The recorded end of a granted epoch, independent of reserved epoch numbers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum EpochEnd {
    Released { final_position: ReplicationPosition },
    Fenced { ended_at: ReplicationPosition },
    Open,
}

/// An actual registry grant and the permanent end established by its successor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EpochHistoryEntry {
    pub grant: EpochGrant,
    pub end: EpochEnd,
}

/// Continue with the returned epoch as `after`, preserving the original `before`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EpochHistoryPage {
    pub entries: Vec<EpochHistoryEntry>,
    pub next_after: Option<Epoch>,
}

/// Read grants in `(after, before)`; `before` is the caller's frozen current epoch.
pub async fn epoch_history<S>(
    scope: &mut S,
    database: DatabaseId,
    after: Option<Epoch>,
    before: Epoch,
    limit: u32,
) -> Result<EpochHistoryPage, LeaseError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    if limit == 0 {
        return Err(LeaseError::InvalidInput {
            field: "history page limit must be positive".into(),
        });
    }
    if after.is_some_and(|after| after >= before) {
        return Ok(EpochHistoryPage {
            entries: Vec::new(),
            next_after: None,
        });
    }
    // A malformed key must not disappear behind the decimal range predicates.
    if let Some(row) =
        checked_queries::read_invalid_history_epoch(scope, database.as_uuid().to_string()).await?
    {
        stored_epoch(row.epoch)?;
        return Err(corrupt("epoch history key"));
    }
    let rows = checked_queries::read_history_page(
        scope,
        database.as_uuid().to_string(),
        after.map_or_else(|| "0".into(), |epoch| epoch.value().to_string()),
        before.value().to_string(),
        (u64::from(limit) + 1).to_string(),
    )
    .await?;
    // Decode the lookahead too: corrupt data never becomes a continuation token.
    let mut grants = rows
        .into_iter()
        .map(|row| {
            let epoch = stored_epoch(row.epoch)?;
            decode_history(database, epoch, row.record)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let has_more =
        u64::try_from(grants.len()).map_err(|_| corrupt("history page size"))? > u64::from(limit);
    if has_more {
        grants.pop();
    }
    let mut entries = Vec::with_capacity(grants.len());
    let mut storage = RegistryStorage(scope);
    for grant in grants {
        let end = storage.historical_end(&grant).await?;
        entries.push(EpochHistoryEntry {
            grant: EpochGrant {
                epoch: grant.epoch,
                holder: grant.holder,
                granted_at: grant.granted_at,
                ended_at: grant.ended_at,
            },
            end,
        });
    }
    let next_after = if has_more {
        entries.last().map(|entry| entry.grant.epoch)
    } else {
        None
    };
    Ok(EpochHistoryPage {
        entries,
        next_after,
    })
}

fn corrupt(field: &str) -> LeaseError {
    LeaseError::Corrupt {
        field: field.into(),
    }
}

fn stored_epoch(raw: Option<String>) -> Result<Epoch, LeaseError> {
    let raw = raw.ok_or_else(|| corrupt("epoch history key"))?;
    let value = raw
        .parse::<u64>()
        .map_err(|_| corrupt("epoch history key"))?;
    let epoch = Epoch::new(value).map_err(|_| corrupt("epoch history key"))?;
    if raw != value.to_string() {
        return Err(corrupt("epoch history key"));
    }
    Ok(epoch)
}

impl<S> RegistryStorage<'_, S>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    async fn historical_end(&mut self, grant: &EpochHistory) -> Result<EpochEnd, LeaseError> {
        let successor = checked_queries::read_history_successor(
            self.0,
            grant.database.as_uuid().to_string(),
            grant.epoch.value().to_string(),
        )
        .await?;
        let Some(successor) = successor else {
            if grant.ended_at.is_some()
                || self
                    .lease(grant.database)
                    .await?
                    .is_none_or(|lease| lease.epoch != grant.epoch || lease.holder != grant.holder)
            {
                return Err(corrupt("open epoch history"));
            }
            return Ok(EpochEnd::Open);
        };
        let successor = decode_history(
            grant.database,
            stored_epoch(successor.epoch)?,
            successor.record,
        )?;
        let ended_at = grant
            .ended_at
            .ok_or_else(|| corrupt("historical grant end"))?;
        let attempts = checked_queries::read_history_transfer(
            self.0,
            grant.database.as_uuid().to_string(),
            encode(&Some(successor.epoch))?,
        )
        .await?;
        let [attempt] = attempts.as_slice() else {
            return Err(corrupt("history successor transfer"));
        };
        let raw = attempt
            .attempt
            .as_ref()
            .ok_or_else(|| corrupt("history successor transfer"))?;
        let id = decode(Some(encode(raw)?), "history successor transfer")?;
        let transfer = self
            .transfer(id)
            .await?
            .ok_or_else(|| corrupt("history successor transfer"))?;
        if transfer.intent.database() != grant.database
            || transfer.intent.new_epoch != Some(successor.epoch)
            || change_checks::target(&transfer.intent.request) != successor.holder
            || !matches!(transfer.outcome, Some(HolderChangeAnswer::Granted { epoch, start, .. }) if epoch == successor.epoch && start == ended_at)
        {
            return Err(corrupt("history successor grant"));
        }
        end_kind(grant, &transfer, ended_at)
    }
}

fn end_kind(
    grant: &EpochHistory,
    transfer: &TransferRecord,
    ended_at: ReplicationPosition,
) -> Result<EpochEnd, LeaseError> {
    match &transfer.intent.request {
        HolderChangeRequest::Move { from_epoch, .. } if *from_epoch == grant.epoch => {
            let mut releases =
                transfer
                    .phase_history
                    .iter()
                    .filter_map(|entry| match entry.phase {
                        HolderChangePhase::Released { final_position } => Some(final_position),
                        _ => None,
                    });
            if releases.next() != Some(ended_at) || releases.next().is_some() {
                return Err(corrupt("historical release"));
            }
            Ok(EpochEnd::Released {
                final_position: ended_at,
            })
        }
        HolderChangeRequest::Takeover { preview, .. }
            if preview.holder == grant.holder && preview.acked == ended_at =>
        {
            Ok(EpochEnd::Fenced { ended_at })
        }
        _ => Err(corrupt("historical holder change")),
    }
}
