use crate::custody_record::sql_error;
use crate::{
    DeliveryError, DeliveryTypes, ItemId, Submitter, SyncTime, delivery_storage_rows as queries,
};
use durable_steps::storage::TransactionScope;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

#[derive(Clone, Copy)]
enum CustodyHome {
    Outbox,
    Inbox,
}
impl CustodyHome {
    const fn table(self) -> &'static str {
        match self {
            Self::Outbox => "_fleet_sync_outbox_items",
            Self::Inbox => "_fleet_sync_inbox_items",
        }
    }
}
enum Change {
    Bind {
        home: CustodyHome,
        item: ItemId,
        submitter: Submitter,
        compact: bool,
    },
    Compact {
        item: ItemId,
    },
    Legacy {
        home: CustodyHome,
        item: ItemId,
    },
}

async fn plan<S, D, B>(
    scope: &mut S,
    binding: &B,
    rows: Vec<queries::StoredSubmitterBackfillRow>,
) -> Result<Vec<Change>, DeliveryError>
where
    S: TransactionScope,
    D: DeliveryTypes,
    B: crate::SenderBinding<S, D>,
{
    let mut changes = Vec::new();
    for row in rows {
        let home = match row.source.as_deref() {
            Some("outbox") => CustodyHome::Outbox,
            Some("inbox") => CustodyHome::Inbox,
            _ => {
                return Err(DeliveryError::Corrupt {
                    field: "sender backfill source",
                });
            }
        };
        let key = format!(
            "{} [rowid {}]",
            row.item_id.as_deref().unwrap_or("<missing>"),
            row.row_key.as_deref().unwrap_or("<missing>")
        );
        let corrupt = |field| DeliveryError::CorruptRow {
            table: home.table(),
            key: key.clone(),
            field,
        };
        if row.item_class.as_deref() != Some("text") {
            return Err(corrupt("item_id"));
        }
        let item = row
            .item_id
            .and_then(|id| ItemId::try_from(id).ok())
            .ok_or_else(|| corrupt("item_id"))?;
        for class in [&row.submitter_kind_class, &row.submitter_id_class] {
            if !matches!(class.as_deref(), Some("text" | "null")) {
                return Err(corrupt("submitter"));
            }
        }
        if !matches!(row.consumer_sender_class.as_deref(), Some("text" | "null")) {
            return Err(corrupt("consumer_sender"));
        }
        let owner = crate::submitter::from_columns(row.submitter_kind, row.submitter_id)
            .map_err(|_| corrupt("submitter"))?;
        let payload = match row.payload_present.as_deref() {
            Some("0") => false,
            Some("1") => true,
            _ => return Err(corrupt("payload")),
        };
        let live = match row.live.as_deref() {
            Some("0") => false,
            Some("1") => true,
            _ => return Err(corrupt("live")),
        };
        let legacy = match row.legacy_read_only.as_deref() {
            Some("0") => false,
            Some("1") => true,
            _ => return Err(corrupt("legacy_read_only")),
        };
        let terminal = matches!(
            row.status_kind.as_deref(),
            Some(
                "delivered"
                    | "uncertain"
                    | "expired"
                    | "cancelled"
                    | "refused"
                    | "dropped"
                    | "prevented"
                    | "tooLate"
            )
        );
        let eligible_legacy = !payload
            && match home {
                CustodyHome::Outbox => {
                    row.record_kind.as_deref() == Some("unresolvable")
                        || terminal
                        || (matches!(
                            row.status_kind.as_deref(),
                            Some("sent" | "atRecipient" | "held")
                        ) && (row.submit_attempted.as_deref() == Some("1")
                            || row.record_kind.as_deref() == Some("recovered")))
                }
                CustodyHome::Inbox => {
                    terminal && matches!(row.progress.as_deref(), Some("final" | "control"))
                }
            };
        let compact = matches!(home, CustodyHome::Inbox)
            && terminal
            && row.progress.as_deref() == Some("final")
            && (payload || live);
        if legacy {
            if owner.is_some() || row.consumer_sender.is_some() || !eligible_legacy {
                return Err(corrupt("legacy sender"));
            }
            continue;
        }
        if let Some(raw) = row.consumer_sender {
            let sender: D::Sender =
                serde_json::from_str(&raw).map_err(|_| corrupt("consumer_sender"))?;
            let context = match home {
                CustodyHome::Outbox => crate::CustodyRow::Outbox(item),
                CustodyHome::Inbox => crate::CustodyRow::Inbox(item),
            };
            let mapped = binding
                .resolve_submitter(scope, &sender)
                .await
                .map_err(|reason| DeliveryError::SenderBindingUnavailable {
                    row: context,
                    reason,
                })?;
            if let Some(owner) = owner {
                if owner != mapped {
                    return Err(corrupt("submitter"));
                }
                if compact {
                    changes.push(Change::Compact { item });
                }
            } else {
                changes.push(Change::Bind {
                    home,
                    item,
                    submitter: mapped,
                    compact,
                });
            }
        } else if !payload
            && matches!(home, CustodyHome::Outbox)
            && matches!(
                row.record_kind.as_deref(),
                Some("recovered" | "unresolvable")
            )
            && owner.is_some()
        {
            // A bound negative record never had an envelope to retain.
        } else if owner.is_none() && eligible_legacy {
            changes.push(Change::Legacy { home, item });
        } else {
            return Err(corrupt("consumer_sender"));
        }
    }
    Ok(changes)
}

/// Map retained consumer sources once in the owning startup transaction.
/// All rows validate before the first mutation; completion is committed last.
#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send preserves borrowed transaction futures"
)]
pub fn backfill_submitters<'scope, D, S, B>(
    scope: &'scope mut S,
    role: crate::DeliveryHostRole,
    binding: &'scope B,
    now: SyncTime,
) -> impl std::future::Future<Output = Result<usize, DeliveryError>> + Send + 'scope
where
    D: DeliveryTypes,
    B: crate::SenderBinding<S, D>,
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        if crate::backfill_state::completed(scope, "submitters").await? {
            return Ok(0);
        }
        let rows = match role {
            crate::DeliveryHostRole::Machine => queries::read_submitter_backfill_rows(scope).await,
            crate::DeliveryHostRole::Relay => {
                queries::read_relay_submitter_backfill_rows(scope).await
            }
        }
        .map_err(sql_error)?;
        let changes = plan::<S, D, B>(scope, binding, rows).await?;
        let count = changes.len();
        for change in changes {
            match change {
                Change::Bind {
                    home,
                    item,
                    submitter,
                    compact,
                } => {
                    let id = item.as_uuid().to_string();
                    let kind = crate::submitter::kind(submitter).to_owned();
                    let owner = crate::submitter::id(submitter);
                    let stamp = now.unix_millis().to_string();
                    match home {
                        CustodyHome::Outbox => {
                            queries::bind_outbox_submitter(scope, id, kind, owner, stamp).await
                        }
                        CustodyHome::Inbox => {
                            queries::bind_inbox_submitter(scope, id, kind, owner, stamp).await
                        }
                    }
                    .map_err(sql_error)?;
                    if compact {
                        queries::compact_final_inbox(
                            scope,
                            item.as_uuid().to_string(),
                            now.unix_millis().to_string(),
                        )
                        .await
                        .map_err(sql_error)?;
                    }
                }
                Change::Compact { item } => {
                    queries::compact_final_inbox(
                        scope,
                        item.as_uuid().to_string(),
                        now.unix_millis().to_string(),
                    )
                    .await
                    .map_err(sql_error)?;
                }
                Change::Legacy { home, item } => {
                    let id = item.as_uuid().to_string();
                    let stamp = now.unix_millis().to_string();
                    match home {
                        CustodyHome::Outbox => queries::mark_legacy_outbox(scope, id, stamp).await,
                        CustodyHome::Inbox => queries::mark_legacy_inbox(scope, id, stamp).await,
                    }
                    .map_err(sql_error)?;
                }
            }
        }
        crate::backfill_state::complete(scope, "submitters", now).await?;
        Ok(count)
    }
}
