//! Sender reconciliation uses retained relay ownership after content release.
use crate::{DeliveryError, Outbox, outbox_record::*};
use durable_steps::{
    JournalScope, Task,
    storage::{JournalStorage, TransactionScope},
};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

fn sender_record<A>(
    state: OutboxHeaderState<A>,
    offered: &ItemDigest,
) -> Result<RecoveredOutbox<A>, DeliveryError> {
    Ok(match state {
        OutboxHeaderState::Item(header) => RecoveredOutbox {
            submitter: header.submitter,
            acceptance_bound: header.acceptance_bound,
            item: header.id,
            digest: ItemDigest::try_from(header.digest).map_err(|_| DeliveryError::Corrupt {
                field: "relay digest",
            })?,
            status: header.status,
            version: header.version,
            cancel: header.cancel,
        },
        OutboxHeaderState::Recovered(record) => record,
        OutboxHeaderState::Unresolvable { item, kind } => RecoveredOutbox {
            submitter: None,
            acceptance_bound: None,
            item,
            digest: offered.clone(),
            status: DeliveryStatus::Refused {
                reason: DeliveryRefusal::ProcessingFailed { kind },
            },
            version: StatusVersion::new(0),
            cancel: None,
        },
    })
}

impl<S, D> Outbox<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    /// Read one item for its authenticated submitter, including released content.
    pub async fn relay_query(
        &mut self,
        submitter: PeerId,
        item: ItemId,
        digest: ItemDigest,
    ) -> Result<ItemStatus<D::Acceptance>, DeliveryError> {
        let Some(state) = self.header_state(item).await? else {
            return Ok(ItemStatus {
                item,
                state: ItemStatusState::Unknown,
            });
        };
        let record = sender_record(state, &digest)?;
        if record.submitter != Some(submitter) {
            return Err(DeliveryError::Refused {
                reason: DeliveryRefusal::NotSubmitter,
            });
        }
        if record.digest != digest {
            return Err(DeliveryError::DigestMismatch);
        }
        Ok(ItemStatus {
            item,
            state: ItemStatusState::Known {
                digest: record.digest,
                status: record.status,
                version: record.version,
                cancel: record.cancel,
            },
        })
    }

    /// Bind an unknown item's first authenticated settlement in the caller's transaction.
    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send avoids async-fn/GAT lifetime generality errors in journal transactions"
    )]
    pub fn relay_settle<'call, TRetention>(
        &'call mut self,
        submitter: PeerId,
        item: ItemId,
        digest: ItemDigest,
        now: SyncTime,
    ) -> impl std::future::Future<Output = Result<SettleResult<D::Acceptance>, DeliveryError>>
    + Send
    + 'call
    where
        S: JournalScope + JournalStorage,
        TRetention: Task<Input = ItemId>,
    {
        async move {
            if now >= crate::removal_deadline(item) {
                return Err(DeliveryError::Refused {
                    reason: DeliveryRefusal::TooOld,
                });
            }
            if let Some(state) = self.header_state(item).await? {
                if matches!(&state,OutboxHeaderState::Item(header) if header.submitter.is_none()) {
                    return Err(DeliveryError::Corrupt {
                        field: "relay submitter missing",
                    });
                }
                let mut record = sender_record(state, &digest)?;
                if record.digest != digest
                    || record.submitter.is_some_and(|owner| owner != submitter)
                {
                    return Ok(SettleResult::ItemConflict {
                        existing: record.digest,
                    });
                }
                if record.submitter.is_none() {
                    record.submitter = Some(submitter);
                    self.save_recovered(&record, now).await?;
                    crate::admit_record_removal::<_, TRetention>(self.scope, item).await?;
                }
                return Ok(SettleResult::Replayed {
                    status: record.status,
                    version: record.version,
                    cancel: record.cancel,
                });
            }
            let reason = DeliveryFailureKind::CustodyUnresolvable;
            let version = StatusVersion::new(1);
            self.save_recovered(
                &RecoveredOutbox {
                    submitter: Some(submitter),
                    acceptance_bound: None,
                    item,
                    digest,
                    status: DeliveryStatus::Refused {
                        reason: DeliveryRefusal::ProcessingFailed { kind: reason },
                    },
                    version,
                    cancel: None,
                },
                now,
            )
            .await?;
            crate::admit_record_removal::<_, TRetention>(self.scope, item).await?;
            Ok(SettleResult::Settled { reason, version })
        }
    }
}
