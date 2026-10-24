use crate::{DeliveryError, checked_queries as queries, custody_record::*, mailbox_rules::Mailbox};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sha2::{Digest, Sha256};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

impl<S, D> Mailbox<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub async fn fetch(
        &mut self,
        recipient: PeerId,
        stream: DeliveryStream,
        after: DeliverySeq,
        max_items: u32,
        max_bytes: u64,
        now: SyncTime,
    ) -> Result<FetchResult<D>, DeliveryError> {
        self.fetch_with_budget(
            recipient,
            crate::FetchInput {
                stream,
                after,
                max_items,
                max_bytes,
                inbox_room_bytes: None,
            },
            now,
        )
        .await
    }

    /// Select a batch against both wire bytes and the receiver's available room.
    /// The legacy fetch entry point supplies no room budget.
    pub async fn fetch_with_budget(
        &mut self,
        recipient: PeerId,
        input: crate::FetchInput,
        now: SyncTime,
    ) -> Result<FetchResult<D>, DeliveryError> {
        let crate::FetchInput {
            stream,
            after,
            max_items,
            max_bytes,
            inbox_room_bytes,
        } = input;
        let inbox_room_bytes = if D::PER_ITEM_SETTLEMENT {
            inbox_room_bytes
        } else {
            None
        };
        if recipient != self.storage.owner {
            return Err(DeliveryError::InvalidBatch);
        }
        let state = self.storage.state().await?;
        if after > state.head(stream) {
            return Err(DeliveryError::InvalidBatch);
        }
        let rows = queries::read_mail_batch(
            self.storage.scope,
            stream_key(stream),
            after.value().to_string(),
            max_items.to_string(),
        )
        .await
        .map_err(sql_error)?;
        let mut candidates = Vec::new();
        for row in rows {
            let seq = DeliverySeq::new(integer(
                row.seq.clone(),
                row.seq_class.clone(),
                "fetch sequence",
            )?);
            match self.storage.decode_mail::<D>(row, None)? {
                MailRecord::Live(record) => candidates.push((seq, Some(record), None)),
                MailRecord::Compacted(_) => candidates.push((seq, None, None)),
            }
        }
        if stream == DeliveryStream::Controls {
            for row in queries::read_controls(
                self.storage.scope,
                after.value().to_string(),
                max_items.to_string(),
            )
            .await
            .map_err(sql_error)?
            {
                let seq = DeliverySeq::new(integer(row.seq, row.seq_class, "retirement sequence")?);
                let entry: FetchEntry<D> = decode(row.record, "retirement control")?;
                if seq != entry.sequence() || !matches!(entry, FetchEntry::Retired { .. }) {
                    return Err(DeliveryError::Corrupt {
                        field: "retirement control identity",
                    });
                }
                candidates.push((seq, None, Some(entry)));
            }
        }
        candidates.sort_by_key(|candidate| candidate.0);
        let mut entries = Vec::new();
        let mut used_bytes = 0_u64;
        let mut used_room = 0_u64;
        let mut through = after;
        for (seq, mail, retirement) in candidates {
            if entries.len() >= max_items as usize {
                break;
            }
            if let Some(mut mail) = mail {
                // Validate above before deciding that a released row has no work.
                if mail.payload.is_none() {
                    through = seq;
                    continue;
                }
                if mail
                    .payload
                    .as_ref()
                    .is_some_and(|item| item.valid_until().is_some_and(|until| now >= until))
                    && !mail.status.is_terminal()
                    && !mail.fetched
                {
                    mail.status = DeliveryStatus::Expired;
                    mail.version = next_version(mail.version)?;
                    mail.payload = None;
                    mail.released_at = Some(now);
                    self.storage.save_live(&mail).await?;
                    self.owe(&mail).await?;
                    through = seq;
                    continue;
                }
                let payload = mail.payload.as_ref().ok_or(DeliveryError::Corrupt {
                    field: "fetch payload missing",
                })?;
                let entry = FetchEntry::Item {
                    seq,
                    item: copy_wire(payload)?,
                };
                let size = u64::try_from(encode(&entry)?.len())
                    .map_err(|_| DeliveryError::SequenceExhausted)?;
                if used_bytes
                    .checked_add(size)
                    .is_none_or(|size| size > max_bytes)
                {
                    break;
                }
                if let Some(available) = inbox_room_bytes {
                    let settlement = match payload {
                        DeliveryEnvelope::Item(item) => {
                            self.limits.settlement_for::<D>(item.body())?
                        }
                        DeliveryEnvelope::Cancel(_) => 0,
                    };
                    let room = size
                        .checked_add(u64::from(settlement))
                        .ok_or(DeliveryError::SequenceExhausted)?;
                    let Some(total) = used_room
                        .checked_add(room)
                        .filter(|total| *total <= available)
                    else {
                        break;
                    };
                    used_room = total;
                }
                mail.fetched = true;
                self.storage.save_live(&mail).await?;
                entries.push(entry);
                used_bytes += size;
            } else if let Some(entry) = retirement {
                let size = u64::try_from(encode(&entry)?.len())
                    .map_err(|_| DeliveryError::SequenceExhausted)?;
                if used_bytes
                    .checked_add(size)
                    .is_none_or(|size| size > max_bytes)
                {
                    break;
                }
                if let Some(available) = inbox_room_bytes {
                    let Some(total) = used_room
                        .checked_add(size)
                        .filter(|total| *total <= available)
                    else {
                        break;
                    };
                    used_room = total;
                }
                used_bytes += size;
                entries.push(entry);
            }
            through = seq;
        }
        Ok(FetchResult {
            stream,
            entries,
            through,
        })
    }

    pub async fn received(
        &mut self,
        recipient: PeerId,
        stream: DeliveryStream,
        through: DeliverySeq,
        now: SyncTime,
    ) -> Result<ReceivedResult, DeliveryError> {
        if recipient != self.storage.owner {
            return Err(DeliveryError::InvalidBatch);
        }
        let mut state = self.storage.state().await?;
        if through > state.head(stream) {
            return Err(DeliveryError::InvalidBatch);
        }
        let previous = match stream {
            DeliveryStream::Items => state.received_items,
            DeliveryStream::Controls => state.received_controls,
        };
        if through <= previous {
            return Ok(ReceivedResult {
                stream,
                released_through: previous,
            });
        }
        let rows = queries::read_mail_receipt_batch(
            self.storage.scope,
            stream_key(stream),
            previous.value().to_string(),
            through.value().to_string(),
        )
        .await
        .map_err(sql_error)?;
        let mut releases = Vec::new();
        for row in rows {
            let MailRecord::Live(record) = self.storage.decode_mail::<D>(row, None)? else {
                continue;
            };
            if record.payload.is_none() {
                continue;
            }
            if !record.fetched {
                return Err(DeliveryError::InvalidBatch);
            }
            releases.push(record);
        }
        for mut record in releases {
            record.payload = None;
            record.released_at = Some(now);
            if record.status.rank() < DeliveryStatus::<D::Acceptance>::AtRecipient.rank() {
                record.status = DeliveryStatus::AtRecipient;
                record.version = next_version(record.version)?;
                self.owe(&record).await?;
            }
            self.storage.save_live(&record).await?;
        }
        match stream {
            DeliveryStream::Items => state.received_items = through,
            DeliveryStream::Controls => {
                state.received_controls = through;
                queries::drop_controls(self.storage.scope, through.value().to_string())
                    .await
                    .map_err(sql_error)?;
            }
        }
        self.storage.save_state(&state).await?;
        Ok(ReceivedResult {
            stream,
            released_through: through,
        })
    }

    pub async fn retire(
        &mut self,
        origin: PeerId,
        call: &CallId,
        items: &[ItemId],
    ) -> Result<RetireResult, DeliveryError> {
        let mut unique = std::collections::BTreeSet::new();
        if items.iter().any(|item| !unique.insert(*item)) {
            return Err(DeliveryError::InvalidBatch);
        }
        let request = encode(&(self.storage.owner, origin, items))?;
        let digest = format!("{:x}", Sha256::digest(request.as_bytes()));
        let call_key = encode(call)?;
        if let Some(saved) = queries::read_receipt(self.storage.scope, call_key.clone())
            .await
            .map_err(sql_error)?
        {
            if saved.request_digest.as_deref() != Some(digest.as_str()) {
                return Err(DeliveryError::CallConflict);
            }
            let answer: RetireResult = decode(saved.answer, "retirement receipt answer")?;
            let result_ids: std::collections::BTreeSet<_> = answer
                .retired
                .iter()
                .chain(answer.unknown.iter())
                .chain(answer.not_current_target.iter())
                .copied()
                .collect();
            if result_ids != unique
                || answer.retired.len() + answer.unknown.len() + answer.not_current_target.len()
                    != items.len()
            {
                return Err(DeliveryError::Corrupt {
                    field: "retirement receipt items",
                });
            }
            return Ok(answer);
        }
        let mut state = self.storage.state().await?;
        let mut answer = RetireResult {
            retired: Vec::new(),
            unknown: Vec::new(),
            not_current_target: Vec::new(),
        };
        let mut newly_retired = Vec::new();
        for item in items {
            match self.storage.item::<D>(*item).await? {
                Some(MailRecord::Live(record)) if record.status_target.peer != origin => {
                    answer.not_current_target.push(*item);
                }
                Some(MailRecord::Live(record)) if record.status.is_terminal() => {
                    self.storage.save_compacted(&record).await?;
                    answer.retired.push(*item);
                    newly_retired.push(*item);
                }
                Some(MailRecord::Compacted(_)) => answer.retired.push(*item),
                _ => answer.unknown.push(*item),
            }
        }
        if !newly_retired.is_empty() {
            let seq = state.allocate(DeliveryStream::Controls)?;
            let control = FetchEntry::<D>::Retired {
                seq,
                items: newly_retired,
            };
            queries::write_control(
                self.storage.scope,
                seq.value().to_string(),
                encode(&control)?,
            )
            .await
            .map_err(sql_error)?;
            self.storage.save_state(&state).await?;
        }
        queries::write_receipt(self.storage.scope, call_key, digest, encode(&answer)?)
            .await
            .map_err(sql_error)?;
        Ok(answer)
    }
}
