use crate::{
    DeliveryError, DeliveryLimits, GateDecision, checked_queries as queries, custody_record::*,
    outbox_record::*,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

pub struct Outbox<'s, S, D> {
    pub(crate) scope: &'s mut S,
    pub(crate) limits: DeliveryLimits,
    payload: PhantomData<fn() -> D>,
}
impl<'s, S, D> Outbox<'s, S, D> {
    pub fn new(scope: &'s mut S, limits: DeliveryLimits) -> Self {
        Self {
            scope,
            limits,
            payload: PhantomData,
        }
    }
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
    pub async fn resolve_submit(
        &mut self,
        reference: &DeliveryItemRef,
    ) -> Result<ResolvedSubmit<D>, DeliveryError> {
        let record = self
            .load(reference.item)
            .await?
            .ok_or(DeliveryError::Corrupt {
                field: "submit custody row missing",
            })?;
        if record.header.digest != reference.digest.as_str() {
            return Err(DeliveryError::Corrupt {
                field: "submit custody digest",
            });
        }
        let item = record.payload.ok_or(DeliveryError::Corrupt {
            field: "submit custody payload missing",
        })?;
        Ok(ResolvedSubmit {
            relay: record.header.relay,
            item,
        })
    }

    pub async fn gate<C>(
        &mut self,
        input: &SubmitInput,
        consumer: &C,
        now: SyncTime,
    ) -> Result<GateDecision, DeliveryError>
    where
        C: crate::DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        let record = match self.load(input.item).await {
            Ok(Some(record)) => record,
            Ok(None)
            | Err(DeliveryError::Corrupt { .. })
            | Err(DeliveryError::ProcessingFailed { .. }) => return Ok(GateDecision::Release),
            Err(error) => return Err(error),
        };
        if record.header.digest != input.digest.as_str()
            || record.header.relay != input.relay
            || record.header.recipient != input.mailbox
        {
            // The dispatch reconciles this identity without sending the body.
            return Ok(GateDecision::Release);
        }
        if record.header.status.is_terminal() || record.payload.is_none() {
            return Ok(GateDecision::Release);
        }
        if record.header.valid_until.is_some_and(|until| now >= until) {
            return Ok(GateDecision::Release);
        }
        match record.payload {
            Some(DeliveryEnvelope::Item(item)) => consumer.release_gate(self.scope, &item).await,
            Some(DeliveryEnvelope::Cancel(_)) | None => Ok(GateDecision::Release),
        }
    }

    pub async fn mark_submit_attempted(&mut self, item: ItemId) -> Result<(), DeliveryError> {
        let mut header = match self.header_state(item).await {
            Ok(Some(OutboxHeaderState::Item(header))) => header,
            Ok(_) | Err(DeliveryError::Corrupt { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        if let Err(error) = header.validate() {
            return match error {
                DeliveryError::Corrupt { .. } => Ok(()),
                error => Err(error),
            };
        }
        header.submit_attempted = true;
        self.update_header(&header).await
    }

    pub async fn mark_submitted(
        &mut self,
        input: &SubmitInput,
        answer: SubmitResult<D::Acceptance>,
    ) -> Result<(), DeliveryError> {
        let item = input.item;
        let mut header = match self.header_state(item).await {
            Ok(Some(OutboxHeaderState::Item(header)))
                if header.digest == input.digest.as_str()
                    && header.relay == input.relay
                    && header.recipient == input.mailbox =>
            {
                header
            }
            Ok(_) | Err(DeliveryError::Corrupt { .. }) => {
                return self.recover_answer(input, answer).await;
            }
            Err(error) => return Err(error),
        };
        if header.status.is_terminal() {
            return Ok(());
        }
        match answer {
            SubmitResult::Accepted {
                status,
                version,
                cancel,
                ..
            }
            | SubmitResult::Replayed {
                status,
                version,
                cancel,
            } => {
                if version > header.version {
                    header.status = status;
                    header.version = version;
                    header.cancel = cancel;
                }
            }
            SubmitResult::CancelAlreadyRequested { existing, .. } => {
                header.status = DeliveryStatus::Refused {
                    reason: DeliveryRefusal::CancelAlreadyRequested { existing },
                };
            }
            SubmitResult::Refused { reason } => header.status = DeliveryStatus::Refused { reason },
        }
        header.submit_attempted = true;
        self.save_header(&header, None).await
    }

    pub async fn status(
        &mut self,
        item: ItemId,
    ) -> Result<OutboxStatus<D::Acceptance>, DeliveryError> {
        let state = match self.header_state(item).await? {
            Some(OutboxHeaderState::Item(header)) => {
                header.validate()?;
                OutboxStatusState::Known {
                    status: header.status,
                    version: header.version,
                    cancel: header.cancel,
                }
            }
            Some(OutboxHeaderState::Recovered(record)) => OutboxStatusState::Known {
                status: record.status,
                version: record.version,
                cancel: record.cancel,
            },
            Some(OutboxHeaderState::Unresolvable { kind, .. }) => OutboxStatusState::Known {
                status: DeliveryStatus::Refused {
                    reason: DeliveryRefusal::ProcessingFailed { kind },
                },
                version: StatusVersion::new(0),
                cancel: None,
            },
            None => OutboxStatusState::Unknown,
        };
        Ok(OutboxStatus { item, state })
    }

    pub async fn record_failure(
        &mut self,
        item: ItemId,
        kind: DeliveryFailureKind,
    ) -> Result<(), DeliveryError> {
        match self.recovery_header(item).await {
            Ok(Some(OutboxHeaderState::Item(mut header))) => {
                if !header.status.is_terminal() {
                    header.status = DeliveryStatus::Refused {
                        reason: DeliveryRefusal::ProcessingFailed { kind },
                    };
                    self.save_header(&header, None).await?;
                }
            }
            Ok(Some(OutboxHeaderState::Recovered(mut record))) => {
                if !record.status.is_terminal() {
                    record.status = DeliveryStatus::Refused {
                        reason: DeliveryRefusal::ProcessingFailed { kind },
                    };
                    self.save_recovered(&record).await?;
                }
            }
            Ok(Some(OutboxHeaderState::Unresolvable { .. })) => {}
            Ok(None) | Err(DeliveryError::Corrupt { .. }) => {
                let failed = OutboxHeaderState::<D::Acceptance>::Unresolvable { item, kind };
                queries::write_outbox_item(
                    self.scope,
                    item_key(item),
                    "0".into(),
                    "0".into(),
                    "0".into(),
                    encode(&failed)?,
                    None,
                    "0".into(),
                )
                .await
                .map_err(sql_error)?;
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    pub async fn record_submission_failure(
        &mut self,
        input: &SubmitInput,
        kind: DeliveryFailureKind,
    ) -> Result<(), DeliveryError> {
        self.recover_answer(
            input,
            SubmitResult::Refused {
                reason: DeliveryRefusal::ProcessingFailed { kind },
            },
        )
        .await
    }
    async fn recover_answer(
        &mut self,
        input: &SubmitInput,
        answer: SubmitResult<D::Acceptance>,
    ) -> Result<(), DeliveryError> {
        let (status, version, cancel) = match answer {
            SubmitResult::Accepted {
                status,
                version,
                cancel,
                ..
            }
            | SubmitResult::Replayed {
                status,
                version,
                cancel,
            } => (status, version, cancel),
            SubmitResult::CancelAlreadyRequested { existing, .. } => (
                DeliveryStatus::Refused {
                    reason: DeliveryRefusal::CancelAlreadyRequested { existing },
                },
                StatusVersion::new(0),
                None,
            ),
            SubmitResult::Refused { reason } => (
                DeliveryStatus::Refused { reason },
                StatusVersion::new(0),
                None,
            ),
        };
        self.save_recovered(&RecoveredOutbox {
            item: input.item,
            digest: input.digest.clone(),
            status,
            version,
            cancel,
        })
        .await
    }
    pub(crate) async fn save_recovered(
        &mut self,
        record: &RecoveredOutbox<D::Acceptance>,
    ) -> Result<(), DeliveryError> {
        let state = OutboxHeaderState::<D::Acceptance>::Recovered(copy_wire(record)?);
        queries::write_outbox_item(
            self.scope,
            item_key(record.item),
            "0".into(),
            "0".into(),
            "0".into(),
            encode(&state)?,
            None,
            "0".into(),
        )
        .await
        .map_err(sql_error)
    }
    pub(crate) async fn header_state(
        &mut self,
        item: ItemId,
    ) -> Result<Option<OutboxHeaderState<D::Acceptance>>, DeliveryError> {
        let Some(row) = queries::read_outbox_item(self.scope, item_key(item))
            .await
            .map_err(sql_error)?
        else {
            return Ok(None);
        };
        let state = self.decode_header(row.record, item)?;
        let live = integer(row.live, row.live_class, "outbox live flag")?;
        let slot = integer(row.capacity_slot, row.slot_class, "outbox capacity slot")?;
        let bytes = integer(row.payload_bytes, row.payload_class, "outbox payload bytes")?;
        let settlement = integer(
            row.settlement_bytes,
            row.settlement_class,
            "outbox settlement bytes",
        )?;
        let valid = match &state {
            OutboxHeaderState::Item(header) => {
                header.validate()?;
                crate::delivery_sizes::check_status::<D>(&header.status).map_err(|_| {
                    DeliveryError::Corrupt {
                        field: "stored outbox status bounds",
                    }
                })?;
                live == u64::from(!header.status.is_terminal())
                    && slot == u64::from(header.capacity_slot)
                    && bytes
                        == u64::try_from(row.payload.as_ref().map_or(0, String::len))
                            .map_err(|_| DeliveryError::SequenceExhausted)?
                    && settlement
                        == if header.status.is_terminal() {
                            0
                        } else {
                            u64::from(header.settlement_bytes)
                        }
                    && (!header.status.is_terminal() || row.payload.is_none())
            }
            OutboxHeaderState::Recovered(record) => {
                record.version.value() <= i64::MAX as u64
                    && record.status.rank() > 0
                    && live == 0
                    && slot == 0
                    && bytes == 0
                    && settlement == 0
                    && row.payload.is_none()
            }
            OutboxHeaderState::Unresolvable { .. } => {
                live == 0 && slot == 0 && bytes == 0 && settlement == 0 && row.payload.is_none()
            }
        };
        if !valid || row.item_id.as_deref() != Some(item_key(item).as_str()) {
            return Err(DeliveryError::Corrupt {
                field: "outbox row metadata",
            });
        }
        Ok(Some(state))
    }
    pub(crate) async fn recovery_header(
        &mut self,
        item: ItemId,
    ) -> Result<Option<OutboxHeaderState<D::Acceptance>>, DeliveryError> {
        let Some(row) = queries::read_outbox_item(self.scope, item_key(item))
            .await
            .map_err(sql_error)?
        else {
            return Ok(None);
        };
        self.decode_header(row.record, item).map(Some)
    }
    fn decode_header(
        &self,
        raw: Option<String>,
        item: ItemId,
    ) -> Result<OutboxHeaderState<D::Acceptance>, DeliveryError> {
        let state: OutboxHeaderState<D::Acceptance> = decode(raw, "outbox header")?;
        let id = match &state {
            OutboxHeaderState::Item(header) => header.id,
            OutboxHeaderState::Recovered(record) => record.item,
            OutboxHeaderState::Unresolvable { item, .. } => *item,
        };
        if id != item {
            return Err(DeliveryError::Corrupt {
                field: "outbox header identity",
            });
        }
        Ok(state)
    }
    pub(crate) async fn load(
        &mut self,
        item: ItemId,
    ) -> Result<Option<OutboxRecord<D>>, DeliveryError> {
        let Some(row) = queries::read_outbox_item(self.scope, item_key(item))
            .await
            .map_err(sql_error)?
        else {
            return Ok(None);
        };
        let state = self.decode_header(row.record, item)?;
        let header = match state {
            OutboxHeaderState::Item(header) => header,
            OutboxHeaderState::Recovered(_) => return Err(DeliveryError::CustodyMissing),
            OutboxHeaderState::Unresolvable { kind, .. } => {
                return Err(DeliveryError::ProcessingFailed { kind });
            }
        };
        let raw_size = row.payload.as_ref().map_or(0, String::len);
        let payload = row
            .payload
            .map(|raw| decode(Some(raw), "outbox custody payload"))
            .transpose()?;
        let record = OutboxRecord { header, payload };
        record.validate()?;
        crate::delivery_sizes::check_status::<D>(&record.header.status).map_err(|_| {
            DeliveryError::Corrupt {
                field: "stored outbox status bounds",
            }
        })?;
        if let Some(payload) = &record.payload {
            crate::delivery_sizes::check_envelope(payload).map_err(|_| DeliveryError::Corrupt {
                field: "stored outbox envelope bounds",
            })?;
        }
        let bytes = integer(row.payload_bytes, row.payload_class, "outbox payload bytes")?;
        let live = integer(row.live, row.live_class, "outbox live flag")?;
        let slot = integer(row.capacity_slot, row.slot_class, "outbox capacity slot")?;
        let settlement = integer(
            row.settlement_bytes,
            row.settlement_class,
            "outbox settlement bytes",
        )?;
        if row.item_id.as_deref() != Some(item_key(item).as_str())
            || slot != u64::from(record.header.capacity_slot)
            || bytes != u64::try_from(raw_size).map_err(|_| DeliveryError::SequenceExhausted)?
            || live != u64::from(!record.header.status.is_terminal())
            || settlement
                != if record.header.status.is_terminal() {
                    0
                } else {
                    u64::from(record.header.settlement_bytes)
                }
        {
            return Err(DeliveryError::Corrupt {
                field: "outbox row metadata",
            });
        }
        Ok(Some(record))
    }
    pub(crate) async fn save(&mut self, record: &OutboxRecord<D>) -> Result<(), DeliveryError> {
        record.validate()?;
        self.save_header(
            &record.header,
            record.payload.as_ref().map(encode).transpose()?,
        )
        .await
    }
    pub(crate) async fn update_header(
        &mut self,
        header: &OutboxHeader<D::Acceptance>,
    ) -> Result<(), DeliveryError> {
        let row = queries::read_outbox_item(self.scope, item_key(header.id))
            .await
            .map_err(sql_error)?
            .ok_or(DeliveryError::CustodyMissing)?;
        self.save_header(header, row.payload).await
    }
    pub(crate) async fn save_header(
        &mut self,
        header: &OutboxHeader<D::Acceptance>,
        payload: Option<String>,
    ) -> Result<(), DeliveryError> {
        header.validate()?;
        let state = OutboxHeaderState::<D::Acceptance>::Item(copy_wire(header)?);
        queries::write_outbox_item(
            self.scope,
            item_key(header.id),
            u8::from(!header.status.is_terminal()).to_string(),
            payload.as_ref().map_or(0, String::len).to_string(),
            if header.status.is_terminal() {
                0
            } else {
                header.settlement_bytes
            }
            .to_string(),
            encode(&state)?,
            payload,
            u8::from(header.capacity_slot).to_string(),
        )
        .await
        .map_err(sql_error)
    }
}
