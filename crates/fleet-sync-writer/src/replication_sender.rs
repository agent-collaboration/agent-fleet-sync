use crate::{
    CaptureBookkeeping, CapturedTransaction, MaintenanceTransaction, OpenError, PieceAnnotation,
    RejectedTail, ReplicatedDatabase, ReplicatedSchema, ReplicationEvents, ReplicationTransport,
    SenderCopyStatus, SenderError, SenderOptions,
    capture_stream::{CapturedCommit, read_window},
    piece_builder::cut_transaction,
    sender_storage,
    writer_file_record::read_file_record,
};
use fleet_sync_lease::WriterState;
use fleet_sync_protocol::{
    ConsumerProtocol, CopyRole, LeaseRefusal, Piece, PieceDigest, PieceRefusal, ReceiverId,
    RenewAnswer, ReplicationAnswer, ReplicationPosition, TransactionPart,
};
use sqlx_core::connection::Connection;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub struct ReplicationSender<
    S: ReplicatedSchema,
    A: PieceAnnotation,
    B: CaptureBookkeeping,
    T: ReplicationTransport<A::Protocol>,
> {
    pub(crate) database: Arc<ReplicatedDatabase<S>>,
    pub(crate) annotation: Arc<A>,
    pub(crate) bookkeeping: Arc<B>,
    pub(crate) transport: Arc<T>,
    pub(crate) options: SenderOptions,
    copy_locks: Mutex<BTreeMap<ReceiverId, Arc<tokio::sync::Mutex<()>>>>,
}

enum CopyPreparation<A> {
    Idle,
    NeedsSnapshot,
    Pieces(Vec<Piece<A>>),
}

pub(crate) struct SentPiece {
    after: ReplicationPosition,
    through: ReplicationPosition,
    part: Option<TransactionPart>,
}

impl<S, A, B, T> ReplicationSender<S, A, B, T>
where
    S: ReplicatedSchema,
    A: PieceAnnotation,
    B: CaptureBookkeeping,
    T: ReplicationTransport<A::Protocol>,
{
    pub fn new(
        database: Arc<ReplicatedDatabase<S>>,
        annotation: Arc<A>,
        bookkeeping: Arc<B>,
        transport: Arc<T>,
        options: SenderOptions,
    ) -> Self {
        Self {
            database,
            annotation,
            bookkeeping,
            transport,
            options,
            copy_locks: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn add_copy(
        &self,
        copy: ReceiverId,
        role: CopyRole,
        start: ReplicationPosition,
    ) -> Result<(), SenderError> {
        let _control = self.database.control.lock().await;
        let mut connection = self.database.maintenance.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let record = read_file_record(&mut transaction)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        if start < record.start
            || start > crate::capture_stream::capture_head(&mut transaction, &record).await?
        {
            return Err(SenderError::Invalid {
                field: "copy.start",
            });
        }
        let copies = sender_storage::copies(&mut transaction, &record).await?;
        if let Some(existing) = copies.iter().find(|existing| existing.copy == copy) {
            if existing.role != role || existing.start != start {
                return Err(SenderError::Invalid {
                    field: "copy.registration_conflict",
                });
            }
        } else {
            sender_storage::save_copy(
                &mut transaction,
                &SenderCopyStatus {
                    copy,
                    role,
                    start,
                    acked: start,
                    needs_snapshot: start < record.retained_after,
                },
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn copies(&self) -> Result<Vec<SenderCopyStatus>, SenderError> {
        let mut connection = self.database.maintenance.acquire().await?;
        let mut transaction = connection.begin().await?;
        let record = read_file_record(&mut transaction)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        let copies = sender_storage::copies(&mut transaction, &record).await?;
        transaction.commit().await?;
        Ok(copies)
    }

    pub async fn rejected_tails(&self) -> Result<Vec<RejectedTail>, SenderError> {
        let mut connection = self.database.maintenance.acquire().await?;
        sender_storage::rejected_tails(&mut connection).await
    }

    pub async fn acknowledge_rejected_tail(
        &self,
        epoch: fleet_sync_protocol::Epoch,
    ) -> Result<(), SenderError> {
        let _control = self.database.control.lock().await;
        let mut connection = self.database.maintenance.acquire().await?;
        crate::checked_queries::delete_rejected_tail(
            &mut crate::writer_scope::WriterScope(&mut connection),
            epoch.value().to_string(),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn record_capture(
        &self,
        connection: &mut sqlx_turso::TursoConnection,
        captured: &CapturedCommit,
        tables: Vec<crate::TableLayout>,
    ) -> Result<(), SenderError> {
        let digest = PieceDigest::of_transaction(&captured.transaction).map_err(|_| {
            SenderError::Invalid {
                field: "capture.digest",
            }
        })?;
        if sender_storage::assigned(connection, captured.position, &digest).await? {
            return Ok(());
        }
        self.bookkeeping
            .record(
                &mut MaintenanceTransaction::new(connection, tables),
                &CapturedTransaction {
                    position: captured.position,
                    transaction: &captured.transaction,
                },
            )
            .await?;
        sender_storage::assign(connection, captured.position, digest).await
    }

    async fn prepare(
        &self,
        copy: ReceiverId,
    ) -> Result<CopyPreparation<<A::Protocol as ConsumerProtocol>::Annotation>, SenderError> {
        let mut control = self.database.control.lock().await;
        let mut connection = self.database.maintenance.acquire().await?;
        crate::writer_authority::restore_authority(&mut connection, &mut control.holder).await?;
        let state = control.holder.writer_state(self.database.clock.now());
        let superseded = matches!(state, Ok(WriterState::Superseded { .. }));
        if !matches!(
            state,
            Ok(WriterState::Writing { .. }
                | WriterState::Paused { .. }
                | WriterState::Draining { .. }
                | WriterState::Released { .. }
                | WriterState::Superseded { .. })
        ) {
            return Ok(CopyPreparation::Idle);
        }
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let record = read_file_record(&mut transaction)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        let status = sender_storage::copies(&mut transaction, &record)
            .await?
            .into_iter()
            .find(|status| status.copy == copy)
            .ok_or(SenderError::Invalid {
                field: "copy.unknown",
            })?;
        if superseded
            && (status.role != CopyRole::Authoritative
                || record.epoch_ended_at.is_some()
                || sender_storage::rejected_tails(&mut transaction)
                    .await?
                    .iter()
                    .any(|tail| tail.epoch == record.epoch))
        {
            return Ok(CopyPreparation::Idle);
        }
        if status.needs_snapshot {
            return Ok(CopyPreparation::NeedsSnapshot);
        }
        let commits = read_window(&mut transaction, &record, status.acked, 1).await?;
        let Some(captured) = commits.first() else {
            return Ok(CopyPreparation::Idle);
        };
        self.record_capture(
            &mut transaction,
            captured,
            control.statements.layouts().to_vec(),
        )
        .await?;
        let pieces = cut_transaction(
            record.database,
            record.epoch,
            status.acked,
            captured,
            self.options.max_part_items,
            self.annotation.as_ref(),
        )?;
        transaction.commit().await?;
        Ok(CopyPreparation::Pieces(pieces))
    }

    /// One copy's request loop serializes independently of every other copy.
    /// Capture assignment commits before the transport is called.
    pub async fn replicate_once(
        &self,
        copy: ReceiverId,
    ) -> Result<Vec<ReplicationEvents>, SenderError> {
        let lock = self.copy_lock(copy)?;
        let _copy_guard = lock.lock().await;
        let mut events = self.enforce_retention().await?;
        let pieces = match self.prepare(copy).await? {
            CopyPreparation::Idle => return Ok(events),
            CopyPreparation::NeedsSnapshot => {
                if !events.iter().any(|event|matches!(event,ReplicationEvents::CopyNeedsSnapshot{copy:changed} if *changed==copy)) { events.push(ReplicationEvents::CopyNeedsSnapshot{copy}); }
                return Ok(events);
            }
            CopyPreparation::Pieces(pieces) => pieces,
        };
        for piece in pieces {
            let sent = SentPiece {
                after: piece.continues_from,
                through: piece.through,
                part: piece.part.clone(),
            };
            let answer = self.transport.send_piece(copy, piece).await?;
            let (mut answered, done) = self.answer(copy, &sent, answer).await?;
            events.append(&mut answered);
            if done {
                break;
            }
        }
        Ok(events)
    }

    pub(crate) fn copy_lock(
        &self,
        copy: ReceiverId,
    ) -> Result<Arc<tokio::sync::Mutex<()>>, SenderError> {
        Ok(self
            .copy_locks
            .lock()
            .map_err(|_| SenderError::Invalid { field: "copy.lock" })?
            .entry(copy)
            .or_default()
            .clone())
    }

    async fn answer(
        &self,
        copy: ReceiverId,
        sent: &SentPiece,
        answer: ReplicationAnswer,
    ) -> Result<(Vec<ReplicationEvents>, bool), SenderError> {
        let mut control = self.database.control.lock().await;
        let mut connection = self.database.maintenance.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let record = read_file_record(&mut transaction)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        let mut status = sender_storage::copies(&mut transaction, &record)
            .await?
            .into_iter()
            .find(|status| status.copy == copy)
            .ok_or(SenderError::Invalid {
                field: "copy.unknown",
            })?;
        let mut events = Vec::new();
        let done = match answer {
            ReplicationAnswer::CopyUnavailable { reason } => {
                return Err(SenderError::CopyUnavailable { copy, reason });
            }
            ReplicationAnswer::OutcomeUnknown { .. } => {
                return Err(crate::SendFailure::AnswerLost.into());
            }
            ReplicationAnswer::Applied { through }
            | ReplicationAnswer::AlreadyApplied { through } => {
                if through != sent.through {
                    return Err(SenderError::Invalid {
                        field: "answer.through",
                    });
                }
                if through > status.acked {
                    status.acked = through;
                    sender_storage::save_copy(&mut transaction, &status).await?;
                    events.push(ReplicationEvents::Acknowledged { copy, through });
                }
                true
            }
            ReplicationAnswer::PartStaged {
                transaction,
                index,
                acked,
            } => {
                if sent.part.as_ref().is_none_or(|part| {
                    part.transaction != transaction
                        || part.index != index
                        || part.index + 1 == part.count
                }) || acked != sent.after
                {
                    return Err(SenderError::Invalid {
                        field: "answer.part",
                    });
                }
                false
            }
            ReplicationAnswer::Refused(PieceRefusal::Gap { acked }) => {
                if acked < status.start
                    || acked
                        > crate::capture_stream::capture_head(&mut transaction, &record).await?
                {
                    return Err(SenderError::Invalid {
                        field: "answer.gap",
                    });
                }
                status.acked = acked;
                if acked < record.retained_after {
                    status.needs_snapshot = true;
                    events.push(ReplicationEvents::CopyNeedsSnapshot { copy });
                }
                sender_storage::save_copy(&mut transaction, &status).await?;
                true
            }
            ReplicationAnswer::Refused(
                PieceRefusal::NeedsSnapshot { .. } | PieceRefusal::NotInitialized,
            ) => {
                if !status.needs_snapshot {
                    status.needs_snapshot = true;
                    sender_storage::save_copy(&mut transaction, &status).await?;
                    events.push(ReplicationEvents::CopyNeedsSnapshot { copy });
                }
                true
            }
            ReplicationAnswer::Refused(reason @ PieceRefusal::StaleEpoch { newer, ended_at }) => {
                if status.role != CopyRole::Authoritative {
                    return Err(SenderError::Refused { copy, reason });
                }
                let head = crate::capture_stream::capture_head(&mut transaction, &record).await?;
                if newer <= record.epoch || ended_at > head {
                    return Err(SenderError::Invalid {
                        field: "answer.stale_epoch",
                    });
                }
                if head > ended_at {
                    let tail = RejectedTail {
                        database: record.database,
                        epoch: record.epoch,
                        ended_at,
                        last_commit: head,
                    };
                    let existing = sender_storage::rejected_tails(&mut transaction)
                        .await?
                        .iter()
                        .any(|existing| existing.epoch == record.epoch);
                    sender_storage::save_rejected_tail(&mut transaction, &tail).await?;
                    if !existing {
                        events.push(ReplicationEvents::RejectedTail(tail));
                    }
                } else if status.acked < head {
                    status.acked = head;
                    sender_storage::save_copy(&mut transaction, &status).await?;
                    events.push(ReplicationEvents::Acknowledged {
                        copy,
                        through: head,
                    });
                }
                let observation = crate::writer_authority::save_observation(
                    &mut transaction,
                    crate::writer_authority::AuthorityObservation::Renewal {
                        answer: RenewAnswer::Refused(LeaseRefusal::StaleEpoch {
                            current_epoch: newer,
                            epoch_ended_at: ended_at,
                        }),
                        request_started: self.database.clock.now(),
                    },
                )
                .await?;
                transaction.commit().await?;
                observation.restore(&mut control.holder, record.epoch)?;
                events.push(ReplicationEvents::LeaseChanged(
                    control
                        .holder
                        .writer_state(self.database.clock.now())
                        .map_err(|_| SenderError::Invalid {
                            field: "holder.state",
                        })?,
                ));
                return Ok((events, true));
            }
            ReplicationAnswer::Refused(
                reason @ (PieceRefusal::Retired | PieceRefusal::NotWriter { .. }),
            ) => {
                let observation = if status.role == CopyRole::Authoritative {
                    let refusal = match &reason {
                        PieceRefusal::Retired => LeaseRefusal::Retired,
                        PieceRefusal::NotWriter { current } => {
                            LeaseRefusal::NotWriter { current: *current }
                        }
                        _ => {
                            return Err(SenderError::Invalid {
                                field: "answer.refusal",
                            });
                        }
                    };
                    Some(
                        crate::writer_authority::save_observation(
                            &mut transaction,
                            crate::writer_authority::AuthorityObservation::Renewal {
                                answer: RenewAnswer::Refused(refusal),
                                request_started: self.database.clock.now(),
                            },
                        )
                        .await?,
                    )
                } else {
                    None
                };
                transaction.commit().await?;
                if let Some(observation) = observation {
                    observation.restore(&mut control.holder, record.epoch)?;
                }
                return Err(SenderError::Refused { copy, reason });
            }
            ReplicationAnswer::Refused(reason) => {
                return Err(SenderError::Refused { copy, reason });
            }
            _ => {
                return Err(SenderError::Invalid {
                    field: "answer.kind",
                });
            }
        };
        transaction.commit().await?;
        Ok((events, done))
    }
}
