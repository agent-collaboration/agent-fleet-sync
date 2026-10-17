use crate::{
    CaptureBookkeeping, OpenError, PieceAnnotation, ReplicatedSchema, ReplicationEvents,
    ReplicationSender, ReplicationTransport, Retention, SenderError,
    capture_stream::read_window,
    sender_storage,
    writer_file_record::{read_file_record, write_file_record},
    writer_scope::WriterScope,
};
use sqlx_core::connection::Connection;

impl<S, A, B, T> ReplicationSender<S, A, B, T>
where
    S: ReplicatedSchema,
    A: PieceAnnotation,
    B: CaptureBookkeeping,
    T: ReplicationTransport<A::Protocol>,
{
    pub(crate) async fn enforce_retention(&self) -> Result<Vec<ReplicationEvents>, SenderError> {
        let mut control = self.database.control.lock().await;
        let mut connection = self.database.maintenance.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        crate::writer_authority::restore_authority(&mut transaction, &mut control.holder).await?;
        let mut record = read_file_record(&mut transaction)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        if matches!(
            control.holder.writer_state(self.database.clock.now()),
            Ok(fleet_sync_lease::WriterState::Superseded { .. })
        ) && record.epoch_ended_at.is_none()
            && sender_storage::rejected_tails(&mut transaction)
                .await?
                .is_empty()
        {
            // A replayed renewal can reveal the newer lease before a piece
            // refusal supplies this file's ended position. Keep its capture
            // intact until the authoritative probe can identify the tail.
            transaction.commit().await?;
            return Ok(Vec::new());
        }
        let mut copies = sender_storage::copies(&mut transaction, &record).await?;
        if copies.is_empty() {
            transaction.commit().await?;
            return Ok(Vec::new());
        }
        let head = crate::capture_stream::capture_head(&mut transaction, &record).await?;
        let mut events = Vec::new();
        for copy in &mut copies {
            if copy.role == fleet_sync_protocol::CopyRole::Authoritative {
                if copy.needs_snapshot || copy.acked < record.retained_after {
                    return Err(SenderError::Invalid {
                        field: "authoritative.capture_floor",
                    });
                }
                continue;
            }
            if copy.needs_snapshot {
                continue;
            }
            let outside_floor = copy.acked < record.retained_after;
            let outside_policy = if let Retention::Bounded {
                max_age,
                max_changes,
            } = self.options.retention
            {
                let changes = if copy.acked.epoch == head.epoch {
                    head.sequence
                        .value()
                        .saturating_sub(copy.acked.sequence.value())
                } else {
                    head.sequence.value()
                };
                let oldest = if outside_floor {
                    None
                } else {
                    read_window(&mut transaction, &record, copy.acked, 1)
                        .await?
                        .into_iter()
                        .next()
                };
                changes > max_changes
                    || oldest.is_some_and(|commit| {
                        commit.changed_at.after(max_age) <= self.database.clock.now()
                    })
            } else {
                false
            };
            if outside_floor || outside_policy {
                copy.needs_snapshot = true;
                sender_storage::save_copy(&mut transaction, copy).await?;
                events.push(ReplicationEvents::CopyNeedsSnapshot { copy: copy.copy });
            }
        }
        let mut floor = copies
            .iter()
            .filter(|copy| !copy.needs_snapshot)
            .map(|copy| copy.acked)
            .min()
            .unwrap_or(head);
        // Rejected capture remains readable until its consumer acknowledges the
        // rejection, even when bounded retention releases every normal copy.
        for tail in sender_storage::rejected_tails(&mut transaction).await? {
            if tail.database != record.database {
                return Err(OpenError::corrupt("rejected_tail.database").into());
            }
            floor = floor.min(tail.ended_at);
        }
        if floor > record.retained_after {
            let commits = read_window(&mut transaction, &record, record.retained_after, 64).await?;
            let mut changed = false;
            for commit in commits
                .into_iter()
                .take_while(|commit| commit.position <= floor)
            {
                self.record_capture(
                    &mut transaction,
                    &commit,
                    control.statements.layouts().to_vec(),
                )
                .await?;
                record.retained_after = commit.position;
                record.retained_change_id = commit.position.sequence.value();
                record.retained_layout = commit.layout_after;
                changed = true;
            }
            if changed {
                crate::checked_queries::prune_capture(
                    &mut WriterScope(&mut transaction),
                    i64::try_from(record.retained_change_id)
                        .map_err(|_| OpenError::corrupt("capture.floor"))?,
                )
                .await?;
                write_file_record(&mut transaction, &record).await?;
            }
        }
        transaction.commit().await?;
        Ok(events)
    }
}
