use crate::{
    CaptureBookkeeping, OpenError, PieceAnnotation, ReplicatedSchema, ReplicationSender,
    ReplicationTransport, SenderCopyStatus, SenderError,
    capture_stream::{capture_head, retained_commit_boundary},
    sender_storage,
    writer_file_record::read_file_record,
};
use fleet_sync_protocol::{CopyRole, ReceiverId, ReplicationPosition};
use sqlx_core::connection::Connection;
impl<S, A, B, T> ReplicationSender<S, A, B, T>
where
    S: ReplicatedSchema,
    A: PieceAnnotation,
    B: CaptureBookkeeping,
    T: ReplicationTransport<A::Protocol>,
{
    /// The host calls this after the same follower reports its installed
    /// position. A retained commit boundary can advance its acknowledgement and
    /// clear the snapshot flag atomically. Registration's original start stays
    /// fixed. An older repeat on an already-following copy never rewinds it.
    pub async fn complete_snapshot(
        &self,
        copy: ReceiverId,
        acknowledged: ReplicationPosition,
    ) -> Result<SenderCopyStatus, SenderError> {
        let lock = self.copy_lock(copy)?;
        let _copy = lock.lock().await;
        let _control = self.database.control.lock().await;
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
        if status.role != CopyRole::Follower {
            return Err(SenderError::Invalid {
                field: "copy.snapshot_role",
            });
        }
        if !status.needs_snapshot && acknowledged <= status.acked {
            transaction.commit().await?;
            return Ok(status);
        }
        if acknowledged < status.acked
            || acknowledged < record.retained_after
            || acknowledged > capture_head(&mut transaction, &record).await?
            || !retained_commit_boundary(&mut transaction, &record, acknowledged).await?
        {
            return Err(SenderError::Invalid {
                field: "copy.snapshot_position",
            });
        }
        status.acked = acknowledged;
        status.needs_snapshot = false;
        sender_storage::save_copy(&mut transaction, &status).await?;
        transaction.commit().await?;
        Ok(status)
    }
}
