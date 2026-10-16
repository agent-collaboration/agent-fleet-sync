use crate::{
    OpenError, RejectedTail, SenderError, checked_queries, schema_inspection::text,
    writer_file_record::WriterFileRecord, writer_scope::WriterScope,
};
use fleet_sync_protocol::{CopyRole, PieceDigest, ReceiverId, ReplicationPosition};
use serde::{Deserialize, Serialize};
use sqlx_turso::TursoConnection;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SenderCopyStatus {
    pub copy: ReceiverId,
    pub role: CopyRole,
    pub start: ReplicationPosition,
    pub acked: ReplicationPosition,
    pub needs_snapshot: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CaptureAssignment {
    position: ReplicationPosition,
    digest: PieceDigest,
}

pub(crate) async fn copies(
    connection: &mut TursoConnection,
    writer: &WriterFileRecord,
) -> Result<Vec<SenderCopyStatus>, SenderError> {
    let mut copies = Vec::new();
    for row in checked_queries::read_copies(&mut WriterScope(connection)).await? {
        let id = text(row.copy_id, row.id_class, "copy.id")?;
        let copy: SenderCopyStatus =
            serde_json::from_str(&text(row.record, row.record_class, "copy.record")?)
                .map_err(|_| OpenError::corrupt("copy.record"))?;
        if copy.copy.as_uuid().to_string() != id
            || copy.start > copy.acked
            || copy.acked.epoch > writer.epoch
        {
            return Err(OpenError::corrupt("copy.positions").into());
        }
        copies.push(copy);
    }
    Ok(copies)
}

pub(crate) async fn save_copy(
    connection: &mut TursoConnection,
    copy: &SenderCopyStatus,
) -> Result<(), SenderError> {
    let record = serde_json::to_string(copy).map_err(|_| OpenError::corrupt("copy.record"))?;
    checked_queries::write_copy(
        &mut WriterScope(connection),
        copy.copy.as_uuid().to_string(),
        record,
    )
    .await?;
    Ok(())
}

pub(crate) async fn assigned(
    connection: &mut TursoConnection,
    position: ReplicationPosition,
    digest: &PieceDigest,
) -> Result<bool, SenderError> {
    let key = format!("{}:{}", position.epoch.value(), position.sequence.value());
    let Some(row) =
        checked_queries::read_capture_assignment(&mut WriterScope(connection), key).await?
    else {
        return Ok(false);
    };
    let record: CaptureAssignment =
        serde_json::from_str(&text(row.record, row.record_class, "assignment.record")?)
            .map_err(|_| OpenError::corrupt("assignment.record"))?;
    if record.position != position || record.digest != *digest {
        return Err(OpenError::corrupt("assignment.content").into());
    }
    Ok(true)
}

pub(crate) async fn assign(
    connection: &mut TursoConnection,
    position: ReplicationPosition,
    digest: PieceDigest,
) -> Result<(), SenderError> {
    let key = format!("{}:{}", position.epoch.value(), position.sequence.value());
    let record = serde_json::to_string(&CaptureAssignment { position, digest })
        .map_err(|_| OpenError::corrupt("assignment.record"))?;
    checked_queries::write_capture_assignment(&mut WriterScope(connection), key, record).await?;
    Ok(())
}

pub(crate) async fn capture_head(
    connection: &mut TursoConnection,
    record: &WriterFileRecord,
) -> Result<ReplicationPosition, SenderError> {
    let head = checked_queries::read_capture_head(&mut WriterScope(connection)).await?;
    let marker = match (head.marker, head.marker_class.as_deref()) {
        (None, Some("null")) => 0,
        (Some(value), Some("integer")) => value
            .parse::<u64>()
            .map_err(|_| OpenError::corrupt("capture.head"))?,
        _ => return Err(OpenError::corrupt("capture.head").into()),
    };
    if marker < record.retained_change_id {
        return Err(OpenError::corrupt("capture.head").into());
    }
    Ok(if marker == 0 {
        record.start
    } else {
        ReplicationPosition::new(
            record.epoch,
            fleet_sync_protocol::CommitSequence::new(marker),
        )
    })
}

pub(crate) async fn rejected_tails(
    connection: &mut TursoConnection,
) -> Result<Vec<RejectedTail>, SenderError> {
    let mut tails = Vec::new();
    for row in checked_queries::read_rejected_tails(&mut WriterScope(connection)).await? {
        let epoch = text(row.epoch, row.epoch_class, "rejected_tail.epoch")?;
        let tail: RejectedTail =
            serde_json::from_str(&text(row.record, row.record_class, "rejected_tail.record")?)
                .map_err(|_| OpenError::corrupt("rejected_tail.record"))?;
        if tail.epoch.value().to_string() != epoch
            || tail.ended_at >= tail.last_commit
            || tail.last_commit.epoch != tail.epoch
            || tail.ended_at.epoch > tail.epoch
        {
            return Err(OpenError::corrupt("rejected_tail.positions").into());
        }
        tails.push(tail);
    }
    Ok(tails)
}

pub(crate) async fn save_rejected_tail(
    connection: &mut TursoConnection,
    tail: &RejectedTail,
) -> Result<(), SenderError> {
    if let Some(existing) = rejected_tails(connection)
        .await?
        .iter()
        .find(|existing| existing.epoch == tail.epoch)
        && existing != tail
    {
        return Err(OpenError::corrupt("rejected_tail.conflict").into());
    }
    checked_queries::write_rejected_tail(
        &mut WriterScope(connection),
        tail.epoch.value().to_string(),
        serde_json::to_string(tail).map_err(|_| OpenError::corrupt("rejected_tail.record"))?,
    )
    .await?;
    Ok(())
}
