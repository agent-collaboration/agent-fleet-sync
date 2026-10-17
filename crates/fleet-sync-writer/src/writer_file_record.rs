use crate::{
    OpenError,
    schema_inspection::{text, unsigned},
    writer_scope::WriterScope,
};
use fleet_sync_protocol::{CommitSequence, DatabaseId, Epoch, PeerId, ReplicationPosition};
use serde::{Deserialize, Serialize};
use sqlx_turso::TursoConnection;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WriterFileRecord {
    pub database: DatabaseId,
    pub peer: PeerId,
    pub epoch: Epoch,
    pub start: ReplicationPosition,
    pub retained_after: ReplicationPosition,
    pub retained_change_id: u64,
    pub retained_layout: Vec<crate::TableLayout>,
    pub authority: Option<crate::writer_authority::AuthorityObservation>,
    pub epoch_ended_at: Option<ReplicationPosition>,
    pub registration: crate::writer_registration_state::RegistrationState,
}

impl WriterFileRecord {
    pub(crate) fn new(database: DatabaseId, peer: PeerId, epoch: Epoch) -> Self {
        let start = ReplicationPosition {
            epoch,
            sequence: CommitSequence::new(0),
        };
        Self {
            database,
            peer,
            epoch,
            start,
            retained_after: start,
            retained_change_id: 0,
            retained_layout: Vec::new(),
            authority: None,
            epoch_ended_at: None,
            registration: crate::writer_registration_state::RegistrationState::Unregistered,
        }
    }
}

pub(crate) async fn read_file_record(
    connection: &mut TursoConnection,
) -> Result<Option<WriterFileRecord>, OpenError> {
    let rows = crate::checked_queries::read_writer(&mut WriterScope(connection)).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() != 1 {
        return Err(OpenError::corrupt("writer.singleton"));
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| OpenError::corrupt("writer.singleton"))?;
    if unsigned(row.singleton, row.singleton_class, "writer.singleton")? != 1 {
        return Err(OpenError::corrupt("writer.singleton"));
    }
    let encoded = text(row.record, row.record_class, "writer.record")?;
    let record: WriterFileRecord =
        serde_json::from_str(&encoded).map_err(|_| OpenError::corrupt("writer.record"))?;
    if record.start.epoch > record.epoch
        || !record
            .registration
            .validate(record.database, record.peer, record.epoch, record.start)
        || record.retained_after < record.start
        || record.retained_after.epoch > record.epoch
        || record
            .epoch_ended_at
            .is_some_and(|end| end < record.start || end.epoch > record.epoch)
    {
        return Err(OpenError::corrupt("writer.positions"));
    }
    Ok(Some(record))
}

pub(crate) async fn write_file_record(
    connection: &mut TursoConnection,
    record: &WriterFileRecord,
) -> Result<(), OpenError> {
    let encoded = serde_json::to_string(record).map_err(|_| OpenError::corrupt("writer.record"))?;
    crate::checked_queries::write_writer(&mut WriterScope(connection), encoded).await?;
    Ok(())
}
