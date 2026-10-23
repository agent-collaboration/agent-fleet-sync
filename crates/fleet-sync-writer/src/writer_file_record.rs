use crate::{
    OpenError,
    schema_inspection::{text, unsigned},
    writer_scope::WriterScope,
};
use fleet_sync_protocol::{CommitSequence, DatabaseId, Epoch, PeerId, ReplicationPosition};
use serde::{Deserialize, Serialize};
use sqlx_turso::TursoConnection;

pub(crate) static BOOKKEEPING_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
pub(crate) const BOOKKEEPING_HISTORY_TABLE: &str = "_fleet_sync_writer_engine_history";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WriterFileRecord {
    pub bootstrap: Option<fleet_sync_receiver::SnapshotInstallState>,
    pub origin: RetainedFileOriginKind,
    pub registration_attempt: Option<fleet_sync_protocol::RequestId>,
    pub registration_failure: Option<durable_steps::OperationFailure>,
    pub renewal_operation: Option<crate::renewal_task::RenewalOperation>,
    pub renewal_failure: Option<durable_steps::OperationFailure>,
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
/// Persisted provenance, shared by writer bookkeeping and retained inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RetainedFileOriginKind {
    Created,
    Adopted,
    Bootstrap,
}

impl WriterFileRecord {
    pub(crate) fn new(database: DatabaseId, peer: PeerId, epoch: Epoch) -> Self {
        let start = ReplicationPosition {
            epoch,
            sequence: CommitSequence::new(0),
        };
        Self {
            bootstrap: None,
            origin: RetainedFileOriginKind::Created,
            registration_attempt: None,
            registration_failure: None,
            renewal_operation: None,
            renewal_failure: None,
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
    if let Some(authority) = &record.authority {
        authority.validate_database(record.database)?;
        if let crate::writer_authority::AuthorityObservation::Renewal {
            answer:
                fleet_sync_protocol::RenewAnswer::Refused(
                    fleet_sync_protocol::LeaseRefusal::StaleEpoch { epoch_ended_at, .. },
                ),
            ..
        } = authority
            && record.epoch_ended_at != Some(*epoch_ended_at)
        {
            return Err(OpenError::corrupt("writer.authority_end"));
        }
    }
    if let Some(marker) = &record.bootstrap
        && (!marker.verified
            || marker.install.database != record.database
            || marker.install.position != record.start
            || marker.install.position.epoch >= record.epoch
            || marker.ready
                != matches!(
                    record.registration,
                    crate::writer_registration_state::RegistrationState::Granted {
                        seed: Some(
                            crate::writer_registration_state::RegistrationSeed::Ready { .. }
                        ),
                        ..
                    }
                ))
    {
        return Err(OpenError::corrupt("writer.bootstrap"));
    }
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
    let expected_cursor = if record.retained_after.epoch == record.epoch {
        record.retained_after.sequence.value()
    } else {
        0
    };
    if record.retained_change_id != expected_cursor
        || i64::try_from(record.retained_change_id).is_err()
    {
        return Err(OpenError::corrupt("writer.capture_cursor"));
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
