use crate::{OpenError, schema_inspection::text, writer_scope::WriterScope};
use durable_steps::CallId;
use fleet_sync_lease::LeaseHolder;
use fleet_sync_protocol::{DatabaseId, ReleaseAnswer, ReleaseRequest, WithdrawAnswer};
use serde::{Deserialize, Serialize};
use sqlx_turso::TursoConnection;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WriterReleaseRecord {
    pub request: ReleaseRequest,
    pub call: CallId,
    pub state: ReleaseState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum ReleaseState {
    Draining,
    Answered { answer: ReleaseAnswer },
    WithdrawalAnswered { answer: WithdrawAnswer },
}

impl WriterReleaseRecord {
    pub(crate) fn restore(&self, holder: &mut LeaseHolder) -> Result<(), OpenError> {
        let final_position = match self.state {
            ReleaseState::Draining => None,
            ReleaseState::Answered {
                answer: ReleaseAnswer::Released { final_position },
            } => Some(final_position),
            ReleaseState::Answered { .. } | ReleaseState::WithdrawalAnswered { .. } => {
                return Ok(());
            }
        };
        holder
            .restore_release(self.request.epoch, self.request.transfer, final_position)
            .map_err(|_| OpenError::corrupt("release.authority"))
    }
    pub(crate) fn withdraws(&self, request: &ReleaseRequest) -> bool {
        self.request == *request
            && matches!(
                self.state,
                ReleaseState::WithdrawalAnswered {
                    answer: WithdrawAnswer::Withdrawn
                }
            )
    }
    fn order(&self) -> (fleet_sync_protocol::Epoch, bool) {
        (
            self.request.epoch,
            matches!(
                self.state,
                ReleaseState::Answered {
                    answer: ReleaseAnswer::Released { .. }
                }
            ),
        )
    }
}

pub(crate) async fn read_releases(
    connection: &mut TursoConnection,
    database: DatabaseId,
) -> Result<Vec<WriterReleaseRecord>, OpenError> {
    let mut records = Vec::new();
    for row in crate::checked_queries::read_releases(&mut WriterScope(connection)).await? {
        let stored_database = text(row.database_id, row.database_class, "release.database")?;
        let call = text(row.call_id, row.call_class, "release.call")?;
        let record: WriterReleaseRecord =
            serde_json::from_str(&text(row.record, row.record_class, "release.record")?)
                .map_err(|_| OpenError::corrupt("release.record"))?;
        if record.request.database != database
            || database.as_uuid().to_string() != stored_database
            || serde_json::to_string(&record.call)
                .map_err(|_| OpenError::corrupt("release.call"))?
                != call
            || matches!(&record.state, ReleaseState::Answered {answer:ReleaseAnswer::Released {final_position}} if final_position.epoch != record.request.epoch)
        {
            return Err(OpenError::corrupt("release.identity"));
        }
        records.push(record);
    }
    records.sort_by_key(WriterReleaseRecord::order);
    Ok(records)
}

pub(crate) async fn restore_releases(
    connection: &mut TursoConnection,
    holder: &mut LeaseHolder,
) -> Result<(), OpenError> {
    let records = read_releases(connection, holder.database()).await?;
    for record in &records {
        if !records
            .iter()
            .any(|withdrawal| withdrawal.withdraws(&record.request))
        {
            record.restore(holder)?;
        }
    }
    // Tombstones outlive the release calls they dominate. Apply them after
    // other transfers so an old withdrawal cannot reopen a newer drain.
    for record in &records {
        if record.withdraws(&record.request) {
            holder.withdraw_release(record.request.epoch, record.request.transfer);
        }
    }
    Ok(())
}

pub(crate) async fn save_release(
    connection: &mut TursoConnection,
    record: &WriterReleaseRecord,
) -> Result<(), OpenError> {
    crate::checked_queries::write_release(
        &mut WriterScope(connection),
        record.request.database.as_uuid().to_string(),
        serde_json::to_string(&record.call).map_err(|_| OpenError::corrupt("release.call"))?,
        serde_json::to_string(record).map_err(|_| OpenError::corrupt("release.record"))?,
    )
    .await?;
    Ok(())
}
