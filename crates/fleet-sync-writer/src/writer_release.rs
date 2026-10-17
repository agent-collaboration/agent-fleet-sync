use crate::{
    OpenError, ReplicatedDatabase, ReplicatedSchema,
    capture_stream::capture_head,
    writer_file_record::read_file_record,
    writer_release_record::{ReleaseState, WriterReleaseRecord, read_releases, save_release},
};
use durable_steps::CallId;
use fleet_sync_lease::WriterState;
use fleet_sync_protocol::{MigrationState, ReleaseAnswer, ReleaseRefusal, ReleaseRequest};
use sqlx_core::connection::Connection;

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    /// Stop admission under the writer guard and save the final committed
    /// capture position. The registry waits for its copy to acknowledge that
    /// position before fencing; network work never holds this guard.
    pub async fn request_release(
        &self,
        request: &ReleaseRequest,
        call: CallId,
    ) -> Result<ReleaseAnswer, OpenError> {
        if request.database != self.database {
            return Err(OpenError::corrupt("release.database"));
        }
        let mut control = self.control.lock().await;
        let mut connection = self.maintenance.acquire().await?;
        let records = read_releases(&mut connection, self.database).await?;
        let existing = records.iter().find(|record| record.call == call);
        if existing.is_some_and(|record| {
            record.request != *request
                || matches!(record.state, ReleaseState::WithdrawalAnswered { .. })
        }) {
            return Ok(ReleaseAnswer::RequestConflict);
        }
        crate::writer_authority::restore_authority(&mut connection, &mut control.holder).await?;
        let file = read_file_record(&mut connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        let current = match control.holder.writer_state(self.clock.now()) {
            Ok(WriterState::Superseded { current }) => current,
            _ => file.epoch,
        };
        if current != request.epoch {
            return Ok(ReleaseAnswer::ReleaseRefused(ReleaseRefusal::Superseded {
                current,
            }));
        }
        if records.iter().any(|record| record.withdraws(request)) {
            let answer = ReleaseAnswer::ReleaseRefused(ReleaseRefusal::Withdrawn);
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            save_release(
                &mut transaction,
                &WriterReleaseRecord {
                    request: request.clone(),
                    call,
                    state: ReleaseState::Answered {
                        answer: answer.clone(),
                    },
                },
            )
            .await?;
            transaction.commit().await?;
            return Ok(answer);
        }
        if let Some(WriterReleaseRecord {
            state: ReleaseState::Answered { answer },
            ..
        }) = existing
        {
            return Ok(answer.clone());
        }
        let mut record = WriterReleaseRecord {
            request: request.clone(),
            call,
            state: ReleaseState::Draining,
        };
        if existing.is_none() {
            // A move cannot release a schema that this process has not finished
            // migrating. An unfinished captured migration never commits partly.
            let applied = crate::checked_queries::read_applied_migrations(
                &mut crate::writer_scope::WriterScope(&mut connection),
            )
            .await?;
            let versions = applied
                .into_iter()
                .map(|row| {
                    if row.version_class.as_deref() != Some("integer") {
                        return Err(OpenError::corrupt("migration.version"));
                    }
                    row.version
                        .and_then(|value| value.parse::<i64>().ok())
                        .ok_or_else(|| OpenError::corrupt("migration.version"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(migration) = self.schema.migrations().iter().find(|migration| {
                migration.migration_type.is_up_migration() && !versions.contains(&migration.version)
            }) {
                record.state = ReleaseState::Answered {
                    answer: ReleaseAnswer::ReleaseRefused(ReleaseRefusal::MigrationPending {
                        migration: u64::try_from(migration.version)
                            .map_err(|_| OpenError::corrupt("migration.version"))?,
                        state: MigrationState::Pending,
                    }),
                };
            }
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            save_release(&mut transaction, &record).await?;
            transaction.commit().await?;
            record.restore(&mut control.holder)?;
            if let ReleaseState::Answered { answer } = record.state {
                return Ok(answer);
            }
        }
        // The draining commit is recoverable independently from this one. On a
        // restart, admission reloads it before considering a cached lease.
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let final_position = capture_head(&mut transaction, &file).await?;
        let answer = ReleaseAnswer::Released { final_position };
        record.state = ReleaseState::Answered {
            answer: answer.clone(),
        };
        save_release(&mut transaction, &record).await?;
        transaction.commit().await?;
        record.restore(&mut control.holder)?;
        Ok(answer)
    }
}
