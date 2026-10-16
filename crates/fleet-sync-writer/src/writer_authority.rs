use crate::{
    OpenError, ReplicatedDatabase, ReplicatedSchema,
    writer_file_record::{read_file_record, write_file_record},
    writer_release_record::restore_releases,
};
use fleet_sync_lease::LeaseHolder;
use fleet_sync_protocol::{Epoch, GateReason, LeaseRefusal, RenewAnswer, SyncTime};
use serde::{Deserialize, Serialize};
use sqlx_core::connection::Connection;
use sqlx_turso::TursoConnection;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum AuthorityObservation {
    Renewal {
        answer: RenewAnswer,
        request_started: SyncTime,
    },
    RegistrationRejected {
        reason: GateReason,
    },
}
impl AuthorityObservation {
    fn epoch(&self) -> Option<Epoch> {
        match self {
            Self::Renewal {
                answer: RenewAnswer::Renewed { current, .. },
                ..
            } => Some(current.epoch),
            Self::Renewal {
                answer: RenewAnswer::Refused(LeaseRefusal::StaleEpoch { current_epoch, .. }),
                ..
            } => Some(*current_epoch),
            Self::Renewal {
                answer: RenewAnswer::Refused(LeaseRefusal::NotWriter { current }),
                ..
            } => *current,
            _ => None,
        }
    }
    fn terminal(&self) -> bool {
        matches!(
            self,
            Self::RegistrationRejected { .. }
                | Self::Renewal {
                    answer: RenewAnswer::Refused(LeaseRefusal::Retired),
                    ..
                }
        )
    }
    pub(crate) fn restore(
        &self,
        holder: &mut LeaseHolder,
        file_epoch: Epoch,
    ) -> Result<(), OpenError> {
        if let Self::Renewal {
            answer: RenewAnswer::Renewed { current, .. },
            ..
        } = self
            && current.database != holder.database()
        {
            return Err(OpenError::Holder {
                error: fleet_sync_lease::HolderError::LeaseMismatch,
            });
        }
        if let Some(current) = self.epoch()
            && current > file_epoch
        {
            holder.observe_supersession(current);
            return Ok(());
        }
        match self {
            Self::Renewal {
                answer,
                request_started,
            } => holder.observe_renewal(answer.clone(), *request_started),
            Self::RegistrationRejected { reason } => holder.reject_registration(reason.clone()),
        }
        .map_err(|error| OpenError::Holder { error })
    }
}

pub(crate) async fn save_observation(
    connection: &mut TursoConnection,
    observation: AuthorityObservation,
) -> Result<AuthorityObservation, OpenError> {
    let mut record = read_file_record(connection)
        .await?
        .ok_or_else(|| OpenError::corrupt("writer.record"))?;
    let observation = match record.authority {
        Some(existing)
            if existing.terminal()
                || (!observation.terminal()
                    && existing.epoch().is_some_and(|epoch| {
                        observation.epoch().is_none_or(|new| new < epoch)
                    })) =>
        {
            existing
        }
        _ => observation,
    };
    record.authority = Some(observation.clone());
    write_file_record(connection, &record).await?;
    Ok(observation)
}

pub(crate) async fn restore_authority(
    connection: &mut TursoConnection,
    holder: &mut LeaseHolder,
) -> Result<(), OpenError> {
    let record = read_file_record(connection)
        .await?
        .ok_or_else(|| OpenError::corrupt("writer.record"))?;
    if let Some(observation) = record.authority {
        observation.restore(holder, record.epoch)?;
    }
    restore_releases(connection, holder).await
}

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    /// Save the registry's current answer with the original request start time.
    /// Restoring this observation never grants time spent in transit or offline.
    pub async fn observe_renewal(
        &self,
        answer: RenewAnswer,
        request_started: SyncTime,
    ) -> Result<(), OpenError> {
        self.record_authority(AuthorityObservation::Renewal {
            answer,
            request_started,
        })
        .await
    }

    pub async fn reject_registration(&self, reason: GateReason) -> Result<(), OpenError> {
        self.record_authority(AuthorityObservation::RegistrationRejected { reason })
            .await
    }

    async fn record_authority(&self, observation: AuthorityObservation) -> Result<(), OpenError> {
        let mut control = self.control.lock().await;
        let mut connection = self.maintenance.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        restore_authority(&mut transaction, &mut control.holder).await?;
        let mut holder = control.holder.clone();
        let file_epoch = read_file_record(&mut transaction)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?
            .epoch;
        observation.restore(&mut holder, file_epoch)?;
        let observation = save_observation(&mut transaction, observation).await?;
        observation.restore(&mut holder, file_epoch)?;
        transaction.commit().await?;
        control.holder = holder;
        Ok(())
    }
}
