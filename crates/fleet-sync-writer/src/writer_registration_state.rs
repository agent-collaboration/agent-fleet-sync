use crate::{
    OpenError,
    writer_file_record::{read_file_record, write_file_record},
};
use fleet_sync_lease::{HolderError, LeaseHolder, WriterState};
use fleet_sync_protocol::{LeaseStatus, RenewAnswer, RenewResult, ReplicationPosition, SyncTime};
use serde::{Deserialize, Serialize};
use sqlx_turso::TursoConnection;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum RegistrationState {
    Unregistered,
    Seeding {
        position: ReplicationPosition,
    },
    Granted {
        grant: RegistrationGrant,
        seed: Option<RegistrationSeed>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RegistrationGrant {
    pub lease: LeaseStatus,
    pub request_started: SyncTime,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum RegistrationSeed {
    Seeding { position: ReplicationPosition },
    Ready { position: ReplicationPosition },
}
impl RegistrationState {
    pub(crate) fn validate(
        &self,
        database: fleet_sync_protocol::DatabaseId,
        peer: fleet_sync_protocol::PeerId,
        epoch: fleet_sync_protocol::Epoch,
        start: ReplicationPosition,
    ) -> bool {
        let position = match self {
            Self::Unregistered => return true,
            Self::Seeding { position } => Some(*position),
            Self::Granted { grant, seed } => {
                if grant.lease.database != database
                    || grant.lease.holder != peer
                    || grant.lease.epoch > epoch
                {
                    return false;
                }
                seed.as_ref().map(|seed| match seed {
                    RegistrationSeed::Seeding { position }
                    | RegistrationSeed::Ready { position } => *position,
                })
            }
        };
        position.is_none_or(|position| position >= start && position.epoch <= epoch)
    }
    pub(crate) fn restore(&self, holder: &mut LeaseHolder) -> Result<(), OpenError> {
        let (seed, ready) = match self {
            Self::Unregistered => return Ok(()),
            Self::Seeding { .. } => (true, false),
            Self::Granted { seed, .. } => (
                seed.is_some(),
                matches!(seed, Some(RegistrationSeed::Ready { .. })),
            ),
        };
        if seed {
            match holder.writer_state(SyncTime::from_unix_millis(0)) {
                Ok(WriterState::Provisional | WriterState::Seeding { .. }) => {
                    holder.begin_seeding().map_err(holder_error)?
                }
                Ok(WriterState::Writing { .. } | WriterState::Paused { .. }) if !ready => {
                    return Err(holder_error(HolderError::InvalidTransition));
                }
                _ => {}
            }
        }
        if let Self::Granted { grant, .. } = self {
            // This immutable registration receipt establishes the initial
            // grant. restore_authority applies the latest renewal after it.
            holder
                .observe_renewal(
                    RenewAnswer::Renewed {
                        recorded: RenewResult {
                            epoch: grant.lease.epoch,
                            expires_at: grant.lease.expires_at,
                        },
                        current: grant.lease.clone(),
                        transfer_in_progress: None,
                        replayed: true,
                    },
                    grant.request_started,
                )
                .map_err(holder_error)?;
        }
        Ok(())
    }
    pub(crate) fn restore_ready(&self, holder: &mut LeaseHolder) -> Result<(), OpenError> {
        if let Self::Granted {
            grant,
            seed: Some(RegistrationSeed::Ready { .. }),
        } = self
            && matches!(holder.writer_state(SyncTime::from_unix_millis(0)), Ok(WriterState::Seeding { epoch }) if epoch == grant.lease.epoch)
        {
            holder
                .complete_seed(grant.lease.epoch)
                .map_err(holder_error)?;
        }
        Ok(())
    }
}
pub(crate) fn holder_error(error: HolderError) -> OpenError {
    OpenError::Holder { error }
}

pub(crate) async fn begin_seed(
    connection: &mut TursoConnection,
    holder: &mut LeaseHolder,
) -> Result<ReplicationPosition, OpenError> {
    let mut record = read_file_record(connection)
        .await?
        .ok_or_else(|| OpenError::corrupt("writer.record"))?;
    let existing = match &record.registration {
        RegistrationState::Unregistered => {
            if !matches!(
                holder.writer_state(SyncTime::from_unix_millis(0)),
                Ok(WriterState::Provisional)
            ) {
                return Err(holder_error(HolderError::InvalidTransition));
            }
            None
        }
        RegistrationState::Seeding { position }
        | RegistrationState::Granted {
            seed: Some(RegistrationSeed::Seeding { position }),
            ..
        } => Some(*position),
        RegistrationState::Granted { .. } => return Err(holder_error(HolderError::AlreadyGranted)),
    };
    holder.begin_seeding().map_err(holder_error)?;
    let position = crate::capture_stream::capture_head(connection, &record).await?;
    if existing.is_some_and(|expected| expected != position) {
        return Err(OpenError::corrupt("registration.seed_position_moved"));
    }
    if existing.is_none() {
        record.registration = RegistrationState::Seeding { position };
        write_file_record(connection, &record).await?;
    }
    Ok(position)
}
pub(crate) async fn record_grant(
    connection: &mut TursoConnection,
    holder: &mut LeaseHolder,
    lease: LeaseStatus,
    started: SyncTime,
) -> Result<(), OpenError> {
    if lease.database != holder.database() || lease.holder != holder.peer() {
        return Err(holder_error(HolderError::LeaseMismatch));
    }
    let mut record = read_file_record(connection)
        .await?
        .ok_or_else(|| OpenError::corrupt("writer.record"))?;
    let seed = match record.registration {
        RegistrationState::Unregistered => {
            if !matches!(
                holder.writer_state(SyncTime::from_unix_millis(0)),
                Ok(WriterState::Provisional)
            ) {
                return Err(holder_error(HolderError::InvalidTransition));
            }
            None
        }
        RegistrationState::Seeding { position } => Some(RegistrationSeed::Seeding { position }),
        RegistrationState::Granted { .. } => return Err(holder_error(HolderError::AlreadyGranted)),
    };
    if lease.epoch != record.epoch {
        return Err(holder_error(HolderError::LeaseMismatch));
    }
    holder
        .observe_registration(lease.clone(), started)
        .map_err(holder_error)?;
    record.registration = RegistrationState::Granted {
        grant: RegistrationGrant {
            lease,
            request_started: started,
        },
        seed,
    };
    write_file_record(connection, &record).await
}
pub(crate) async fn ready_seed(
    connection: &mut TursoConnection,
    holder: &mut LeaseHolder,
) -> Result<(), OpenError> {
    let mut record = read_file_record(connection)
        .await?
        .ok_or_else(|| OpenError::corrupt("writer.record"))?;
    let RegistrationState::Granted {
        grant,
        seed: Some(seed),
    } = &mut record.registration
    else {
        return Err(holder_error(HolderError::InvalidTransition));
    };
    if let RegistrationSeed::Seeding { position } = *seed {
        holder
            .complete_seed(grant.lease.epoch)
            .map_err(holder_error)?;
        *seed = RegistrationSeed::Ready { position };
        write_file_record(connection, &record).await?;
    }
    Ok(())
}
