use crate::{
    OpenError, RegisterDatabaseInput, WriterJournalScope, WriterRegistrationOutcome,
    writer_authority::{AuthorityObservation, save_observation},
    writer_file_record::{WriterFileOrigin, WriterFileRecord, read_file_record, write_file_record},
    writer_registration_state::{RegistrationSeed, RegistrationState},
};
use durable_steps::{Host, OperationFailure, RequestKey, StartRefusal, Uuid};
use fleet_sync_protocol::*;

pub(crate) fn outcome(record: &WriterFileRecord) -> WriterRegistrationOutcome {
    if let RegistrationState::Granted {
        grant,
        seed: None | Some(RegistrationSeed::Ready { .. }),
    } = &record.registration
    {
        return WriterRegistrationOutcome::Ready {
            lease: grant.lease.clone(),
        };
    }
    let refusal = match &record.authority {
        Some(AuthorityObservation::RegistrationRejected { reason }) => {
            Some(RegistrationRefusal::DatabaseRejected {
                reason: reason.clone(),
            })
        }
        Some(AuthorityObservation::Renewal {
            answer: RenewAnswer::Refused(LeaseRefusal::NotWriter { current }),
            ..
        }) => Some(RegistrationRefusal::NotWriter { current: *current }),
        Some(AuthorityObservation::Renewal {
            answer: RenewAnswer::Refused(LeaseRefusal::Retired),
            ..
        }) => Some(RegistrationRefusal::Retired),
        _ => None,
    };
    if let Some(reason) = refusal {
        return WriterRegistrationOutcome::Refused { reason };
    }
    match &record.registration_failure {
        Some(reason) => WriterRegistrationOutcome::Failed {
            reason: reason.clone(),
        },
        None => WriterRegistrationOutcome::Pending,
    }
}
impl<H: Host> WriterJournalScope<'_, H> {
    pub async fn registration_outcome(&mut self) -> Result<WriterRegistrationOutcome, OpenError> {
        let record = read_file_record(self.maintenance.connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        Ok(outcome(&record))
    }
    pub(crate) async fn registration_input(
        &mut self,
        receiver: ReceiverId,
    ) -> Result<RegisterDatabaseInput, OpenError> {
        let record = read_file_record(self.maintenance.connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        if record.origin == WriterFileOrigin::Bootstrap {
            return Err(OpenError::Holder {
                error: fleet_sync_lease::HolderError::AlreadyGranted,
            });
        }
        if record.registration_attempt.is_none()
            && !matches!(
                self.holder.writer_state(self.host.now()),
                Ok(fleet_sync_lease::WriterState::Provisional
                    | fleet_sync_lease::WriterState::Seeding { .. })
            )
        {
            return Err(OpenError::Holder {
                error: fleet_sync_lease::HolderError::AlreadyGranted,
            });
        }
        let origin = match record.origin {
            WriterFileOrigin::Created => Origin::Empty,
            WriterFileOrigin::Adopted => {
                let position = match &record.registration {
                    RegistrationState::Seeding { position }
                    | RegistrationState::Granted {
                        seed:
                            Some(
                                RegistrationSeed::Seeding { position }
                                | RegistrationSeed::Ready { position },
                            ),
                        ..
                    } => *position,
                    _ => self.begin_seeding().await?,
                };
                Origin::Seeded { position }
            }
            WriterFileOrigin::Bootstrap => {
                return Err(OpenError::corrupt("registration.bootstrap_origin"));
            }
        };
        let mut record = read_file_record(self.maintenance.connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        let attempt = match record.registration_attempt {
            Some(attempt) => attempt,
            None => {
                let attempt = RequestId::new(self.host.new_id())
                    .map_err(|_| OpenError::corrupt("registration.attempt"))?;
                record.registration_attempt = Some(attempt);
                write_file_record(self.maintenance.connection, &record).await?;
                attempt
            }
        };
        Ok(RegisterDatabaseInput {
            request: Registration {
                attempt,
                database: record.database,
                writer: record.peer,
                origin,
            },
            receiver,
        })
    }
    pub(crate) async fn record_registration_answer(
        &mut self,
        answer: RegistrationAnswer,
        started: SyncTime,
    ) -> Result<(), OpenError> {
        match answer {
            RegistrationAnswer::Registered { lease, .. } => {
                self.observe_registration(lease, started).await
            }
            RegistrationAnswer::Refused(refusal) => {
                let observation = match refusal {
                    RegistrationRefusal::DatabaseRejected { reason } => {
                        AuthorityObservation::RegistrationRejected { reason }
                    }
                    RegistrationRefusal::NotWriter { current } => AuthorityObservation::Renewal {
                        answer: RenewAnswer::Refused(LeaseRefusal::NotWriter { current }),
                        request_started: started,
                    },
                    RegistrationRefusal::Retired => AuthorityObservation::Renewal {
                        answer: RenewAnswer::Refused(LeaseRefusal::Retired),
                        request_started: started,
                    },
                };
                let epoch = self.holder_epoch().await?;
                observation.restore(&mut self.holder, epoch)?;
                save_observation(self.maintenance.connection, observation).await?;
                Ok(())
            }
        }
    }
    pub(crate) async fn fail_registration(
        &mut self,
        reason: &OperationFailure,
    ) -> Result<(), OpenError> {
        let mut record = read_file_record(self.maintenance.connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        if matches!(outcome(&record), WriterRegistrationOutcome::Pending) {
            record.registration_failure = Some(reason.clone());
            write_file_record(self.maintenance.connection, &record).await?;
        }
        Ok(())
    }
    pub(crate) async fn register_sender_copy(
        &mut self,
        receiver: ReceiverId,
        position: ReplicationPosition,
    ) -> Result<(), OpenError> {
        let record = read_file_record(self.maintenance.connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?;
        let copies = crate::sender_storage::copies(self.maintenance.connection, &record)
            .await
            .map_err(|error| OpenError::Storage {
                message: error.to_string(),
            })?;
        if let Some(existing) = copies.iter().find(|copy| copy.copy == receiver) {
            if existing.role != CopyRole::Authoritative || existing.start != position {
                return Err(OpenError::corrupt("registration.copy_conflict"));
            }
        } else {
            crate::sender_storage::save_copy(
                self.maintenance.connection,
                &crate::SenderCopyStatus {
                    copy: receiver,
                    role: CopyRole::Authoritative,
                    start: position,
                    acked: position,
                    needs_snapshot: false,
                },
            )
            .await
            .map_err(|error| OpenError::Storage {
                message: error.to_string(),
            })?;
        }
        Ok(())
    }
    pub(crate) async fn holder_epoch(&mut self) -> Result<Epoch, OpenError> {
        Ok(read_file_record(self.maintenance.connection)
            .await?
            .ok_or_else(|| OpenError::corrupt("writer.record"))?
            .epoch)
    }
}
pub(crate) fn registration_key() -> Result<RequestKey, StartRefusal> {
    RequestKey::new("writer-registration")
        .map_err(durable_steps::StoreError::backend)
        .map_err(Into::into)
}
pub(crate) fn renewal_key(id: Uuid) -> Result<RequestKey, durable_steps::StoreError> {
    RequestKey::new(format!("writer-renewal:{id}")).map_err(durable_steps::StoreError::backend)
}
