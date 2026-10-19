use crate::{LeaseError, checked_queries, registry_records::*};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use serde::{Serialize, de::DeserializeOwned};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) fn encode<T: Serialize>(record: &T) -> Result<String, LeaseError> {
    serde_json::to_string(record).map_err(|_| LeaseError::Corrupt {
        field: "encoded registry record".into(),
    })
}
pub(crate) fn decode<T: DeserializeOwned>(
    record: Option<String>,
    field: &str,
) -> Result<T, LeaseError> {
    let record = record.ok_or_else(|| LeaseError::Corrupt {
        field: field.into(),
    })?;
    serde_json::from_str(&record).map_err(|_| LeaseError::Corrupt {
        field: field.into(),
    })
}
fn conflict(field: &str) -> LeaseError {
    LeaseError::InvalidInput {
        field: format!("{field} identity has different contents"),
    }
}
pub(crate) fn decode_history(
    database: DatabaseId,
    epoch: Epoch,
    record: Option<String>,
) -> Result<EpochHistory, LeaseError> {
    let record: EpochHistory = decode(record, "epoch history")?;
    if record.database != database
        || record.epoch != epoch
        || record.ended_at.is_some_and(|end| end.epoch > epoch)
    {
        return Err(LeaseError::Corrupt {
            field: "epoch history".into(),
        });
    }
    Ok(record)
}
pub(crate) struct RegistryStorage<'scope, S>(pub &'scope mut S);
impl<S> RegistryStorage<'_, S>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub(crate) async fn database_ids(&mut self) -> Result<Vec<DatabaseId>, LeaseError> {
        let mut databases = Vec::new();
        for row in checked_queries::read_databases(self.0).await? {
            let record: DatabaseRecord = decode(row.record, "database record")?;
            if row.database.as_deref() != Some(record.database.as_uuid().to_string().as_str()) {
                return Err(LeaseError::Corrupt {
                    field: "database record".into(),
                });
            }
            if !matches!(record.lifecycle, DatabaseLifecycle::Retired { .. }) {
                databases.push(record.database);
            }
        }
        Ok(databases)
    }
    pub(crate) async fn database(
        &mut self,
        database: DatabaseId,
    ) -> Result<Option<DatabaseRecord>, LeaseError> {
        let Some(row) =
            checked_queries::read_database(self.0, database.as_uuid().to_string()).await?
        else {
            return Ok(None);
        };
        let record: DatabaseRecord = decode(row.record, "database record")?;
        if record.database != database {
            return Err(LeaseError::Corrupt {
                field: "database record".into(),
            });
        }
        Ok(Some(record))
    }
    pub(crate) async fn save_database(
        &mut self,
        record: &DatabaseRecord,
    ) -> Result<(), LeaseError> {
        if let Some(existing) = self.database(record.database).await?
            && matches!(existing.lifecycle, DatabaseLifecycle::Retired { .. })
            && existing != *record
        {
            return Err(LeaseError::Retired);
        }
        checked_queries::write_database(
            self.0,
            record.database.as_uuid().to_string(),
            encode(record)?,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn lease(
        &mut self,
        database: DatabaseId,
    ) -> Result<Option<RegistryLease>, LeaseError> {
        let Some(row) = checked_queries::read_lease(self.0, database.as_uuid().to_string()).await?
        else {
            return Ok(None);
        };
        let record: RegistryLease = decode(row.record, "lease")?;
        if record.database != database || record.expires_at < record.last_renewed_at {
            return Err(LeaseError::Corrupt {
                field: "lease".into(),
            });
        }
        Ok(Some(record))
    }
    pub(crate) async fn save_lease(&mut self, record: &RegistryLease) -> Result<(), LeaseError> {
        if let Some(previous) = self.lease(record.database).await?
            && (previous.epoch > record.epoch
                || (previous.epoch == record.epoch && previous.holder != record.holder))
        {
            return Err(LeaseError::Corrupt {
                field: "lease epoch regression or holder conflict".into(),
            });
        }
        checked_queries::write_lease(
            self.0,
            record.database.as_uuid().to_string(),
            encode(record)?,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn floor(
        &mut self,
        database: DatabaseId,
    ) -> Result<Option<Epoch>, LeaseError> {
        checked_queries::read_floor(self.0, database.as_uuid().to_string())
            .await?
            .map(|row| decode(row.epoch, "epoch floor"))
            .transpose()
    }
    pub(crate) async fn save_floor(
        &mut self,
        database: DatabaseId,
        epoch: Epoch,
    ) -> Result<(), LeaseError> {
        if self
            .floor(database)
            .await?
            .is_some_and(|previous| previous > epoch)
        {
            return Err(LeaseError::Corrupt {
                field: "epoch floor regression".into(),
            });
        }
        checked_queries::write_floor(
            self.0,
            database.as_uuid().to_string(),
            epoch.value().to_string(),
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn history(
        &mut self,
        database: DatabaseId,
        epoch: Epoch,
    ) -> Result<Option<EpochHistory>, LeaseError> {
        let Some(row) = checked_queries::read_history(
            self.0,
            database.as_uuid().to_string(),
            epoch.value().to_string(),
        )
        .await?
        else {
            return Ok(None);
        };
        decode_history(database, epoch, row.record).map(Some)
    }
    pub(crate) async fn save_history(&mut self, record: &EpochHistory) -> Result<(), LeaseError> {
        if let Some(existing) = self.history(record.database, record.epoch).await?
            && (existing.holder != record.holder
                || existing.granted_at != record.granted_at
                || existing
                    .ended_at
                    .is_some_and(|ended| Some(ended) != record.ended_at))
        {
            return Err(LeaseError::Corrupt {
                field: "immutable epoch history".into(),
            });
        }
        checked_queries::write_history(
            self.0,
            record.database.as_uuid().to_string(),
            record.epoch.value().to_string(),
            encode(record)?,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn renewal(
        &mut self,
        database: DatabaseId,
        attempt: RequestId,
    ) -> Result<Option<RenewalAttempt>, LeaseError> {
        let Some(row) = checked_queries::read_renewal(
            self.0,
            database.as_uuid().to_string(),
            attempt.as_uuid().to_string(),
        )
        .await?
        else {
            return Ok(None);
        };
        let record: RenewalAttempt = decode(row.record, "renewal attempt")?;
        if record.request.database != database || record.request.attempt != attempt {
            return Err(LeaseError::Corrupt {
                field: "renewal attempt".into(),
            });
        }
        Ok(Some(record))
    }
    pub(crate) async fn save_renewal(&mut self, record: &RenewalAttempt) -> Result<(), LeaseError> {
        if let Some(existing) = self
            .renewal(record.request.database, record.request.attempt)
            .await?
            && existing != *record
        {
            return Err(conflict("renewal attempt"));
        }
        checked_queries::write_renewal(
            self.0,
            record.request.database.as_uuid().to_string(),
            record.request.attempt.as_uuid().to_string(),
            encode(record)?,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn lifecycle(
        &mut self,
        attempt: RequestId,
    ) -> Result<Option<LifecycleAttempt>, LeaseError> {
        let Some(row) =
            checked_queries::read_lifecycle(self.0, attempt.as_uuid().to_string()).await?
        else {
            return Ok(None);
        };
        let intent: LifecycleIntent = decode(row.intent, "lifecycle intent")?;
        if intent.attempt() != attempt {
            return Err(LeaseError::Corrupt {
                field: "lifecycle attempt".into(),
            });
        }
        Ok(Some(LifecycleAttempt {
            intent,
            outcome: row
                .outcome
                .map(|value| decode(Some(value), "lifecycle outcome"))
                .transpose()?,
        }))
    }
    pub(crate) async fn active_lifecycle(
        &mut self,
        database: DatabaseId,
    ) -> Result<Option<LifecycleAttempt>, LeaseError> {
        let Some(row) =
            checked_queries::active_lifecycle(self.0, database.as_uuid().to_string()).await?
        else {
            return Ok(None);
        };
        let intent: LifecycleIntent = decode(row.intent, "active lifecycle intent")?;
        if intent.database() != database || row.outcome.is_some() {
            return Err(LeaseError::Corrupt {
                field: "active lifecycle attempt".into(),
            });
        }
        Ok(Some(LifecycleAttempt {
            intent,
            outcome: None,
        }))
    }
    pub(crate) async fn save_lifecycle(
        &mut self,
        record: &LifecycleAttempt,
    ) -> Result<(), LeaseError> {
        if let Some(existing) = self.lifecycle(record.intent.attempt()).await?
            && (existing.intent != record.intent
                || (existing.outcome.is_some() && existing.outcome != record.outcome))
        {
            return Err(conflict("lifecycle attempt"));
        }
        checked_queries::write_lifecycle(
            self.0,
            record.intent.attempt().as_uuid().to_string(),
            record.intent.database().as_uuid().to_string(),
            encode(&record.intent)?,
            record.outcome.as_ref().map(encode).transpose()?,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn preview(
        &mut self,
        token: &PreviewToken,
    ) -> Result<Option<PreviewRecord>, LeaseError> {
        checked_queries::read_preview(self.0, token.as_str().to_owned())
            .await?
            .map(|row| decode(row.record, "takeover preview"))
            .transpose()
    }
    pub(crate) async fn save_preview(&mut self, record: &PreviewRecord) -> Result<(), LeaseError> {
        if let Some(existing) = self.preview(&record.preview.token).await? {
            return if existing == *record {
                Ok(())
            } else {
                Err(conflict("takeover preview"))
            };
        }
        checked_queries::write_preview(
            self.0,
            record.preview.token.as_str().to_owned(),
            record.database.as_uuid().to_string(),
            encode(record)?,
        )
        .await?;
        Ok(())
    }
}
