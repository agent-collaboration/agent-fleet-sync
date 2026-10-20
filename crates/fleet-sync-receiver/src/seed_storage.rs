use crate::{
    ReceiverError, StoreError, checked_queries,
    copy_storage::{decode_record, encode_record},
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::SeedAttemptId;
use fleet_sync_protocol::{DatabaseId, PageDigest, SeedComplete, SeedId, SeedPage};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeedRecord {
    pub database: DatabaseId,
    pub seed: SeedId,
    pub published: Option<SeedComplete>,
}

pub(crate) struct SeedStorage<'scope, S>(pub &'scope mut S);
impl<S> SeedStorage<'_, S>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub(crate) async fn load(
        &mut self,
        database: DatabaseId,
    ) -> Result<Option<SeedRecord>, StoreError> {
        let Some(row) = checked_queries::read_seed(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?
        else {
            return Ok(None);
        };
        let record: SeedRecord = decode_record(row.record, "seed record")?;
        if record.database != database {
            return Err(StoreError::Corrupt {
                field: "seed record database".into(),
            });
        }
        if record.seed.generation == 0
            || record
                .published
                .as_ref()
                .is_some_and(|done| done.database != record.database || done.seed != record.seed)
        {
            return Err(StoreError::Corrupt {
                field: "seed record".into(),
            });
        }
        Ok(Some(record))
    }
    pub(crate) async fn save(&mut self, record: &SeedRecord) -> Result<(), StoreError> {
        checked_queries::write_seed(self.0, encode_record(record)?)
            .await
            .map_err(crate::sql_transport::sql_error)
    }
    pub(crate) async fn attempt(
        &mut self,
        attempt: SeedAttemptId,
    ) -> Result<Option<SeedId>, StoreError> {
        checked_queries::read_seed_attempt(self.0, attempt.to_string())
            .await
            .map_err(crate::sql_transport::sql_error)?
            .map(|row| decode_record(row.record, "seed attempt"))
            .transpose()
    }
    pub(crate) async fn save_attempt(
        &mut self,
        attempt: SeedAttemptId,
        seed: SeedId,
    ) -> Result<(), StoreError> {
        checked_queries::write_seed_attempt(self.0, attempt.to_string(), encode_record(&seed)?)
            .await
            .map_err(crate::sql_transport::sql_error)
    }
    pub(crate) async fn page(&mut self, page: &SeedPage) -> Result<Option<SeedPage>, StoreError> {
        let Some(row) = checked_queries::read_seed_page(
            self.0,
            page.seed.generation.to_string(),
            page.page.to_string(),
        )
        .await
        .map_err(crate::sql_transport::sql_error)?
        else {
            return Ok(None);
        };
        Self::decode_page(row.payload, row.digest, page.seed, page.page).map(Some)
    }
    fn decode_page(
        payload: Option<String>,
        digest: Option<String>,
        seed: SeedId,
        index: u32,
    ) -> Result<SeedPage, StoreError> {
        let page: SeedPage = decode_record(payload, "seed page")?;
        if page.seed != seed
            || page.page != index
            || digest.as_deref() != Some(page.digest.as_str())
            || PageDigest::of(&page.contents).ok().as_ref() != Some(&page.digest)
        {
            return Err(StoreError::Corrupt {
                field: "seed page".into(),
            });
        }
        Ok(page)
    }
    pub(crate) async fn pages(&mut self, seed: SeedId) -> Result<Vec<SeedPage>, StoreError> {
        checked_queries::read_seed_pages(self.0, seed.generation.to_string())
            .await
            .map_err(crate::sql_transport::sql_error)?
            .into_iter()
            .map(|row| {
                let index = row
                    .page
                    .and_then(|value| value.parse::<u32>().ok())
                    .ok_or_else(|| StoreError::Corrupt {
                        field: "seed page index".into(),
                    })?;
                if row.page_class.as_deref() != Some("integer") {
                    return Err(StoreError::Corrupt {
                        field: "seed page index".into(),
                    });
                }
                Self::decode_page(row.payload, row.digest, seed, index)
            })
            .collect()
    }
    pub(crate) async fn save_page(&mut self, page: &SeedPage) -> Result<(), ReceiverError> {
        let payload = encode_record(page)?;
        if !fleet_sync_protocol::unit_budget::encoded_seed_page_fits_unit_limit(page, &payload) {
            return Err(ReceiverError::InvalidInput {
                field: "seed page exceeds encoded unit limit".into(),
            });
        }
        checked_queries::write_seed_page(
            self.0,
            page.seed.generation.to_string(),
            page.page.to_string(),
            page.digest.as_str().to_owned(),
            payload,
        )
        .await
        .map_err(crate::sql_transport::sql_error)?;
        Ok(())
    }
    pub(crate) async fn drop_pages(&mut self) -> Result<(), StoreError> {
        checked_queries::drop_seed_pages(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)
    }
}
