use crate::{
    InstallPageReceipt, SnapshotInstall, SnapshotInstallError, StoreError, checked_queries,
    copy_storage::{decode_record, encode_record},
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{CopyStatus, SnapshotManifest};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct InstallMarker {
    pub install: SnapshotInstall,
    pub source: CopyStatus,
    pub schema_steps: Option<Vec<String>>,
    pub verified: Option<SnapshotManifest>,
    pub ready: bool,
}
pub(crate) struct InstallStorage<'a, S>(pub &'a mut S);
impl<S> InstallStorage<'_, S>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'a> String: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Option<String>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub async fn load(&mut self) -> Result<InstallMarker, SnapshotInstallError> {
        self.optional()
            .await?
            .ok_or(SnapshotInstallError::NotPrepared)
    }
    pub async fn optional(&mut self) -> Result<Option<InstallMarker>, SnapshotInstallError> {
        let Some(row) = checked_queries::read_install(self.0).await? else {
            return Ok(None);
        };
        let marker: InstallMarker = decode_record(row.record, "snapshot install marker")?;
        crate::copy_contract::validate_copy_status(&marker.source)?;
        if marker.install.install_id.is_nil()
            || marker.install.database != marker.source.database
            || marker.install.position != marker.source.acked
            || marker.install.schema != marker.source.schema
            || (marker.ready && marker.verified.is_none())
            || marker.verified.as_ref().is_some_and(|manifest| {
                manifest.database != marker.install.database
                    || manifest.position != marker.install.position
                    || manifest.schema != marker.install.schema
            })
        {
            return Err(StoreError::Corrupt {
                field: "snapshot install marker".into(),
            }
            .into());
        }
        Ok(Some(marker))
    }
    pub async fn save(&mut self, marker: &InstallMarker) -> Result<(), SnapshotInstallError> {
        checked_queries::write_install(self.0, encode_record(marker)?).await?;
        Ok(())
    }
    pub async fn pages(
        &mut self,
        id: uuid::Uuid,
    ) -> Result<Vec<InstallPageReceipt>, SnapshotInstallError> {
        let mut receipts = Vec::new();
        for row in checked_queries::read_install_pages(self.0, id.to_string()).await? {
            let receipt: InstallPageReceipt = decode_record(row.record, "snapshot page receipt")?;
            if receipt.install_id != id
                || usize::try_from(receipt.page).ok() != Some(receipts.len())
            {
                return Err(StoreError::Corrupt {
                    field: "snapshot page receipt order".into(),
                }
                .into());
            }
            receipts.push(receipt);
        }
        Ok(receipts)
    }
    pub async fn save_page(
        &mut self,
        receipt: &InstallPageReceipt,
    ) -> Result<(), SnapshotInstallError> {
        checked_queries::write_install_page(
            self.0,
            receipt.install_id.to_string(),
            receipt.page.to_string(),
            encode_record(receipt)?,
        )
        .await?;
        Ok(())
    }
}
