use crate::{StoreError, checked_queries, copy_contract::CopyRecord};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{Piece, PieceDigest, TransactionKey};
use serde::{Serialize, de::DeserializeOwned};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) fn encode_record<T: Serialize>(record: &T) -> Result<String, StoreError> {
    serde_json::to_string(record).map_err(|_| StoreError::Corrupt {
        field: "encoded record".into(),
    })
}

pub(crate) fn decode_record<T: DeserializeOwned>(
    record: Option<String>,
    field: &str,
) -> Result<T, StoreError> {
    let record = record.ok_or_else(|| StoreError::Corrupt {
        field: field.into(),
    })?;
    serde_json::from_str(&record).map_err(|_| StoreError::Corrupt {
        field: field.into(),
    })
}

pub(crate) fn piece_identity<A>(piece: &Piece<A>) -> Result<String, StoreError> {
    encode_record(&(
        piece.database,
        piece.epoch,
        piece.continues_from,
        piece.through,
        piece.part.as_ref().map(|part| part.index),
    ))
}

pub(crate) struct CopyStorage<'scope, S>(pub &'scope mut S);

impl<S> CopyStorage<'_, S>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub(crate) async fn load(&mut self) -> Result<Option<CopyRecord>, StoreError> {
        let Some(row) = checked_queries::read_copy(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?
        else {
            return Ok(None);
        };
        let copy: CopyRecord = decode_record(row.record, "copy record")?;
        copy.validate()?;
        Ok(Some(copy))
    }

    pub(crate) async fn save(&mut self, copy: &CopyRecord) -> Result<(), StoreError> {
        copy.validate()?;
        checked_queries::write_copy(self.0, encode_record(copy)?)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        Ok(())
    }

    pub(crate) async fn erase_records(&mut self) -> Result<(), StoreError> {
        checked_queries::erase_receipts(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        checked_queries::erase_staged_parts(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        checked_queries::erase_fence_receipts(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        checked_queries::erase_schema_steps(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        checked_queries::erase_seed(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        checked_queries::erase_seed_pages(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        checked_queries::erase_seed_attempts(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        checked_queries::erase_creation(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        checked_queries::erase_copy_tables(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        Ok(())
    }

    pub(crate) async fn creation(
        &mut self,
    ) -> Result<Option<fleet_sync_protocol::CreateCopy>, StoreError> {
        checked_queries::read_creation(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?
            .map(|row| decode_record(row.request, "original copy creation"))
            .transpose()
    }
    pub(crate) async fn save_creation(
        &mut self,
        request: &fleet_sync_protocol::CreateCopy,
    ) -> Result<(), StoreError> {
        checked_queries::write_creation(self.0, encode_record(request)?)
            .await
            .map_err(crate::sql_transport::sql_error)
    }

    pub(crate) async fn receipt<A>(
        &mut self,
        piece: &Piece<A>,
    ) -> Result<Option<PieceDigest>, StoreError> {
        let Some(row) = checked_queries::read_receipt(self.0, piece_identity(piece)?)
            .await
            .map_err(crate::sql_transport::sql_error)?
        else {
            return Ok(None);
        };
        let digest = row.digest.ok_or_else(|| StoreError::Corrupt {
            field: "receipt digest".into(),
        })?;
        PieceDigest::try_from(digest)
            .map(Some)
            .map_err(|_| StoreError::Corrupt {
                field: "receipt digest".into(),
            })
    }

    pub(crate) async fn save_receipt<A>(&mut self, piece: &Piece<A>) -> Result<(), StoreError> {
        checked_queries::write_receipt(
            self.0,
            piece_identity(piece)?,
            piece.digest.as_str().to_owned(),
        )
        .await
        .map_err(crate::sql_transport::sql_error)?;
        Ok(())
    }

    pub(crate) async fn parts<A: Serialize + DeserializeOwned>(
        &mut self,
        transaction: TransactionKey,
    ) -> Result<Vec<Piece<A>>, StoreError> {
        let rows = checked_queries::read_parts(self.0, encode_record(&transaction)?)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        rows.into_iter()
            .map(|row| {
                let piece: Piece<A> = decode_record(row.payload, "staged part")?;
                let part = piece.part.as_ref().ok_or_else(|| StoreError::Corrupt {
                    field: "staged part".into(),
                })?;
                let index = row
                    .part_index
                    .and_then(|value| value.parse::<u32>().ok())
                    .ok_or_else(|| StoreError::Corrupt {
                        field: "staged index".into(),
                    })?;
                if row.part_index_class.as_deref() != Some("integer")
                    || index != part.index
                    || part.transaction != transaction
                    || piece.recomputed_digest().map_err(|_| StoreError::Corrupt {
                        field: "staged digest".into(),
                    })? != piece.digest
                {
                    return Err(StoreError::Corrupt {
                        field: "staged part".into(),
                    });
                }
                Ok(piece)
            })
            .collect()
    }

    pub(crate) async fn save_part<A: Serialize>(
        &mut self,
        piece: &Piece<A>,
        staged_at: fleet_sync_protocol::SyncTime,
    ) -> Result<(), StoreError> {
        let part = piece.part.as_ref().ok_or_else(|| StoreError::Corrupt {
            field: "staged part".into(),
        })?;
        checked_queries::write_part(
            self.0,
            encode_record(&part.transaction)?,
            part.index.to_string(),
            staged_at.unix_millis().to_string(),
            encode_record(piece)?,
        )
        .await
        .map_err(crate::sql_transport::sql_error)?;
        Ok(())
    }

    pub(crate) async fn drop_parts(
        &mut self,
        transaction: TransactionKey,
    ) -> Result<(), StoreError> {
        checked_queries::drop_parts(self.0, encode_record(&transaction)?)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        Ok(())
    }

    pub(crate) async fn fence_receipt(
        &mut self,
        request: &fleet_sync_protocol::FenceRequest,
    ) -> Result<Option<fleet_sync_protocol::FenceResult>, crate::ReceiverError> {
        let Some(row) =
            checked_queries::read_fence_receipt(self.0, encode_record(&request.transfer_id)?)
                .await
                .map_err(crate::sql_transport::sql_error)?
        else {
            return Ok(None);
        };
        let saved_request = encode_record(request)?;
        if row.request.as_deref() != Some(saved_request.as_str()) {
            return Err(crate::ReceiverError::FenceConflict);
        }
        let result = decode_record(row.result, "fence receipt")?;
        Ok(Some(result))
    }

    pub(crate) async fn save_fence(
        &mut self,
        request: &fleet_sync_protocol::FenceRequest,
        old_holder: Option<fleet_sync_protocol::PeerId>,
        result: &fleet_sync_protocol::FenceResult,
    ) -> Result<(), StoreError> {
        let fleet_sync_protocol::FenceResult::Fenced { old_epoch, .. } = result else {
            return Err(StoreError::Corrupt {
                field: "successful fence receipt".into(),
            });
        };
        let saved_request = encode_record(request)?;
        checked_queries::write_fence_receipt(
            self.0,
            encode_record(&request.transfer_id)?,
            saved_request,
            old_epoch.value().to_string(),
            old_holder.as_ref().map(encode_record).transpose()?,
            encode_record(result)?,
        )
        .await
        .map_err(crate::sql_transport::sql_error)?;
        Ok(())
    }

    pub(crate) async fn epoch_end(
        &mut self,
        epoch: fleet_sync_protocol::Epoch,
    ) -> Result<
        Option<(
            Option<fleet_sync_protocol::PeerId>,
            fleet_sync_protocol::ReplicationPosition,
        )>,
        StoreError,
    > {
        let Some(row) = checked_queries::read_epoch_fence(self.0, epoch.value().to_string())
            .await
            .map_err(crate::sql_transport::sql_error)?
        else {
            return Ok(None);
        };
        let holder = row
            .old_holder
            .map(|value| decode_record(Some(value), "epoch holder"))
            .transpose()?;
        let result: fleet_sync_protocol::FenceResult = decode_record(row.result, "epoch fence")?;
        let fleet_sync_protocol::FenceResult::Fenced {
            old_epoch, start, ..
        } = result
        else {
            return Err(StoreError::Corrupt {
                field: "epoch fence".into(),
            });
        };
        if old_epoch != epoch {
            return Err(StoreError::Corrupt {
                field: "epoch fence".into(),
            });
        }
        Ok(Some((holder, start)))
    }
}
