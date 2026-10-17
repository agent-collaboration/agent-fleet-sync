use crate::{DeliveryError, PublicationRef, checked_queries as queries, custody_record::*};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub async fn resolve_publication<S, D>(
    scope: &mut S,
    reference: PublicationRef,
) -> Result<Option<StatusPublication<D::Acceptance>>, DeliveryError>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    let Some(row) =
        queries::read_publication(scope, item_key(reference.item), encode(&reference.target)?)
            .await
            .map_err(sql_error)?
    else {
        return Ok(None);
    };
    let publication: StatusPublication<D::Acceptance> = decode(row.record, "owed publication")?;
    let version = integer(row.version, row.version_class, "owed publication version")?;
    if publication.item != reference.item || publication.version.value() != version || version == 0
    {
        return Err(DeliveryError::Corrupt {
            field: "owed publication identity",
        });
    }
    if publication.version > reference.version {
        return Ok(None);
    }
    if publication.version < reference.version {
        return Err(DeliveryError::Corrupt {
            field: "owed publication went backward",
        });
    }
    Ok(Some(publication))
}

pub async fn retire_publication<S>(
    scope: &mut S,
    reference: PublicationRef,
) -> Result<(), DeliveryError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
{
    queries::retire_publication(
        scope,
        item_key(reference.item),
        encode(&reference.target)?,
        reference.version.value().to_string(),
    )
    .await
    .map_err(sql_error)
}

/// Admit every owed revision before committing the transaction that created it.
pub async fn admit_publications<S, D, T>(
    scope: &mut S,
) -> Result<Vec<durable_steps::Admitted>, DeliveryError>
where
    S: TransactionScope + durable_steps::JournalScope,
    D: DeliveryTypes,
    T: durable_steps::Task<Input = PublicationRef>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    let rows = queries::list_publications(scope).await.map_err(sql_error)?;
    let mut admitted = Vec::with_capacity(rows.len());
    for row in rows {
        let publication: StatusPublication<D::Acceptance> = decode(row.record, "owed publication")?;
        let version = integer(row.version, row.version_class, "owed publication version")?;
        if row.item_id.as_deref() != Some(item_key(publication.item).as_str())
            || publication.version.value() != version
            || version == 0
        {
            return Err(DeliveryError::Corrupt {
                field: "owed publication identity",
            });
        }
        let target = decode(row.target, "owed publication target")?;
        let reference = PublicationRef {
            item: publication.item,
            version: publication.version,
            target,
        };
        admitted.push(
            scope
                .admit::<T>(&crate::delivery_request_key(&reference)?, &reference)
                .await
                .map_err(|error| DeliveryError::Storage(error.to_string()))?,
        );
    }
    Ok(admitted)
}
