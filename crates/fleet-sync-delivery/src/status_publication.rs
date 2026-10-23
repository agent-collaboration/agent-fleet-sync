use crate::{DeliveryError, PublicationRef, checked_queries as queries, custody_record::*};
use crate::{PublicationTarget, status_stream::canonical_encode};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub async fn resolve_publication<S, D>(
    scope: &mut S,
    reference: PublicationRef<D::StatusRoute>,
) -> Result<Option<StatusPublication<D::Acceptance>>, DeliveryError>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    crate::delivery_sizes::check_status_route::<D>(reference.target.route())?;
    let Some(row) = queries::read_publication(
        scope,
        item_key(reference.item),
        canonical_encode(&reference.target)?,
    )
    .await
    .map_err(sql_error)?
    else {
        return Ok(None);
    };
    let publication: StatusPublication<D::Acceptance> = decode(row.record, "owed publication")?;
    crate::delivery_sizes::check_status::<D>(&publication.status).map_err(|_| {
        DeliveryError::Corrupt {
            field: "stored publication bounds",
        }
    })?;
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

pub async fn retire_publication<S, D>(
    scope: &mut S,
    reference: PublicationRef<D::StatusRoute>,
) -> Result<(), DeliveryError>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
{
    crate::delivery_sizes::check_status_route::<D>(reference.target.route())?;
    queries::retire_publication(
        scope,
        item_key(reference.item),
        canonical_encode(&reference.target)?,
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
    T: durable_steps::Task<Input = PublicationRef<D::StatusRoute>>,
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
        let encoded_target = row.target.ok_or(DeliveryError::Corrupt {
            field: "owed publication target",
        })?;
        let target: PublicationTarget<D::StatusRoute> =
            decode(Some(encoded_target.clone()), "owed publication target")?;
        crate::delivery_sizes::check_status_route::<D>(target.route()).map_err(|_| {
            DeliveryError::Corrupt {
                field: "stored publication route bounds",
            }
        })?;
        if canonical_encode(&target)? != encoded_target {
            return Err(DeliveryError::Corrupt {
                field: "owed publication target key",
            });
        }
        let reference = PublicationRef {
            item: publication.item,
            version: publication.version,
            target,
        };
        admitted.push(
            scope
                .admit::<T>(&publication_request_key(&reference)?, &reference)
                .await
                .map_err(|error| DeliveryError::Storage(error.to_string()))?,
        );
    }
    Ok(admitted)
}

pub(crate) fn publication_request_key<R: serde::Serialize>(
    reference: &PublicationRef<R>,
) -> Result<durable_steps::RequestKey, DeliveryError> {
    durable_steps::RequestKey::new(canonical_encode(reference)?)
        .map_err(|error| DeliveryError::Encoding(error.to_string()))
}
