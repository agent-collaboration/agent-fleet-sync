use crate::{conformance::check, support::TestHost};
use durable_steps::{
    AtomicStore, OperationId, StoreError,
    durable_object_store::DurableObjectStore,
    storage::{JournalStorage, TransactionScope},
};
use sqlx_core::{query::query, query_as::query_as};
use sqlx_durable_object::Do;

const ORIGINAL: &str = "0197abc0-def0-7123-8123-abcdef012345";
enum MalformedId {
    Text(String),
    Blob(Vec<u8>),
}
#[derive(sqlx::FromRow)]
struct IdRow {
    id: String,
}
#[derive(sqlx::FromRow)]
struct ChecksumRow {
    checksum: Vec<u8>,
}

pub(crate) async fn run(store: &DurableObjectStore<TestHost>) -> Result<(), StoreError> {
    let original = store
            .transaction(move |scope| {
                Box::pin(async move {
                    Ok(query_as::<Do, IdRow>("SELECT id FROM _fleet_sync_steps WHERE operation_id='0197abc0-def0-7123-8123-abcdef012345' AND position=0")
                        .fetch_one(scope.executor())
                        .await?
                        .id)
                })
            })
            .await?;
    for invalid in [
        MalformedId::Text(ORIGINAL.replace("7123", "4123")),
        MalformedId::Text(ORIGINAL.to_uppercase()),
        MalformedId::Text(ORIGINAL.replace('-', "")),
        MalformedId::Text("not-an-id".into()),
        MalformedId::Blob(ORIGINAL.as_bytes().to_vec()),
    ] {
        store
                .transaction(move |scope| {
                    Box::pin(async move {
                        let query = query::<Do>("UPDATE _fleet_sync_steps SET id=? WHERE operation_id='0197abc0-def0-7123-8123-abcdef012345' AND position=0");
                        match invalid {
                            MalformedId::Text(value) => {
                                query.bind(value).execute(scope.executor()).await?;
                            }
                            MalformedId::Blob(value) => {
                                query.bind(value).execute(scope.executor()).await?;
                            }
                        }
                        Ok(())
                    })
                })
                .await?;
        let result = store
            .transaction(|scope| {
                Box::pin(async move {
                    let id =
                        OperationId::try_from(ORIGINAL.to_owned()).map_err(StoreError::backend)?;
                    scope.step(id, 0).await?;
                    Ok(())
                })
            })
            .await;
        check(
            matches!(result,Err(StoreError::Corrupt{field,..}) if field=="step.id"),
            "DO malformed row id did not produce its diagnostic refusal",
        )?;
        let restored = original.clone();
        store
                .transaction(move |scope| {
                    Box::pin(async move {
                        query::<Do>("UPDATE _fleet_sync_steps SET id=? WHERE operation_id='0197abc0-def0-7123-8123-abcdef012345' AND position=0")
                            .bind(restored)
                            .execute(scope.executor())
                            .await?;
                        Ok(())
                    })
                })
                .await?;
        store.migrate().await?;
        {
            store
                .transaction(|scope| {
                    Box::pin(async move {
                        let id = OperationId::try_from(ORIGINAL.to_owned())
                            .map_err(StoreError::backend)?;
                        check(
                            scope.step(id, 0).await?.is_some(),
                            "valid DO step disappeared after refusal",
                        )
                    })
                })
                .await?;
        }
    }
    let checksum = store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(query_as::<Do, ChecksumRow>(
                    "SELECT checksum FROM _fleet_sync_migrations WHERE version=1",
                )
                .fetch_one(scope.executor())
                .await?
                .checksum)
            })
        })
        .await?;
    store
        .transaction(|scope| {
            Box::pin(async move {
                query::<Do>("UPDATE _fleet_sync_migrations SET checksum=x'00' WHERE version=1")
                    .execute(scope.executor())
                    .await?;
                Ok(())
            })
        })
        .await?;
    check(
        matches!(store.migrate().await,Err(StoreError::Corrupt{field,..}) if field=="migration checksum"),
        "edited DO migration checksum was accepted",
    )?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                query::<Do>("UPDATE _fleet_sync_migrations SET checksum=? WHERE version=1")
                    .bind(checksum)
                    .execute(scope.executor())
                    .await?;
                Ok(())
            })
        })
        .await?;
    store.migrate().await?;
    Ok(())
}
