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

pub(crate) async fn run(store: &DurableObjectStore<TestHost>) -> Result<(), StoreError> {
    for schema in [false, true] {
        let (read, write) = if schema {
            (
                "SELECT id FROM _fleet_sync_schema",
                "UPDATE _fleet_sync_schema SET id=?",
            )
        } else {
            (
                "SELECT id FROM _fleet_sync_steps WHERE operation_id='0197abc0-def0-7123-8123-abcdef012345' AND position=0",
                "UPDATE _fleet_sync_steps SET id=? WHERE operation_id='0197abc0-def0-7123-8123-abcdef012345' AND position=0",
            )
        };
        let original = store
            .transaction(move |scope| {
                Box::pin(async move {
                    Ok(query_as::<Do, IdRow>(read)
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
                        let query = query::<Do>(write);
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
            let result = if schema {
                store.migrate().await
            } else {
                store
                    .transaction(|scope| {
                        Box::pin(async move {
                            let id = OperationId::try_from(ORIGINAL.to_owned())
                                .map_err(StoreError::backend)?;
                            scope.step(id, 0).await?;
                            Ok(())
                        })
                    })
                    .await
            };
            check(
                matches!(result,Err(StoreError::Corrupt{field,..}) if field==if schema {"_fleet_sync_schema.id"}else{"step.id"}),
                "DO malformed row id did not produce its diagnostic refusal",
            )?;
            let restored = original.clone();
            store
                .transaction(move |scope| {
                    Box::pin(async move {
                        query::<Do>(write)
                            .bind(restored)
                            .execute(scope.executor())
                            .await?;
                        Ok(())
                    })
                })
                .await?;
            store.migrate().await?;
            if !schema {
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
    }
    Ok(())
}
