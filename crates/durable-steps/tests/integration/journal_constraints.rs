use super::{
    journal_metadata::{Result, connect, history, legacy_file, metadata, operations, receipts},
    legacy_rows::{NATURAL_CONSTRAINTS, unique_failure},
};
use crate::support::TestHost;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope, turso_store::TursoStore};
use sqlx_core::{connection::Connection, query::query};
use sqlx_turso::Turso;

#[tokio::test]
async fn every_natural_unique_refuses_duplicates_without_changing_the_upgraded_rows() -> Result {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("constraints.sqlite");
    legacy_file(&path).await?.close().await?;
    let store = TursoStore::open_and_migrate(&path, TestHost::new()).await?;
    let mut connection = connect(&path).await?;
    let before = (
        operations(&mut connection).await?,
        history(&mut connection).await?,
        receipts(&mut connection).await?,
    );
    let schema = metadata(&mut connection, "_fleet_sync_schema", "id").await?;
    let mut cases = NATURAL_CONSTRAINTS.to_vec();
    cases.push(("INSERT INTO _fleet_sync_migrations(id,version,description,installed_on,success,checksum,execution_time,created_at,updated_at) SELECT '0197abc0-def0-7123-8123-abcdef012349',version,description,installed_on,success,checksum,execution_time,created_at,updated_at FROM _fleet_sync_migrations WHERE version=1","_fleet_sync_migrations.version"));
    for (sql, columns) in cases {
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    match query::<Turso>(sql).execute(scope.executor()).await {
                        Err(error) => assert!(
                            unique_failure(&error.to_string(), columns),
                            "wrong constraint failure: {error}"
                        ),
                        Ok(_) => {
                            return Err(StoreError::Aborted {
                                reason: format!("missing UNIQUE on {columns}"),
                            });
                        }
                    }
                    Ok(())
                })
            })
            .await?;
        assert_eq!(
            (
                operations(&mut connection).await?,
                history(&mut connection).await?,
                receipts(&mut connection).await?
            ),
            before
        );
        assert_eq!(
            metadata(&mut connection, "_fleet_sync_schema", "id").await?,
            schema
        );
    }
    Ok(())
}
