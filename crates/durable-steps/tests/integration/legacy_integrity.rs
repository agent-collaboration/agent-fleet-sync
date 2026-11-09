//! Rebuilding metadata may reject broken foreign keys but must never drop rows.
use super::journal_metadata::{Result, connect, history, legacy_file};
use crate::support::TestHost;
use durable_steps::turso_store::{TursoStore, TursoStoreOptions};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};

#[tokio::test]
async fn upgrade_never_silently_discards_a_legacy_step_with_no_parent() -> Result {
    for foreign_keys in [true, false] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("orphan-step.sqlite");
        legacy_file(&path).await?.close().await?;
        let mut connection = TursoConnectOptions::new()
            .filename(&path)
            .foreign_keys(false)
            .connect()
            .await?;
        query::<Turso>("INSERT INTO _fleet_sync_steps(operation_id,position,step_name,kind,output) VALUES('0197abc0-def0-7123-8123-abcdef012346',2,'orphan','local',X'37')")
            .execute(&mut connection).await?;
        let before = history(&mut connection).await?;
        assert_eq!(before.len(), 4);
        connection.close().await?;
        let result =
            TursoStore::open_with(&path, TursoStoreOptions { foreign_keys }, TestHost::new()).await;
        if foreign_keys {
            assert!(
                result.is_err(),
                "enforced upgrade must refuse the missing parent"
            );
        } else {
            drop(result?);
        }
        let mut connection = connect(&path).await?;
        assert_eq!(
            history(&mut connection).await?,
            before,
            "upgrade silently discarded a saved step"
        );
        let receipt = query::<Turso>("SELECT max(version) AS version FROM _fleet_sync_migrations")
            .fetch_one(&mut connection)
            .await?;
        assert_eq!(
            receipt.try_get::<i64, _>("version")?,
            if foreign_keys { 1 } else { 3 }
        );
    }
    Ok(())
}
