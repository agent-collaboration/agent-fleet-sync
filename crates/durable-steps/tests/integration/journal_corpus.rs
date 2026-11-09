use super::journal_metadata::{Result, connect};
use durable_steps::turso_store::{journal_migrations, migrate_journal};
use sqlx_core::{connection::Connection, executor::Executor, query::query, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn exported_corpus_is_exactly_what_migrate_journal_applies() -> Result {
    let directory = tempfile::tempdir()?;
    let mut connection = connect(&directory.path().join("corpus.sqlite")).await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    migrate_journal(&mut transaction).await?;
    transaction.commit().await?;

    let applied = query::<Turso>("SELECT version FROM _fleet_sync_migrations ORDER BY version")
        .fetch_all(&mut connection)
        .await?
        .iter()
        .map(|row| row.try_get::<i64, _>("version"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let exported = journal_migrations()
        .map(|migration| migration.version)
        .collect::<Vec<_>>();
    assert!(!exported.is_empty());
    assert_eq!(applied, exported);
    connection.close().await?;
    Ok(())
}

#[tokio::test]
async fn extended_native_receipts_are_adopted_exactly_and_wrong_checksums_refuse() -> Result {
    for wrong_checksum in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("extended.sqlite");
        let mut connection = connect(&path).await?;
        let mut old = sqlx_core::migrate::Migrator::with_migrations(
            journal_migrations()
                .filter(|migration| migration.version <= 2)
                .cloned()
                .collect(),
        );
        old.dangerous_set_table_name("_fleet_sync_migrations");
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        old.run_direct(None, &mut *transaction, false).await?;
        transaction
            .execute(include_str!(
                "../../native-migrations/0001_receipt_metadata.sql"
            ))
            .await?;
        if wrong_checksum {
            query::<Turso>("UPDATE _fleet_sync_migrations SET checksum=x'00' WHERE version=1")
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        let before = super::journal_metadata::receipts(&mut connection).await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = migrate_journal(&mut transaction).await;
        if wrong_checksum {
            assert!(
                matches!(result, Err(durable_steps::StoreError::Corrupt { field, .. }) if field == "migration checksum")
            );
            transaction.rollback().await?;
        } else {
            result?;
            transaction.commit().await?;
        }
        let after = super::journal_metadata::receipts(&mut connection).await?;
        assert_eq!(&after[..before.len()], before.as_slice());
        let columns = query::<Turso>("PRAGMA table_info(_fleet_sync_migrations)")
            .fetch_all(&mut connection)
            .await?;
        let names = columns
            .iter()
            .map(|row| row.try_get::<String, _>("name"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if wrong_checksum {
            assert!(names.iter().any(|name| name == "id"));
        } else {
            assert_eq!(
                names,
                [
                    "version",
                    "description",
                    "installed_on",
                    "success",
                    "checksum",
                    "execution_time"
                ]
            );
            let primary = columns
                .iter()
                .filter(|row| row.try_get::<i64, _>("pk").is_ok_and(|value| value > 0))
                .map(|row| row.try_get::<String, _>("name"))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            assert_eq!(primary, ["version"]);
            assert_eq!(after.len(), 3);
        }
        connection.close().await?;
    }
    Ok(())
}
