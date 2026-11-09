use super::journal_metadata::{Result, connect};
use durable_steps::turso_store::{journal_migrations, migrate_journal};
use sqlx_core::{
    connection::Connection, executor::Executor, query::query, row::Row, sql_str::AssertSqlSafe,
};
use sqlx_turso::{Turso, TursoConnection};

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

#[derive(Debug, PartialEq)]
struct ReceiptStoreImage {
    schema: Vec<(String, String, String, Option<String>)>,
    columns: Vec<(String, Vec<(String, String)>)>,
}

async fn receipt_store_image(connection: &mut TursoConnection) -> Result<ReceiptStoreImage> {
    let schema =
        query::<Turso>("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")
            .fetch_all(&mut *connection)
            .await?
            .iter()
            .map(|row| {
                Ok((
                    row.try_get("type")?,
                    row.try_get("name")?,
                    row.try_get("tbl_name")?,
                    row.try_get("sql")?,
                ))
            })
            .collect::<std::result::Result<Vec<_>, sqlx_core::Error>>()?;
    let names = query::<Turso>("PRAGMA table_info(_fleet_sync_migrations)")
        .fetch_all(&mut *connection)
        .await?;
    let mut columns = Vec::new();
    for row in names {
        let name = row.try_get::<String, _>("name")?;
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!(
            "SELECT typeof({quoted}) AS storage_class,quote({quoted}) AS value FROM _fleet_sync_migrations ORDER BY version"
        );
        let cells = query::<Turso>(AssertSqlSafe(sql))
            .fetch_all(&mut *connection)
            .await?
            .iter()
            .map(|row| Ok((row.try_get("storage_class")?, row.try_get("value")?)))
            .collect::<std::result::Result<Vec<_>, sqlx_core::Error>>()?;
        columns.push((name, cells));
    }
    Ok(ReceiptStoreImage { schema, columns })
}

async fn assert_unsupported_receipt_shape_refused(legacy_sql: String) -> Result {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("unsupported-receipts.sqlite");
    let mut connection = connect(&path).await?;
    let mut old = sqlx_core::migrate::Migrator::with_migrations(
        journal_migrations()
            .filter(|migration| migration.version <= 2)
            .cloned()
            .collect(),
    );
    old.dangerous_set_table_name("_fleet_sync_migrations");
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    Box::pin(old.run_direct(None, &mut *transaction, false)).await?;
    sqlx_core::raw_sql::raw_sql(AssertSqlSafe(legacy_sql))
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    let before = Box::pin(receipt_store_image(&mut connection)).await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let result = Box::pin(migrate_journal(&mut transaction)).await;
    assert!(
        matches!(result,Err(durable_steps::StoreError::Corrupt {field,..}) if field=="migration receipt schema"),
        "unsupported receipt shape must refuse before rebuilding"
    );
    assert_eq!(
        Box::pin(receipt_store_image(&mut transaction)).await?,
        before,
        "refusal changed the owner's open transaction"
    );
    transaction.rollback().await?;
    connection.close().await?;
    let mut reopened = connect(&path).await?;
    assert_eq!(
        Box::pin(receipt_store_image(&mut reopened)).await?,
        before,
        "refusal changed the persisted schema or receipt values"
    );
    reopened.close().await?;
    Ok(())
}

#[tokio::test]
async fn native_adoption_refuses_an_extra_populated_receipt_column() -> Result {
    let sql = include_str!("../../native-migrations/0001_receipt_metadata.sql").replace(
        "    id TEXT PRIMARY KEY",
        "    owner_extension TEXT NOT NULL DEFAULT 'retained owner value',\n    id TEXT PRIMARY KEY",
    );
    Box::pin(assert_unsupported_receipt_shape_refused(sql)).await
}

#[tokio::test]
async fn native_adoption_refuses_a_missing_receipt_column() -> Result {
    let sql = include_str!("../../native-migrations/0001_receipt_metadata.sql")
        .lines()
        .filter(|line| !line.trim_start().starts_with("id TEXT PRIMARY KEY"))
        .collect::<Vec<_>>()
        .join("\n");
    Box::pin(assert_unsupported_receipt_shape_refused(sql)).await
}

#[tokio::test]
async fn native_adoption_refuses_a_wrong_receipt_column_type() -> Result {
    let sql = include_str!("../../native-migrations/0001_receipt_metadata.sql")
        .replace("description TEXT NOT NULL", "description BLOB NOT NULL");
    Box::pin(assert_unsupported_receipt_shape_refused(sql)).await
}
