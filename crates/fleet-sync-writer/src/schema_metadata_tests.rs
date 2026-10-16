#![allow(clippy::panic_in_result_fn)] // Assertions decide behavior; Result propagates fixture failures.

use crate::{
    OpenError, SchemaForm,
    foreign_key_integrity::reject_orphans,
    schema_inspection::{inspect_replicated_tables, read_layout},
};
use fleet_sync_protocol::TableName;
use sqlx_core::{
    connection::ConnectOptions,
    executor::Executor,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};
use sqlx_turso::TursoConnectOptions;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn explicit_and_implicit_keys_reject_persisted_orphans_without_rejecting_null_children()
-> TestResult {
    for (parent, child, good_parent, good_child, bad_child, null_child) in [
        (
            "id INTEGER PRIMARY KEY",
            "id TEXT PRIMARY KEY,parent_id INTEGER REFERENCES parents(id)",
            "(1)",
            "('good',1)",
            "('orphan',9)",
            "('null',NULL)",
        ),
        (
            "a TEXT,b INTEGER,PRIMARY KEY(a,b)",
            "id TEXT PRIMARY KEY,a TEXT,b INTEGER,FOREIGN KEY(a,b) REFERENCES parents(a,b)",
            "('x',1)",
            "('good','x',1)",
            "('orphan','x',9)",
            "('null','x',NULL)",
        ),
        (
            "id INTEGER PRIMARY KEY",
            "id TEXT PRIMARY KEY,parent_id INTEGER REFERENCES parents",
            "(1)",
            "('good',1)",
            "('orphan',9)",
            "('null',NULL)",
        ),
    ] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("migration.db");
        let mut connection = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .foreign_keys(true)
            .connect()
            .await?;
        connection.execute(AssertSqlSafe(format!("CREATE TABLE parents({parent}); CREATE TABLE children({child}); INSERT INTO parents VALUES {good_parent}; INSERT INTO children VALUES {good_child},{null_child}")).into_sql_str()).await?;
        let tables = [TableName::new("parents")?, TableName::new("children")?];
        inspect_replicated_tables(&mut connection, &tables).await?;
        reject_orphans(&mut connection, &tables).await?;
        assert!(
            connection
                .execute(
                    AssertSqlSafe(format!("INSERT INTO children VALUES {bad_child}"))
                        .into_sql_str()
                )
                .await
                .is_err(),
            "writer enforcement must remain real"
        );
        connection.execute("PRAGMA foreign_keys=OFF").await?;
        connection
            .execute(
                AssertSqlSafe(format!("INSERT INTO children VALUES {bad_child}")).into_sql_str(),
            )
            .await?;
        sqlx_core::connection::Connection::close(connection).await?;
        let mut reopened = TursoConnectOptions::new()
            .filename(&path)
            .foreign_keys(true)
            .connect()
            .await?;
        assert!(
            reopened
                .fetch_all("PRAGMA foreign_key_check")
                .await?
                .is_empty(),
            "upstream behavior changed: reassess the explicit workaround"
        );
        assert!(matches!(
            reject_orphans(&mut reopened, &tables).await,
            Err(OpenError::SchemaUnsupported {
                form: SchemaForm::ForeignKeyOrphan { .. }
            })
        ));
    }
    Ok(())
}

#[tokio::test]
async fn implicit_references_without_a_matching_parent_key_fail_closed() -> TestResult {
    for (parent, child) in [
        (
            "id INTEGER",
            "id TEXT PRIMARY KEY,parent_id INTEGER REFERENCES parents",
        ),
        (
            "id INTEGER PRIMARY KEY",
            "id TEXT PRIMARY KEY,a INTEGER,b INTEGER,FOREIGN KEY(a,b) REFERENCES parents",
        ),
    ] {
        let mut connection = TursoConnectOptions::new()
            .foreign_keys(false)
            .connect()
            .await?;
        connection
            .execute(
                AssertSqlSafe(format!(
                    "CREATE TABLE parents({parent}); CREATE TABLE children({child})"
                ))
                .into_sql_str(),
            )
            .await?;
        let tables = [TableName::new("children")?];
        assert!(matches!(
            inspect_replicated_tables(&mut connection, &tables).await,
            Err(OpenError::SchemaUnsupported {
                form: SchemaForm::ForeignKeyMetadata { .. }
            })
        ));
    }
    Ok(())
}

#[tokio::test]
async fn table_layout_keeps_declared_column_and_key_order_distinct() -> TestResult {
    let mut connection = TursoConnectOptions::new().connect().await?;
    connection
        .execute("CREATE TABLE ordered_keys(b INTEGER,label TEXT,a TEXT,PRIMARY KEY(a,b))")
        .await?;
    let layout = read_layout(&mut connection, &TableName::new("ordered_keys")?).await?;
    assert_eq!(layout.columns(), ["b", "label", "a"]);
    assert_eq!(layout.primary_key(), [2, 0]);
    Ok(())
}

#[tokio::test]
async fn unsupported_schema_is_reported_at_its_table() -> TestResult {
    let mut connection = TursoConnectOptions::new().connect().await?;
    connection.execute("CREATE TABLE no_key(payload TEXT); CREATE TABLE parents(id INTEGER PRIMARY KEY); CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE)").await?;
    assert!(matches!(
        inspect_replicated_tables(&mut connection, &[TableName::new("no_key")?]).await,
        Err(OpenError::SchemaUnsupported {
            form: SchemaForm::NoPrimaryKey { .. }
        })
    ));
    assert!(matches!(
        inspect_replicated_tables(&mut connection, &[TableName::new("children")?]).await,
        Err(OpenError::SchemaUnsupported {
            form: SchemaForm::ForeignKeyAction { .. }
        })
    ));
    assert!(matches!(
        inspect_replicated_tables(&mut connection, &[TableName::new("_fleet_sync_writer")?]).await,
        Err(OpenError::ReservedTableName { .. })
    ));
    Ok(())
}
