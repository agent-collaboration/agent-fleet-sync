use sqlx_core::{
    connection::{ConnectOptions, Connection},
    row::Row,
    sql_str::AssertSqlSafe,
};
use sqlx_turso::{Turso, TursoConnectOptions};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

// Preserve the journal portion of the former standalone engine-key audit.
#[tokio::test]
async fn journal_keys_declare_not_null_and_reject_null_with_valid_insert_controls() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mut checked_keys = 0;
    let mut checked_refusals = 0;
    // Arrange: use the production journal corpus on native Turso. Foreign-key
    // behavior belongs to the engine tests; these rows isolate key constraints.
    let migrations = sqlx::migrate!("./migrations");
    let tables = [
        "_fleet_sync_operations",
        "_fleet_sync_steps",
        "_fleet_sync_schema",
    ];
    let owner = "journal";
    let mut connection = TursoConnectOptions::new()
        .filename(directory.path().join(format!("{owner}.db")))
        .create_if_missing(true)
        .foreign_keys(false)
        .connect()
        .await?;
    migrations.run_direct(None, &mut connection, false).await?;
    for table in tables {
        let columns = sqlx::query::<Turso>(AssertSqlSafe(format!("PRAGMA table_info({table})")))
            .fetch_all(&mut connection)
            .await?;
        let keys = columns
            .iter()
            .filter_map(|row| match row.try_get::<i64, _>("pk") {
                Ok(0) => None,
                result => Some(result.map(|_| row)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert!(!keys.is_empty(), "audited table {table} has no key");
        let names = columns
            .iter()
            .map(|row| row.try_get::<String, _>("name"))
            .collect::<Result<Vec<_>, _>>()?;
        let literals = columns
            .iter()
            .map(|row| {
                let kind: String = row.try_get("type")?;
                Ok::<_, sqlx_core::Error>(if kind.eq_ignore_ascii_case("BLOB") {
                    "X'00'"
                } else if kind.eq_ignore_ascii_case("INTEGER") {
                    "0"
                } else {
                    "'{}'"
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Act and assert: each declared key rejects NULL, while its valid
        // counterpart inserts through the same real database connection.
        for key in keys {
            let name: String = key.try_get("name")?;
            let kind: String = key.try_get("type")?;
            assert_eq!(
                key.try_get::<i64, _>("notnull")?,
                1,
                "{owner}.{table}.{name} must declare NOT NULL"
            );
            checked_keys += 1;
            let rowid_alias = kind.eq_ignore_ascii_case("INTEGER")
                && columns
                    .iter()
                    .filter(|row| row.try_get::<i64, _>("pk").is_ok_and(|key| key > 0))
                    .count()
                    == 1;
            if rowid_alias {
                continue;
            }
            let offered = names
                .iter()
                .zip(&literals)
                .map(
                    |(column, value)| {
                        if column == &name { "NULL" } else { *value }
                    },
                )
                .collect::<Vec<_>>()
                .join(",");
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            let result = sqlx::query::<Turso>(AssertSqlSafe(format!(
                "INSERT INTO {table}({}) VALUES({offered})",
                names.join(",")
            )))
            .execute(&mut *transaction)
            .await;
            let error = result
                .err()
                .ok_or_else(|| format!("NULL accepted at {table}.{name}"))?;
            assert!(
                error.to_string().contains(&format!("{table}.{name}")),
                "wrong refusal for {table}.{name}: {error}"
            );
            transaction.rollback().await?;
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query::<Turso>(AssertSqlSafe(format!(
                "INSERT INTO {table}({}) VALUES({})",
                names.join(","),
                literals.join(",")
            )))
            .execute(&mut *transaction)
            .await?;
            transaction.rollback().await?;
            checked_refusals += 1;
        }
    }
    assert_eq!(
        checked_keys, 3,
        "all three journal primary keys were audited"
    );
    assert_eq!(
        checked_refusals, 3,
        "every journal key has a NULL and valid control"
    );
    eprintln!(
        "audited {checked_keys} key declarations; {checked_refusals} NULL refusals with valid-insert controls"
    );
    Ok(())
}
