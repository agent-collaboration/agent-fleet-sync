use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::TryStreamExt;
use sqlx_core::{
    column::Column,
    connection::{ConnectOptions, Connection},
    error::{Error, ErrorKind},
    executor::Executor,
    query::query,
    row::Row,
    sql_str::{SqlSafeStr, SqlStr},
    statement::Statement,
    value::ValueRef,
};

use crate::{Turso, TursoAdapterError, TursoConnectOptions, TursoConnection};

async fn memory_connection() -> sqlx_core::Result<TursoConnection> {
    TursoConnectOptions::new().connect().await
}

fn adapter_error(error: &Error) -> Option<&TursoAdapterError> {
    match error {
        Error::Configuration(source) => source.downcast_ref::<TursoAdapterError>(),
        _ => None,
    }
}

#[tokio::test]
async fn executes_literal_sql_and_fetches_rows() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;
    (&mut connection)
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        .await?;

    // Act
    let result = (&mut connection)
        .execute("INSERT INTO test (name) VALUES ('alice')")
        .await?;
    let rows = (&mut connection)
        .fetch_all("SELECT id, name FROM test")
        .await?;

    // Assert
    assert_eq!(result.rows_affected(), 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns()[0].name(), "id");
    assert!(!rows[0].try_get_raw(0)?.is_null());
    Ok(())
}

#[tokio::test]
async fn executes_multi_statement_batch() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;

    // Act
    (&mut connection)
        .execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT); \
             INSERT INTO test (name) VALUES ('alice'); \
             INSERT INTO test (name) VALUES ('bob')",
        )
        .await?;

    // Assert
    let rows = (&mut connection)
        .fetch_all("SELECT name FROM test ORDER BY id")
        .await?;
    assert_eq!(rows.len(), 2);
    Ok(())
}

#[tokio::test]
async fn parser_splits_original_batch_text_with_unicode_literals_comments_and_trigger_bodies()
-> sqlx_core::Result<()> {
    let mut connection = memory_connection().await?;
    (&mut connection)
        .execute(
            "CREATE TABLE items(id INTEGER PRIMARY KEY, text TEXT); \
         CREATE TABLE notes(text TEXT); \
         CREATE TRIGGER note_insert AFTER INSERT ON items BEGIN \
           INSERT INTO notes(text) VALUES('a;雪'); \
           INSERT INTO notes(text) VALUES('it''s; valid'); \
         END; /* not; a statement */ \
         INSERT INTO items(id,text) VALUES(1,'escaped;☃'); -- final; comment",
        )
        .await?;
    let texts: Vec<String> =
        sqlx_core::query_scalar::query_scalar("SELECT text FROM notes ORDER BY rowid")
            .fetch_all(&mut connection)
            .await?;
    assert_eq!(texts, vec!["a;雪", "it's; valid"]);
    let text: String = sqlx_core::query_scalar::query_scalar("SELECT text FROM items WHERE id=1")
        .fetch_one(&mut connection)
        .await?;
    assert_eq!(text, "escaped;☃");
    Ok(())
}

#[tokio::test]
async fn a_later_batch_parse_error_preserves_its_executed_prefix() -> sqlx_core::Result<()> {
    let mut connection = memory_connection().await?;
    (&mut connection)
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")
        .await?;
    let failure = (&mut connection)
        .execute("INSERT INTO items(id) VALUES(1); definitely not SQL")
        .await;
    assert!(failure.is_err());
    let count: i64 = sqlx_core::query_scalar::query_scalar("SELECT count(*) FROM items")
        .fetch_one(&mut connection)
        .await?;
    assert_eq!(
        count, 1,
        "parsing a later statement must not prevalidate away an earlier committed effect"
    );
    Ok(())
}

#[tokio::test]
async fn rejects_arguments_for_multi_statement_sql() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;

    // Act
    let error = query::<Turso>("SELECT ?; SELECT 2")
        .bind(1_i64)
        .fetch_all(&mut connection)
        .await
        .expect_err("batches take no arguments");

    // Assert
    assert_eq!(
        adapter_error(&error),
        Some(&TursoAdapterError::BatchArgumentsUnsupported)
    );
    Ok(())
}

#[tokio::test]
async fn optional_fetch_returns_none_for_no_rows() -> sqlx_core::Result<()> {
    let mut connection = memory_connection().await?;

    let row = (&mut connection)
        .fetch_optional("SELECT 1 WHERE false")
        .await?;

    assert!(row.is_none());
    Ok(())
}

#[tokio::test]
async fn semicolon_inside_string_stays_single_statement() -> sqlx_core::Result<()> {
    let mut connection = memory_connection().await?;

    let rows = (&mut connection).fetch_all("SELECT ';'").await?;

    assert_eq!(rows.len(), 1);
    Ok(())
}

#[tokio::test]
async fn binds_anonymous_and_numbered_arguments() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;

    // Act
    let row =
        query::<Turso>("SELECT ? AS anonymous, ?2 AS numbered_question, $3 AS numbered_dollar")
            .bind(11_i64)
            .bind(22_i64)
            .bind(33_i64)
            .fetch_one(&mut connection)
            .await?;

    // Assert
    assert_eq!(row.try_get::<i64, _>("anonymous")?, 11);
    assert_eq!(row.try_get::<i64, _>("numbered_question")?, 22);
    assert_eq!(row.try_get::<i64, _>("numbered_dollar")?, 33);
    Ok(())
}

#[tokio::test]
async fn rejects_non_integer_named_placeholders() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;

    // Act
    let error = query::<Turso>("SELECT $name")
        .bind(11_i64)
        .fetch_one(&mut connection)
        .await
        .expect_err("named placeholder should be rejected");

    // Assert
    assert_eq!(
        adapter_error(&error),
        Some(&TursoAdapterError::NamedPlaceholderUnsupported {
            placeholder: "$name".to_owned()
        })
    );
    Ok(())
}

#[tokio::test]
async fn round_trips_basic_storage_classes() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;

    // Act
    let row = query::<Turso>(
        "SELECT ? AS int_value, ? AS bool_value, ? AS real_value, \
         ? AS text_value, ? AS blob_value, ? AS null_value",
    )
    .bind(42_i64)
    .bind(true)
    .bind(3.5_f64)
    .bind("hello")
    .bind(vec![1_u8, 2, 3])
    .bind(Option::<i64>::None)
    .fetch_one(&mut connection)
    .await?;

    // Assert
    assert_eq!(row.try_get::<u32, _>("int_value")?, 42);
    assert!(row.try_get::<bool, _>("bool_value")?);
    assert_eq!(row.try_get::<f64, _>("real_value")?, 3.5);
    assert_eq!(row.try_get::<String, _>("text_value")?, "hello");
    assert_eq!(row.try_get::<Vec<u8>, _>("blob_value")?, [1, 2, 3]);
    assert_eq!(row.try_get::<Option<i64>, _>("null_value")?, None);
    Ok(())
}

#[tokio::test]
async fn decoding_the_wrong_storage_class_fails() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;
    let row = (&mut connection)
        .fetch_one("SELECT 'text' AS value")
        .await?;

    // Act: skip SQLx's declared-type check so the value's storage class is what fails
    let error = row
        .try_get_unchecked::<i64, _>("value")
        .expect_err("text is not an integer");

    // Assert
    let Error::ColumnDecode { source, .. } = &error else {
        return Err(error);
    };
    assert!(matches!(
        source.downcast_ref::<TursoAdapterError>(),
        Some(TursoAdapterError::StorageClassMismatch { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn checked_unsigned_decode_rejects_negative_values() -> sqlx_core::Result<()> {
    let mut connection = memory_connection().await?;

    let row = (&mut connection).fetch_one("SELECT -1 AS value").await?;

    assert!(row.try_get::<u32, _>("value").is_err());
    Ok(())
}

#[tokio::test]
async fn declared_type_affinity_drives_decode_compatibility() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;
    (&mut connection)
        .execute(
            "CREATE TABLE test (\
             id BIGINT, \
             flag BOOLEAN, \
             label VARCHAR(20), \
             payload BLOB, \
             score DOUBLE PRECISION\
             )",
        )
        .await?;
    query::<Turso>("INSERT INTO test (id, flag, label, payload, score) VALUES (?, ?, ?, ?, ?)")
        .bind(7_i64)
        .bind(true)
        .bind("alice")
        .bind(vec![9_u8, 8, 7])
        .bind(4.25_f64)
        .execute(&mut connection)
        .await?;

    // Act
    let row = (&mut connection)
        .fetch_one("SELECT id, flag, label, payload, score FROM test")
        .await?;

    // Assert
    assert_eq!(row.try_get::<i64, _>("id")?, 7);
    assert!(row.try_get::<bool, _>("flag")?);
    assert_eq!(row.try_get::<String, _>("label")?, "alice");
    assert_eq!(row.try_get::<Vec<u8>, _>("payload")?, [9, 8, 7]);
    assert_eq!(row.try_get::<f64, _>("score")?, 4.25);
    Ok(())
}

#[tokio::test]
async fn describe_leaves_parameter_metadata_unknown() -> sqlx_core::Result<()> {
    let mut connection = memory_connection().await?;

    let describe = (&mut connection)
        .describe(SqlStr::from_static("SELECT ? AS value"))
        .await?;

    assert!(describe.parameters().is_none());
    assert_eq!(describe.columns().len(), 1);
    Ok(())
}

#[tokio::test]
async fn row_and_statement_name_lookup_use_same_metadata() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;
    (&mut connection)
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .await?;
    (&mut connection)
        .execute("INSERT INTO test (id, name) VALUES (1, 'alice')")
        .await?;
    let sql = SqlStr::from_static("SELECT id, name FROM test WHERE id = 1");

    // Act
    let statement = (&mut connection).prepare(sql.clone()).await?;
    let row = (&mut connection).fetch_one(sql).await?;

    // Assert
    assert_eq!(statement.column("name").ordinal(), 1);
    assert!(statement.try_column("missing").is_err());
    assert_eq!(row.try_get::<String, _>("name")?, "alice");
    assert!(row.try_get_raw("missing").is_err());
    Ok(())
}

#[tokio::test]
async fn maps_turso_database_errors_to_sqlx_database_errors() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;
    (&mut connection)
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        .await?;
    (&mut connection)
        .execute("INSERT INTO test (id) VALUES (1)")
        .await?;

    // Act
    let error = (&mut connection)
        .execute("INSERT INTO test (id) VALUES (1)")
        .await
        .expect_err("duplicate primary key should fail");

    // Assert
    let database_error = error
        .as_database_error()
        .expect("Turso errors should map to SQLx database errors");
    assert_eq!(database_error.code().as_deref(), Some("SQLITE_CONSTRAINT"));
    assert_eq!(database_error.kind(), ErrorKind::UniqueViolation);
    Ok(())
}

#[tokio::test]
async fn a_not_null_violation_on_a_column_named_like_a_constraint_is_not_null()
-> sqlx_core::Result<()> {
    // Arrange: column names that contain other constraint words
    let mut connection = memory_connection().await?;
    (&mut connection)
        .execute(
            "CREATE TABLE accounts (\
             id INTEGER PRIMARY KEY, \
             unique_key TEXT NOT NULL, \
             primary_account_id INTEGER NOT NULL)",
        )
        .await?;

    // Act
    let error = (&mut connection)
        .execute("INSERT INTO accounts (id, unique_key, primary_account_id) VALUES (1, NULL, 7)")
        .await
        .expect_err("unique_key is NOT NULL");

    // Assert
    let database_error = error
        .as_database_error()
        .expect("Turso errors should map to SQLx database errors");
    assert_eq!(
        database_error.kind(),
        ErrorKind::NotNullViolation,
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn supports_builtin_regexp_operator() -> sqlx_core::Result<()> {
    let mut connection = memory_connection().await?;

    let row = (&mut connection)
        .fetch_one("SELECT 'alphabet' REGEXP '^alpha' AS matched")
        .await?;

    assert_eq!(row.try_get::<i64, _>("matched")?, 1);
    Ok(())
}

#[tokio::test]
async fn serializes_describe_metadata_for_offline_mode() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;
    (&mut connection)
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .await?;
    let describe = (&mut connection)
        .describe("SELECT id, name FROM test ORDER BY id".into_sql_str())
        .await?;

    // Act
    let serialized = serde_json::to_string(&describe).map_err(Error::config)?;
    let deserialized: sqlx_core::describe::Describe<Turso> =
        serde_json::from_str(&serialized).map_err(Error::config)?;

    // Assert
    assert_eq!(deserialized.columns().len(), 2);
    assert_eq!(deserialized.columns()[0].name(), "id");
    assert_eq!(
        sqlx_core::type_info::TypeInfo::name(deserialized.columns()[1].type_info()),
        "TEXT"
    );
    Ok(())
}

#[cfg(feature = "chrono")]
#[tokio::test]
async fn round_trips_chrono_values() -> sqlx_core::Result<()> {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

    // Arrange
    let mut connection = memory_connection().await?;
    let date = NaiveDate::from_ymd_opt(2026, 5, 26).expect("valid date");
    let time = NaiveTime::from_hms_micro_opt(14, 30, 15, 123_000).expect("valid time");
    let datetime = NaiveDateTime::new(date, time);
    let utc: DateTime<Utc> = datetime.and_utc();

    // Act
    let row = query::<Turso>(
        "SELECT ? AS date_value, ? AS time_value, ? AS datetime_value, ? AS utc_value",
    )
    .bind(date)
    .bind(time)
    .bind(datetime)
    .bind(utc)
    .fetch_one(&mut connection)
    .await?;

    // Assert
    assert_eq!(row.try_get::<NaiveDate, _>("date_value")?, date);
    assert_eq!(row.try_get::<NaiveTime, _>("time_value")?, time);
    assert_eq!(row.try_get::<NaiveDateTime, _>("datetime_value")?, datetime);
    assert_eq!(row.try_get::<DateTime<Utc>, _>("utc_value")?, utc);
    assert_eq!(
        row.try_get::<String, _>("utc_value")?,
        "2026-05-26T14:30:15.123+00:00"
    );
    Ok(())
}

#[tokio::test]
async fn dropped_stream_allows_connection_reuse() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = memory_connection().await?;
    (&mut connection)
        .execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY); \
             INSERT INTO test (id) VALUES (1); \
             INSERT INTO test (id) VALUES (2)",
        )
        .await?;

    // Act
    let mut rows = (&mut connection).fetch("SELECT id FROM test ORDER BY id");
    assert!(rows.try_next().await?.is_some());
    drop(rows);
    let row = (&mut connection).fetch_one("SELECT 3").await?;

    // Assert
    assert!(!row.try_get_raw(0)?.is_null());
    Ok(())
}

#[tokio::test]
async fn dropped_unpolled_stream_does_not_execute_sql() -> sqlx_core::Result<()> {
    let mut connection = memory_connection().await?;

    let rows = (&mut connection).fetch("CREATE TABLE test (id INTEGER PRIMARY KEY)");
    drop(rows);

    (&mut connection)
        .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        .await?;
    Ok(())
}

#[tokio::test]
async fn bounds_and_clears_statement_cache() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = TursoConnectOptions::new()
        .statement_cache_capacity(2)
        .connect()
        .await?;

    // Act and Assert
    (&mut connection)
        .prepare(SqlStr::from_static("SELECT 1"))
        .await?;
    (&mut connection)
        .prepare(SqlStr::from_static("SELECT 2"))
        .await?;
    assert_eq!(connection.cached_statements_size(), 2);

    (&mut connection)
        .prepare(SqlStr::from_static("SELECT 3"))
        .await?;
    assert_eq!(connection.cached_statements_size(), 2);

    connection.clear_cached_statements().await?;
    assert_eq!(connection.cached_statements_size(), 0);
    Ok(())
}

/// Keeps everything a `tracing` subscriber writes, so a test can assert on emitted logs
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn text(&self) -> String {
        self.0
            .lock()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_default()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_poisoned| std::io::Error::other("log buffer poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedLogs;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn statement_logging_follows_the_connection_settings() -> sqlx_core::Result<()> {
    // Arrange: a slow threshold of zero marks every statement slow; the other connection logs
    // nothing at all
    let logs = CapturedLogs::default();
    // Only SQLx's statement logs: the engine traces SQL text through its own targets
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_env_filter(tracing_subscriber::EnvFilter::new("sqlx::query=trace"))
        .with_ansi(false)
        .finish();
    let _default_subscriber = tracing::subscriber::set_default(subscriber);
    let slow_options =
        TursoConnectOptions::new().log_slow_statements(log::LevelFilter::Warn, Duration::ZERO);
    let quiet_options = TursoConnectOptions::new()
        .log_statements(log::LevelFilter::Off)
        .log_slow_statements(log::LevelFilter::Off, Duration::from_secs(3_600));

    // Act
    let mut quiet = quiet_options.connect().await?;
    (&mut quiet).fetch_one("SELECT 2 AS quiet_marker").await?;
    let mut slow = slow_options.connect().await?;
    (&mut slow).fetch_one("SELECT 1 AS slow_marker").await?;

    // Assert
    let text = logs.text();
    assert!(text.contains("slow statement"), "{text}");
    assert!(text.contains("slow_marker"), "{text}");
    assert!(!text.contains("quiet_marker"), "{text}");
    Ok(())
}
