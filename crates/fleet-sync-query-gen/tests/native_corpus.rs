#![allow(clippy::panic_in_result_fn)]
#![cfg(feature = "native")]
use fleet_sync_query_gen::*;
use std::{fs, path::Path};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn corpus(
    root: &Path,
    sql: &str,
    parameters: Vec<ParameterSpec>,
) -> TestResult<std::path::PathBuf> {
    fs::create_dir_all(root.join("migrations"))?;
    fs::create_dir_all(root.join("sql"))?;
    fs::write(
        root.join("migrations/1_example.sql"),
        "CREATE TABLE example(record_id TEXT PRIMARY KEY, amount INTEGER NOT NULL, ratio REAL, payload BLOB);",
    )?;
    fs::write(root.join("sql/read_example.sql"), sql)?;
    let manifest = QueryManifest {
        migrations: vec!["migrations".into()],
        output: "src/checked_queries.rs".into(),
        metadata: "checked-query-metadata.json".into(),
        queries: vec![QuerySpec {
            name: "read_example".into(),
            sql: "sql/read_example.sql".into(),
            row: Some("StoredExampleRow".into()),
            result: QueryResult::Many,
            parameters,
        }],
    };
    let path = root.join("checked-queries.json");
    fs::write(&path, serde_json::to_vec_pretty(&manifest)?)?;
    Ok(path)
}

#[tokio::test]
async fn real_describe_keeps_storage_types_and_freshness_rejects_changed_inputs() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let manifest = corpus(
        temporary.path(),
        "SELECT record_id, amount, ratio, payload FROM example",
        vec![],
    )?;
    generate_manifest(&manifest, false).await?;
    generate_manifest(&manifest, true).await?;
    verify_manifest(&manifest)?;
    let metadata: CorpusMetadata = serde_json::from_slice(&fs::read(
        temporary.path().join("checked-query-metadata.json"),
    )?)?;
    let columns = &metadata
        .queries
        .first()
        .ok_or("missing described query")?
        .columns;
    assert_eq!(
        columns
            .iter()
            .map(|column| (column.name.as_str(), column.sql_type.as_str()))
            .collect::<Vec<_>>(),
        [
            ("record_id", "TEXT"),
            ("amount", "INTEGER"),
            ("ratio", "REAL"),
            ("payload", "BLOB")
        ]
    );
    assert!(columns.iter().all(|column| column.nullable == Some(false) || column.rust_type.starts_with("Option<")));
    fs::write(
        temporary.path().join("sql/read_example.sql"),
        "SELECT amount FROM example",
    )?;
    assert!(
        matches!(verify_manifest(&manifest), Err(GenerateError::Stale(path)) if path == "sql/read_example.sql")
    );
    generate_manifest(&manifest, false).await?;
    fs::write(
        temporary.path().join("migrations/2_later.sql"),
        "ALTER TABLE example ADD COLUMN later TEXT;",
    )?;
    assert!(
        matches!(verify_manifest(&manifest), Err(GenerateError::Stale(path)) if path == "migrations/2_later.sql")
    );
    Ok(())
}

#[tokio::test]
async fn unknown_result_types_and_incorrect_bind_arity_are_refused() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let manifest = corpus(
        temporary.path(),
        "SELECT NULL AS mystery FROM example",
        vec![],
    )?;
    assert!(
        matches!(generate_manifest(&manifest, false).await, Err(GenerateError::UnsupportedType { column, .. }) if column == "mystery")
    );
    fs::write(
        temporary.path().join("sql/read_example.sql"),
        "SELECT record_id FROM example WHERE record_id = ?",
    )?;
    assert!(matches!(
        generate_manifest(&manifest, false).await,
        Err(GenerateError::BindCount {
            expected: 0,
            actual: 1,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn changed_generated_output_cannot_pass_the_lightweight_build_gate() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let manifest = corpus(temporary.path(), "SELECT record_id FROM example", vec![])?;
    generate_manifest(&manifest, false).await?;
    fs::write(
        temporary.path().join("src/checked_queries.rs"),
        "// corrupt generated result\n",
    )?;
    assert!(
        matches!(verify_manifest(&manifest), Err(GenerateError::Stale(path)) if path == "src/checked_queries.rs")
    );
    assert!(
        matches!(generate_manifest(&manifest, true).await, Err(GenerateError::Stale(path)) if path == "src/checked_queries.rs")
    );
    Ok(())
}

#[tokio::test]
async fn closed_typeof_and_cast_expressions_are_checked_without_result_overrides() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let manifest = corpus(
        temporary.path(),
        "SELECT typeof(amount) AS amount_class, CAST(amount AS TEXT) AS amount_text, CAST(amount AS INTEGER) AS amount_integer, CAST(amount AS REAL) AS amount_real, CAST(amount AS BLOB) AS amount_blob FROM example",
        vec![],
    )?;
    generate_manifest(&manifest, false).await?;
    let metadata: CorpusMetadata = serde_json::from_slice(&fs::read(
        temporary.path().join("checked-query-metadata.json"),
    )?)?;
    let columns = &metadata.queries.first().ok_or("missing query")?.columns;
    assert_eq!(
        columns
            .iter()
            .map(|column| column.sql_type.as_str())
            .collect::<Vec<_>>(),
        ["TEXT", "TEXT", "INTEGER", "REAL", "BLOB"]
    );
    fs::write(
        temporary.path().join("sql/read_example.sql"),
        "SELECT length(record_id) AS unrelated_function FROM example",
    )?;
    assert!(
        matches!(generate_manifest(&manifest, false).await, Err(GenerateError::UnsupportedType { column, .. }) if column == "unrelated_function")
    );
    fs::write(
        temporary.path().join("sql/read_example.sql"),
        "SELECT CAST(amount AS NUMERIC) AS unsupported_cast FROM example",
    )?;
    assert!(
        matches!(generate_manifest(&manifest, false).await, Err(GenerateError::UnsupportedType { column, .. }) if column == "unsupported_cast")
    );
    Ok(())
}

#[tokio::test]
async fn existence_mode_prepares_exact_sql_without_interpreting_columns() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let manifest = corpus(temporary.path(), "PRAGMA foreign_key_check", vec![])?;
    let mut specification: QueryManifest = serde_json::from_slice(&fs::read(&manifest)?)?;
    let query = specification.queries.first_mut().ok_or("missing query")?;
    query.result = QueryResult::Exists;
    query.row = None;
    fs::write(&manifest, serde_json::to_vec_pretty(&specification)?)?;
    generate_manifest(&manifest, false).await?;
    let generated = fs::read_to_string(temporary.path().join("src/checked_queries.rs"))?;
    assert!(generated.contains("Result<bool, sqlx_core::Error>"));
    assert!(!generated.contains("try_get"));
    fs::write(
        temporary.path().join("sql/read_example.sql"),
        "SELECT NULL FROM example",
    )?;
    generate_manifest(&manifest, false).await?;
    fs::write(
        temporary.path().join("sql/read_example.sql"),
        "SELECT NULL FROM misspelled_example",
    )?;
    assert!(matches!(
        generate_manifest(&manifest, false).await,
        Err(GenerateError::Sql(_))
    ));
    Ok(())
}
