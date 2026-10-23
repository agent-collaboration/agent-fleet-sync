//! Local hook tables stay outside the copy's replicated schema and snapshot.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use fleet_sync_protocol::*;
use sqlx_core::{connection::Connection, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn schema_membership_indexes_and_views_roll_back_with_a_refused_hook() -> TestResult {
    let mut connection = connection().await?;
    let statements = vec![
        "CREATE TABLE extra(id TEXT NOT NULL PRIMARY KEY, body TEXT)".to_owned(),
        "CREATE INDEX extra_body ON extra(body)".to_owned(),
        "CREATE VIEW extra_view AS SELECT id,body FROM extra".to_owned(),
    ];
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::SchemaStep {
            commit_id: position(1)?.sequence,
            migration_version: 2,
            statements: statements.clone(),
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let result = apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &writer()?,
        &Hook { refuse: true },
        SyncTime::from_unix_millis(1),
    )
    .await;
    assert!(matches!(
        result,
        Err(ApplyFailure::Refused(PieceRefusal::Hook(_)))
    ));
    transaction.rollback().await?;
    let row = sqlx_core::query::query::<Turso>("SELECT CAST(count(*) AS TEXT) AS objects FROM sqlite_schema WHERE name IN ('extra','extra_body','extra_view')").fetch_one(&mut connection).await?;
    assert_eq!(row.try_get::<String, _>("objects")?, "0");
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(2),
    )
    .await?;
    sqlx_core::query::query::<Turso>("CREATE INDEX local_hook_index ON hook_records(value)")
        .execute(&mut *transaction)
        .await?;
    let expected = schema_fingerprint(&[
        SchemaEntry {
            kind: SchemaObjectKind::Table,
            name: "records".into(),
            sql: "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT)".into(),
        },
        SchemaEntry {
            kind: SchemaObjectKind::Table,
            name: "extra".into(),
            sql: statements.first().ok_or("table SQL")?.clone(),
        },
        SchemaEntry {
            kind: SchemaObjectKind::Index,
            name: "extra_body".into(),
            sql: statements.get(1).ok_or("index SQL")?.clone(),
        },
        SchemaEntry {
            kind: SchemaObjectKind::View,
            name: "extra_view".into(),
            sql: statements.get(2).ok_or("view SQL")?.clone(),
        },
    ])?;
    let status = ready_status(copy_status(&mut TursoScope(&mut transaction)).await?)?;
    assert_eq!(status.schema.fingerprint, expected);
    assert_eq!(
        crate::schema_reads::SchemaReads(&mut TursoScope(&mut transaction))
            .fingerprint()
            .await?,
        expected
    );
    let SnapshotManifestAnswer::Manifest(manifest) =
        snapshot_manifest(&mut TursoScope(&mut transaction), status.acked).await?
    else {
        return Err("missing manifest".into());
    };
    assert_eq!(
        manifest
            .tables
            .iter()
            .map(|table| table.table.as_str())
            .collect::<Vec<_>>(),
        vec!["extra", "records"]
    );
    assert_eq!(
        snapshot_manifest(&mut TursoScope(&mut transaction), position(0)?).await?,
        SnapshotManifestAnswer::Moved {
            position: position(1)?
        }
    );
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn hook_table_exists_but_never_enters_snapshot_pages_or_fingerprint() -> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_core::query::query::<Turso>("INSERT INTO hook_records(value) VALUES('local only')")
        .execute(&mut *transaction)
        .await?;
    let status = ready_status(copy_status(&mut TursoScope(&mut transaction)).await?)?;
    let expected = schema_fingerprint(&[SchemaEntry {
        kind: SchemaObjectKind::Table,
        name: "records".into(),
        sql: "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT)".into(),
    }])?;
    assert_eq!(status.schema.fingerprint, expected);
    let SnapshotRead::Page(page) = snapshot_page(&mut TursoScope(&mut transaction), None).await?
    else {
        return Err("unexpected moved snapshot".into());
    };
    assert_eq!(page.table, "records");
    assert!(page.rows.is_empty());
    assert!(page.next.is_none());
    let SnapshotManifestAnswer::Manifest(manifest) =
        snapshot_manifest(&mut TursoScope(&mut transaction), status.acked).await?
    else {
        return Err("manifest unavailable".into());
    };
    assert_eq!(
        manifest.tables,
        vec![TableManifest {
            table: TableName::new("records")?,
            row_count: 0,
            digest: TableDigestBuilder::new().finish()
        }]
    );
    assert_eq!(manifest.schema.fingerprint, expected);
    initialize_hook(&mut TursoScope(&mut transaction), &Hook { refuse: false }).await?;
    let row = sqlx_core::query::query::<Turso>("SELECT value FROM hook_records")
        .fetch_one(&mut *transaction)
        .await?;
    assert_eq!(row.try_get::<String, _>("value")?, "local only");
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn copy_snapshot_header_uses_small_current_catalog_after_large_schema_history() -> TestResult
{
    let mut connection = connection().await?;
    // Each piece commits a real schema edit whose transient table is gone again.
    // Its retained history is deliberately larger than the current-catalog cap.
    for version in 2_u64..=13 {
        let incoming = Piece::<()>::new(PieceContents {
            database: database()?,
            epoch: Epoch::new(1)?,
            continues_from: position(version - 2)?,
            through: position(version - 1)?,
            transactions: vec![ReplicatedTransaction::SchemaStep {
                commit_id: CommitSequence::new(version),
                migration_version: version,
                statements: vec![
                    format!(
                        "CREATE TABLE discarded_{version}(id TEXT NOT NULL PRIMARY KEY,payload TEXT DEFAULT '{}')",
                        "x".repeat(32 * 1024)
                    ),
                    format!("DROP TABLE discarded_{version}"),
                ],
            }],
            part: None,
            annotation: None,
        })?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1),
        )
        .await?;
        transaction.commit().await?;
    }
    let index = "CREATE INDEX records_body ON records(body)";
    let view = "CREATE VIEW records_view AS SELECT id,body FROM records";
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(12)?,
        through: position(13)?,
        transactions: vec![ReplicatedTransaction::SchemaStep {
            commit_id: CommitSequence::new(14),
            migration_version: 14,
            statements: vec![index.into(), view.into()],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(2),
    )
    .await?;
    sqlx_core::query::query::<Turso>("CREATE INDEX hook_private ON hook_records(value)")
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;

    // Act through the public copy-page producer in a fresh real transaction.
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let history: i64 = sqlx_core::query::query::<Turso>(
        "SELECT SUM(length(record)) AS encoded FROM _fleet_sync_schema_steps",
    )
    .fetch_one(&mut *transaction)
    .await?
    .try_get("encoded")?;
    assert!(history > 256 * 1024);
    let SnapshotRead::Page(page) = snapshot_page(&mut TursoScope(&mut transaction), None).await?
    else {
        return Err("snapshot moved without a source write".into());
    };

    assert_eq!(
        page.schema_steps.len(),
        3,
        "snapshot header replayed retired schema history"
    );
    assert!(serde_json::to_vec(&page.schema_steps)?.len() < 1024);
    let entries = page
        .schema_steps
        .iter()
        .map(|sql| snapshot_catalog_entry(sql))
        .collect::<TestResult<Vec<_>>>()?;
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.kind, entry.name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (SchemaObjectKind::Table, "records"),
            (SchemaObjectKind::Index, "records_body"),
            (SchemaObjectKind::View, "records_view"),
        ]
    );
    let expected = schema_fingerprint(&[
        SchemaEntry {
            kind: SchemaObjectKind::Table,
            name: "records".into(),
            sql: "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT)".into(),
        },
        SchemaEntry {
            kind: SchemaObjectKind::Index,
            name: "records_body".into(),
            sql: index.into(),
        },
        SchemaEntry {
            kind: SchemaObjectKind::View,
            name: "records_view".into(),
            sql: view.into(),
        },
    ])?;
    assert_eq!(schema_fingerprint(&entries)?, expected);
    assert_eq!(page.schema.fingerprint, expected);
    assert_eq!(
        page.table, "records",
        "view was treated as a row-bearing table"
    );
    assert!(page.next.is_none());
    transaction.commit().await?;
    Ok(())
}

fn snapshot_catalog_entry(sql: &str) -> TestResult<SchemaEntry> {
    use turso_parser::ast::{Cmd, Stmt};
    let command = turso_parser::parser::Parser::new(sql.as_bytes())
        .next_cmd()?
        .ok_or("empty snapshot catalog statement")?;
    let (kind, name) = match command {
        Cmd::Stmt(Stmt::CreateTable { tbl_name, .. }) => (SchemaObjectKind::Table, tbl_name.name),
        Cmd::Stmt(Stmt::CreateIndex { idx_name, .. }) => (SchemaObjectKind::Index, idx_name.name),
        Cmd::Stmt(Stmt::CreateView { view_name, .. }) => (SchemaObjectKind::View, view_name.name),
        _ => return Err("snapshot header contains a historical or unsupported statement".into()),
    };
    Ok(SchemaEntry {
        kind,
        name: name.as_str().into(),
        sql: sql.into(),
    })
}

#[tokio::test]
async fn imported_over_bound_catalog_has_a_typed_snapshot_schema_refusal() -> TestResult {
    let mut connection = connection().await?;
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::SchemaStep {
            commit_id: CommitSequence::new(2),
            migration_version: 2,
            statements: vec![format!(
                "ALTER TABLE records ADD COLUMN escaped_default TEXT DEFAULT '{}'",
                "\"".repeat(150 * 1024)
            )],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(1),
    )
    .await?;
    transaction.commit().await?;
    let actual: String =
        sqlx_core::query::query::<Turso>("SELECT sql FROM sqlite_schema WHERE name='records'")
            .fetch_one(&mut connection)
            .await?
            .try_get("sql")?;
    assert!(
        actual.len() < 256 * 1024,
        "fixture must require encoded-byte accounting"
    );
    let expected_size = serde_json::to_vec(&vec![actual])?.len();
    assert!(expected_size > 256 * 1024);
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(
        matches!(snapshot_page(&mut TursoScope(&mut transaction), None).await,
        Err(ReceiverError::SnapshotSchema { encoded_size }) if encoded_size == expected_size)
    );
    assert!(
        matches!(snapshot_manifest(&mut TursoScope(&mut transaction), position(1)?).await,
        Err(ReceiverError::SnapshotSchema { encoded_size }) if encoded_size == expected_size)
    );
    transaction.rollback().await?;
    Ok(())
}
