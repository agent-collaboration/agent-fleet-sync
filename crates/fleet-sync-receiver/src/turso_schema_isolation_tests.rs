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
        "CREATE TABLE extra(id INTEGER PRIMARY KEY, body TEXT)".to_owned(),
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
            sql: "CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT)".into(),
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
        sql: "CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT)".into(),
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
