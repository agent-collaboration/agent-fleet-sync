#![allow(clippy::panic_in_result_fn)] // Assertions decide behavior; Result propagates fixture failures.

use fleet_sync_protocol::{RowChange, SqlValue};
use fleet_sync_writer::{CaptureChange, TableLayout, normalize_capture_change};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn capture_changes(
    connection: &turso::Connection,
    layout: &TableLayout,
) -> TestResult<Vec<RowChange>> {
    let mut rows = connection
        .query(
            "SELECT change_type,before,after FROM turso_cdc ORDER BY change_id",
            (),
        )
        .await?;
    let mut changes = Vec::new();
    while let Some(row) = rows.next().await? {
        let before = row.get::<Option<Vec<u8>>>(1)?;
        let after = row.get::<Option<Vec<u8>>>(2)?;
        let change = match row.get::<i64>(0)? {
            -1 => CaptureChange::Delete {
                before: before.ok_or("before")?,
            },
            0 => CaptureChange::Update {
                before: before.ok_or("before")?,
                after: after.ok_or("after")?,
            },
            1 => CaptureChange::Insert { before, after },
            2 => continue,
            _ => return Err("unknown capture type".into()),
        };
        changes.extend(normalize_capture_change(layout, change)?);
    }
    Ok(changes)
}

#[tokio::test]
async fn replace_captures_each_implicit_delete_in_all_eighteen_conflicts() -> TestResult {
    for (declaration, key_indices, integer_alias) in [
        (
            "key_text TEXT PRIMARY KEY,key_number INTEGER,slot TEXT UNIQUE",
            vec![0],
            None,
        ),
        (
            "key_text TEXT,key_number INTEGER,slot TEXT UNIQUE,PRIMARY KEY(key_text,key_number)",
            vec![0, 1],
            None,
        ),
        (
            "key_text TEXT,key_number INTEGER PRIMARY KEY,slot TEXT UNIQUE",
            vec![1],
            Some(1),
        ),
    ] {
        for syntax in ["INSERT OR REPLACE INTO", "REPLACE INTO"] {
            for (offered, expected_deleted) in [
                ("('a',10,'z')", 1),
                ("('c',30,'x')", 1),
                ("('a',10,'y')", 2),
            ] {
                // Arrange: two rows so secondary conflicts can remove another key.
                let directory = tempfile::tempdir()?;
                let path = directory.path().join("writer.db");
                let database = turso::Builder::new_local(path.to_str().ok_or("path")?)
                    .build()
                    .await?;
                let connection = database.connect()?;
                connection
                    .execute(&format!("CREATE TABLE records({declaration})"), ())
                    .await?;
                connection
                    .execute("INSERT INTO records VALUES('a',10,'x'),('b',20,'y')", ())
                    .await?;
                connection
                    .execute("PRAGMA capture_data_changes_conn('full')", ())
                    .await?;
                let layout = TableLayout::new(
                    "records".into(),
                    ["key_text", "key_number", "slot"]
                        .map(str::to_owned)
                        .to_vec(),
                    key_indices.clone(),
                    integer_alias,
                )?;

                // Act: a real conflict, followed by the production normalizer.
                connection.execute("BEGIN IMMEDIATE", ()).await?;
                connection
                    .execute(&format!("{syntax} records VALUES {offered}"), ())
                    .await?;
                connection.execute("COMMIT", ()).await?;
                let changes = capture_changes(&connection, &layout).await?;

                // Assert: each discarded row is captured, before the new upsert.
                let deleted = changes
                    .iter()
                    .take_while(|change| matches!(change, RowChange::Delete { .. }))
                    .count();
                assert_eq!(
                    deleted, expected_deleted,
                    "{declaration} {syntax} {offered}"
                );
                assert_eq!(changes.len(), expected_deleted + 1);
                assert!(matches!(changes.last(), Some(RowChange::Upsert { .. })));
                let mut expected_keys =
                    vec![vec![SqlValue::Text("a".into()), SqlValue::Integer(10)]];
                if expected_deleted == 2 {
                    expected_keys.push(vec![SqlValue::Text("b".into()), SqlValue::Integer(20)]);
                }
                for key_values in expected_keys {
                    let key = key_indices
                        .iter()
                        .map(|index| key_values.get(*index).cloned().ok_or("key index"))
                        .collect::<Result<Vec<_>, _>>()?;
                    assert!(changes.contains(&RowChange::Delete {
                        table: "records".into(),
                        key
                    }));
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn pinned_upsert_reproduction_detects_when_the_upstream_workaround_can_be_removed()
-> TestResult {
    for (key_declaration, key_indices, assignment, expected_key) in [
        (
            "key_text TEXT PRIMARY KEY,key_number INTEGER,slot TEXT UNIQUE",
            vec![0],
            "key_text=excluded.key_text",
            vec![SqlValue::Text("c".into())],
        ),
        (
            "key_text TEXT,key_number INTEGER,slot TEXT UNIQUE,PRIMARY KEY(key_text,key_number)",
            vec![0, 1],
            "key_number=excluded.key_number",
            vec![SqlValue::Text("a".into()), SqlValue::Integer(30)],
        ),
    ] {
        // This diagnostic intentionally calls Turso directly to expose its pinned defect.
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("writer.db");
        let database = turso::Builder::new_local(path.to_str().ok_or("path")?)
            .build()
            .await?;
        let connection = database.connect()?;
        connection
            .execute(&format!("CREATE TABLE records({key_declaration})"), ())
            .await?;
        connection
            .execute("INSERT INTO records VALUES('a',10,'x')", ())
            .await?;
        connection
            .execute("PRAGMA capture_data_changes_conn('full')", ())
            .await?;
        connection.execute(&format!("INSERT INTO records VALUES('c',30,'x') ON CONFLICT(slot) DO UPDATE SET {assignment}"),()).await?;
        let mut rows = connection
            .query("SELECT before,after FROM turso_cdc WHERE change_type=0", ())
            .await?;
        let row = rows.next().await?.ok_or("missing update")?;
        assert_eq!(
            row.get::<Vec<u8>>(0)?,
            row.get::<Vec<u8>>(1)?,
            "upstream behavior changed: reassess the refusal before updating this oracle"
        );
        let layout = TableLayout::new(
            "records".into(),
            ["key_text", "key_number", "slot"]
                .map(str::to_owned)
                .to_vec(),
            key_indices,
            None,
        )?;
        let changes = capture_changes(&connection, &layout).await?;
        assert_eq!(
            changes.len(),
            1,
            "upstream behavior changed: a correct old-key delete is now possible"
        );
        assert!(matches!(changes.first(),Some(RowChange::Upsert {key,..}) if *key==expected_key));
    }
    Ok(())
}
