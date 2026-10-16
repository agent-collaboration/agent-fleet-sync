#![allow(clippy::panic_in_result_fn)] // Assertions decide behavior; Result propagates fixture failures.

use fleet_sync_protocol::{RowChange, SqlValue};
use fleet_sync_writer::{
    CaptureChange, StatementForm, StatementGuard, TableLayout, normalize_capture_change,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn table_layout(
    primary_key: Vec<usize>,
    integer_primary_key: Option<usize>,
) -> TestResult<TableLayout> {
    Ok(TableLayout::new(
        "records".into(),
        [
            "key_text",
            "key_number",
            "wide_integer",
            "ratio",
            "payload",
            "empty",
        ]
        .map(str::to_owned)
        .to_vec(),
        primary_key,
        integer_primary_key,
    )?)
}

#[tokio::test]
async fn actual_capture_removes_old_declared_keys_and_preserves_every_storage_class() -> TestResult
{
    for (declaration, primary_key, integer_primary_key, old_key, new_key) in [
        (
            "key_text TEXT NOT NULL PRIMARY KEY,key_number INTEGER",
            vec![0],
            None,
            vec![SqlValue::Text("a".into())],
            vec![SqlValue::Text("c".into())],
        ),
        (
            "key_text TEXT NOT NULL,key_number INTEGER NOT NULL",
            vec![0, 1],
            None,
            vec![SqlValue::Text("a".into()), SqlValue::Integer(10)],
            vec![SqlValue::Text("c".into()), SqlValue::Integer(30)],
        ),
        (
            "key_text TEXT,key_number INTEGER PRIMARY KEY",
            vec![1],
            Some(1),
            vec![SqlValue::Integer(10)],
            vec![SqlValue::Integer(30)],
        ),
    ] {
        // Arrange: real files, seeded before capture; literals are SO4/SO5's oracle.
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("writer.db");
        let database = turso::Builder::new_local(path.to_str().ok_or("path")?)
            .build()
            .await?;
        let connection = database.connect()?;
        let composite = if primary_key.len() == 2 {
            ",PRIMARY KEY(key_text,key_number)"
        } else {
            ""
        };
        connection.execute(&format!("CREATE TABLE records({declaration},wide_integer INTEGER,ratio REAL,payload BLOB,empty TEXT{composite})"),()).await?;
        connection
            .execute(
                "INSERT INTO records VALUES('a',10,9007199254740993,1.25,x'00ff',NULL)",
                (),
            )
            .await?;
        connection
            .execute("PRAGMA capture_data_changes_conn('full')", ())
            .await?;
        let layout = table_layout(primary_key, integer_primary_key)?;

        // Act: normalize only what the pinned engine actually captured.
        connection.execute("BEGIN IMMEDIATE", ()).await?;
        connection
            .execute("UPDATE records SET key_text='c',key_number=30", ())
            .await?;
        connection.execute("COMMIT", ()).await?;
        let mut captured = connection
            .query(
                "SELECT change_type,before,after FROM turso_cdc ORDER BY change_id",
                (),
            )
            .await?;
        let mut changes = Vec::new();
        while let Some(row) = captured.next().await? {
            let before = row.get::<Option<Vec<u8>>>(1)?;
            let after = row.get::<Option<Vec<u8>>>(2)?;
            let change = match row.get::<i64>(0)? {
                0 => CaptureChange::Update {
                    before: before.ok_or("before")?,
                    after: after.ok_or("after")?,
                },
                1 => CaptureChange::Insert { before, after },
                -1 => CaptureChange::Delete {
                    before: before.ok_or("before")?,
                },
                2 => continue,
                _ => return Err("unknown capture kind".into()),
            };
            changes.extend(normalize_capture_change(&layout, change)?);
        }

        // Assert: a new-key upsert alone would leave the old row behind.
        assert_eq!(
            changes,
            vec![
                RowChange::Delete {
                    table: "records".into(),
                    key: old_key
                },
                RowChange::Upsert {
                    table: "records".into(),
                    key: new_key,
                    values: vec![
                        SqlValue::Text("c".into()),
                        SqlValue::Integer(30),
                        SqlValue::Integer(9_007_199_254_740_993),
                        SqlValue::Real(1.25),
                        SqlValue::Blob(vec![0, 255]),
                        SqlValue::Null,
                    ]
                },
            ]
        );
    }
    Ok(())
}

#[test]
fn upsert_guard_rejects_every_primary_key_assignment_including_inside_a_batch() -> TestResult {
    let mut guard = StatementGuard::new(vec![table_layout(vec![0, 1], None)?]);
    for sql in [
        "INSERT INTO records(key_text) VALUES('c') ON CONFLICT DO UPDATE SET key_text=excluded.key_text",
        "INSERT INTO main.\"RECORDS\"(key_text) VALUES('c') ON CONFLICT DO UPDATE SET \"KEY_NUMBER\"=30",
        "WITH input AS (SELECT 'c') INSERT INTO records(key_text) SELECT * FROM input WHERE true ON CONFLICT DO UPDATE SET key_number=30",
        "UPDATE records SET ratio=2; INSERT INTO records(key_text) VALUES('c') ON CONFLICT DO UPDATE SET key_text='c'",
        "INSERT INTO records(key_text) VALUES('c') ON CONFLICT(key_text) DO NOTHING ON CONFLICT(key_number) DO UPDATE SET key_number=30",
        "INSERT INTO records(key_text) VALUES('c') ON CONFLICT DO UPDATE SET (key_text,ratio)=('c',2)",
    ] {
        for _ in 0..2 {
            let refusal = guard
                .check(sql)
                .err()
                .ok_or("unsafe assignment must be refused, including from cache")?;
            assert_eq!(
                refusal.form,
                StatementForm::KeyChangingUpsert {
                    table: "records".into()
                },
                "{sql}"
            );
        }
    }
    Ok(())
}

#[test]
fn upsert_guard_allows_replace_payload_updates_and_unreplicated_tables() -> TestResult {
    let mut guard = StatementGuard::new(vec![table_layout(vec![0], None)?]);
    for sql in [
        "REPLACE INTO records(key_text) VALUES('c')",
        "INSERT OR REPLACE INTO records(key_text) VALUES('c')",
        "INSERT INTO records(key_text) VALUES('c') ON CONFLICT DO UPDATE SET ratio=2",
        "INSERT INTO records(key_text) VALUES('c') ON CONFLICT DO NOTHING",
        "UPDATE records SET key_text='c'",
        "INSERT INTO local_records(key_text) VALUES('c') ON CONFLICT DO UPDATE SET key_text='c'",
        "SELECT 'ON CONFLICT DO UPDATE SET key_text=c'",
    ] {
        guard.check(sql)?;
    }
    Ok(())
}

#[test]
fn corrupt_images_and_invalid_catalogs_fail_closed() -> TestResult {
    let layout = table_layout(vec![0], None)?;
    assert!(
        normalize_capture_change(
            &layout,
            CaptureChange::Insert {
                before: Some(vec![0]),
                after: None
            }
        )
        .is_err()
    );
    assert!(
        normalize_capture_change(
            &layout,
            CaptureChange::Update {
                before: vec![255],
                after: vec![255]
            }
        )
        .is_err()
    );
    assert!(TableLayout::new("records".into(), vec!["id".into()], vec![1], None).is_err());
    assert!(
        TableLayout::new(
            "_fleet_sync_copies".into(),
            vec!["id".into()],
            vec![0],
            None
        )
        .is_err()
    );
    let mut guard = StatementGuard::new(vec![layout]);
    assert!(matches!(
        guard
            .check("INSERT INTO")
            .err()
            .ok_or("malformed SQL was admitted")?
            .form,
        StatementForm::InvalidSql { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn native_infinities_remain_reals_and_native_nan_is_captured_as_null() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("real-values.db");
    let database = turso::Builder::new_local(path.to_str().ok_or("path")?)
        .build()
        .await?;
    let connection = database.connect()?;
    connection
        .execute(
            "CREATE TABLE records(id INTEGER PRIMARY KEY,value REAL)",
            (),
        )
        .await?;
    connection
        .execute("PRAGMA capture_data_changes_conn('full')", ())
        .await?;
    for (id, value) in [(1, f64::INFINITY), (2, f64::NEG_INFINITY), (3, f64::NAN)] {
        connection
            .execute("INSERT INTO records VALUES(?,?)", (id, value))
            .await?;
    }
    connection
        .execute("INSERT INTO records VALUES(4,1e999)", ())
        .await?;
    let table = TableLayout::new(
        "records".into(),
        vec!["id".into(), "value".into()],
        vec![0],
        Some(0),
    )?;
    let mut capture=connection.query("SELECT before,after FROM turso_cdc WHERE table_name='records' AND change_type=1 ORDER BY change_id",()).await?;
    let mut values = Vec::new();
    while let Some(row) = capture.next().await? {
        let normalized = normalize_capture_change(
            &table,
            CaptureChange::Insert {
                before: row.get::<Option<Vec<u8>>>(0)?,
                after: row.get::<Option<Vec<u8>>>(1)?,
            },
        )?;
        for change in normalized {
            if let RowChange::Upsert { values: row, .. } = change {
                values.push(row.get(1).cloned().ok_or("no real column")?);
            }
        }
    }
    assert!(
        matches!(values.as_slice(),[SqlValue::Real(positive),SqlValue::Real(negative),SqlValue::Null,SqlValue::Real(overflow)] if *positive==f64::INFINITY && *negative==f64::NEG_INFINITY && *overflow==f64::INFINITY)
    );
    Ok(())
}
