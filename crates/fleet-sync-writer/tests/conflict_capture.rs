#![allow(clippy::panic_in_result_fn)]
use fleet_sync_protocol::{RowChange, SqlValue};
use fleet_sync_writer::{CaptureChange, TableLayout, normalize_capture_change};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const FIRST: &str = "019eef00-0000-7000-8000-000000000001";
const SECOND: &str = "019eef00-0000-7000-8000-000000000002";
const THIRD: &str = "019eef00-0000-7000-8000-000000000003";
const FOURTH: &str = "019eef00-0000-7000-8000-000000000004";
const FIFTH: &str = "019eef00-0000-7000-8000-000000000005";
const SIXTH: &str = "019eef00-0000-7000-8000-000000000006";
#[tokio::test]
async fn replace_captures_implicit_deletes_for_single_and_composite_uuid_keys_in_twelve_conflicts()
-> TestResult {
    for composite in [false, true] {
        for syntax in ["INSERT OR REPLACE INTO", "REPLACE INTO"] {
            for (first, second, slot, deleted) in [
                (FIRST, SECOND, "z", 1),
                (FIFTH, SIXTH, "x", 1),
                (FIRST, SECOND, "y", 2),
            ] {
                let directory = tempfile::tempdir()?;
                let path = directory.path().join("writer.db");
                let database = turso::Builder::new_local(path.to_str().ok_or("path")?)
                    .build()
                    .await?;
                let connection = database.connect()?;
                let primary = if composite {
                    "PRIMARY KEY(key_first,key_second)"
                } else {
                    "PRIMARY KEY(key_first)"
                };
                connection.execute(&format!("CREATE TABLE records(key_first TEXT NOT NULL,key_second TEXT NOT NULL,slot TEXT UNIQUE,{primary})"),()).await?;
                connection
                    .execute(
                        "INSERT INTO records VALUES(?,?,'x'),(?,?,'y')",
                        (FIRST, SECOND, THIRD, FOURTH),
                    )
                    .await?;
                connection
                    .execute("PRAGMA capture_data_changes_conn('full')", ())
                    .await?;
                connection
                    .execute(
                        &format!("{syntax} records VALUES(?,?,?)"),
                        (first, second, slot),
                    )
                    .await?;
                let layout = TableLayout::new(
                    "records".into(),
                    ["key_first", "key_second", "slot"]
                        .map(str::to_owned)
                        .to_vec(),
                    if composite { vec![0, 1] } else { vec![0] },
                )?;
                let mut capture = connection
                    .query(
                        "SELECT change_type,before,after FROM turso_cdc ORDER BY change_id",
                        (),
                    )
                    .await?;
                let mut changes = Vec::new();
                while let Some(row) = capture.next().await? {
                    let change = match row.get::<i64>(0)? {
                        -1 => CaptureChange::Delete {
                            before: row.get(1)?,
                        },
                        0 => CaptureChange::Update {
                            before: row.get(1)?,
                            after: row.get(2)?,
                        },
                        1 => CaptureChange::Insert { after: row.get(2)? },
                        2 => continue,
                        _ => return Err("unknown capture kind".into()),
                    };
                    changes.extend(normalize_capture_change(&layout, change)?);
                }
                assert_eq!(
                    changes
                        .iter()
                        .take_while(|change| matches!(change, RowChange::Delete { .. }))
                        .count(),
                    deleted
                );
                assert_eq!(changes.len(), deleted + 1);
                assert!(matches!(changes.last(), Some(RowChange::Upsert { .. })));
                let mut expected = vec![(FIRST, SECOND)];
                if deleted == 2 {
                    expected.push((THIRD, FOURTH));
                }
                for (first, second) in expected {
                    let key = if composite {
                        vec![SqlValue::Text(first.into()), SqlValue::Text(second.into())]
                    } else {
                        vec![SqlValue::Text(first.into())]
                    };
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
