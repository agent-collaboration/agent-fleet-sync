#![allow(clippy::panic_in_result_fn)]
use fleet_sync_protocol::{RowChange, SqlValue};
use fleet_sync_writer::{
    CaptureChange, CaptureError, StatementForm, StatementGuard, TableLayout,
    normalize_capture_change,
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const FIRST: &str = "019eef00-0000-7000-8000-000000000001";
const SECOND: &str = "019eef00-0000-7000-8000-000000000002";
const CHANGED: &str = "019eef00-0000-7000-8000-000000000003";
fn table_layout(primary_key: Vec<usize>) -> TestResult<TableLayout> {
    Ok(TableLayout::new(
        "records".into(),
        [
            "key_first",
            "key_second",
            "wide_integer",
            "ratio",
            "payload",
            "empty",
        ]
        .map(str::to_owned)
        .to_vec(),
        primary_key,
    )?)
}
#[tokio::test]
async fn actual_capture_keeps_uuid_keys_and_every_non_key_storage_class_and_fails_closed_on_mutation()
-> TestResult {
    for composite in [false, true] {
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
        connection.execute(&format!("CREATE TABLE records(key_first TEXT NOT NULL,key_second TEXT NOT NULL,wide_integer INTEGER,ratio REAL,payload BLOB,empty TEXT,{primary})"),()).await?;
        connection
            .execute(
                "INSERT INTO records VALUES(?,?,9007199254740993,1.25,x'00ff',NULL)",
                (FIRST, SECOND),
            )
            .await?;
        connection
            .execute("PRAGMA capture_data_changes_conn('full')", ())
            .await?;
        connection
            .execute("UPDATE records SET ratio=2.5", ())
            .await?;
        let layout = table_layout(if composite { vec![0, 1] } else { vec![0] })?;
        let mut capture = connection
            .query(
                "SELECT before,after FROM turso_cdc WHERE change_type=0 ORDER BY change_id",
                (),
            )
            .await?;
        let row = capture.next().await?.ok_or("no update")?;
        let changed = normalize_capture_change(
            &layout,
            CaptureChange::Update {
                before: row.get(0)?,
                after: row.get(1)?,
            },
        )?;
        let key = if composite {
            vec![SqlValue::Text(FIRST.into()), SqlValue::Text(SECOND.into())]
        } else {
            vec![SqlValue::Text(FIRST.into())]
        };
        assert_eq!(
            changed,
            vec![RowChange::Upsert {
                table: "records".into(),
                key,
                values: vec![
                    SqlValue::Text(FIRST.into()),
                    SqlValue::Text(SECOND.into()),
                    SqlValue::Integer(9_007_199_254_740_993),
                    SqlValue::Real(2.5),
                    SqlValue::Blob(vec![0, 255]),
                    SqlValue::Null
                ]
            }]
        );
        drop(capture);
        // Bypass the writer deliberately: an impossible captured mutation must
        // stop the stream rather than become delete+upsert (decision 51).
        connection
            .execute("UPDATE records SET key_first=?", (CHANGED,))
            .await?;
        let mut capture=connection.query("SELECT before,after FROM turso_cdc WHERE change_type=0 ORDER BY change_id DESC LIMIT 1",()).await?;
        let row = capture.next().await?.ok_or("no mutation capture")?;
        assert!(matches!(
            normalize_capture_change(
                &layout,
                CaptureChange::Update {
                    before: row.get(0)?,
                    after: row.get(1)?
                }
            ),
            Err(CaptureError::KeyChange { .. })
        ));
    }
    Ok(())
}
#[test]
fn statement_guard_refuses_every_key_assignment_including_cached_batches_and_each_composite_column()
-> TestResult {
    let mut guard = StatementGuard::new(vec![table_layout(vec![0, 1])?]);
    for sql in [
        "UPDATE records SET key_first=key_first",
        "UPDATE records SET (key_second,ratio)=('next',2)",
        "INSERT INTO records(key_first) VALUES('next') ON CONFLICT DO UPDATE SET key_first=excluded.key_first",
        "INSERT INTO main.\"RECORDS\"(key_first) VALUES('next') ON CONFLICT DO UPDATE SET \"KEY_SECOND\"='next'",
        "WITH input AS (SELECT 'next') INSERT INTO records(key_first) SELECT * FROM input WHERE true ON CONFLICT DO UPDATE SET key_second='next'",
        "UPDATE records SET ratio=2; INSERT INTO records(key_first) VALUES('next') ON CONFLICT DO UPDATE SET key_first='next'",
        "INSERT INTO records(key_first) VALUES('next') ON CONFLICT(key_first) DO NOTHING ON CONFLICT(key_second) DO UPDATE SET key_second='next'",
    ] {
        for _ in 0..2 {
            assert!(
                matches!(
                    guard.check(sql),
                    Err(fleet_sync_writer::StatementRefusal {
                        form: StatementForm::KeyChange { .. }
                    })
                ),
                "{sql}"
            );
        }
    }
    for sql in [
        "REPLACE INTO records(key_first) VALUES('next')",
        "INSERT OR REPLACE INTO records(key_first) VALUES('next')",
        "INSERT INTO records(key_first) VALUES('next') ON CONFLICT DO UPDATE SET ratio=2",
        "INSERT INTO records(key_first) VALUES('next') ON CONFLICT DO NOTHING",
        "UPDATE records SET ratio=2",
        "UPDATE local_records SET key_first='next'",
        "INSERT INTO local_records(key_first) VALUES('next') ON CONFLICT DO UPDATE SET key_first='next'",
        "SELECT 'ON CONFLICT DO UPDATE SET key_first=next'",
    ] {
        guard.check(sql)?;
    }
    Ok(())
}
#[test]
fn corrupt_images_missing_after_images_and_invalid_catalogs_fail_closed() -> TestResult {
    let layout = table_layout(vec![0])?;
    assert!(normalize_capture_change(&layout, CaptureChange::Insert { after: None }).is_err());
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
    assert!(TableLayout::new("records".into(), vec!["id".into()], vec![1]).is_err());
    assert!(TableLayout::new("_fleet_sync_copies".into(), vec!["id".into()], vec![0]).is_err());
    let mut guard = StatementGuard::new(vec![layout]);
    assert!(matches!(
        guard.check("INSERT INTO"),
        Err(fleet_sync_writer::StatementRefusal {
            form: StatementForm::InvalidSql { .. }
        })
    ));
    Ok(())
}
#[tokio::test]
async fn native_infinities_remain_reals_and_native_nan_is_captured_as_null() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("reals.db");
    let database = turso::Builder::new_local(path.to_str().ok_or("path")?)
        .build()
        .await?;
    let connection = database.connect()?;
    connection
        .execute(
            "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,value REAL)",
            (),
        )
        .await?;
    connection
        .execute("PRAGMA capture_data_changes_conn('full')", ())
        .await?;
    for (index, value) in [(1, f64::INFINITY), (2, f64::NEG_INFINITY), (3, f64::NAN)] {
        let id = format!("019eef00-0000-7000-8000-{index:012x}");
        connection
            .execute("INSERT INTO records VALUES(?,?)", (id, value))
            .await?;
    }
    connection
        .execute(
            "INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000004',1e999)",
            (),
        )
        .await?;
    let layout = TableLayout::new("records".into(), vec!["id".into(), "value".into()], vec![0])?;
    let mut capture=connection.query("SELECT after FROM turso_cdc WHERE table_name='records' AND change_type=1 ORDER BY change_id",()).await?;
    let mut values = Vec::new();
    while let Some(row) = capture.next().await? {
        for change in
            normalize_capture_change(&layout, CaptureChange::Insert { after: row.get(0)? })?
        {
            if let RowChange::Upsert { values: row, .. } = change {
                values.push(row.get(1).cloned().ok_or("no real value")?);
            }
        }
    }
    assert!(
        matches!(values.as_slice(),[SqlValue::Real(positive),SqlValue::Real(negative),SqlValue::Null,SqlValue::Real(overflow)] if *positive==f64::INFINITY && *negative==f64::NEG_INFINITY && *overflow==f64::INFINITY)
    );
    Ok(())
}
