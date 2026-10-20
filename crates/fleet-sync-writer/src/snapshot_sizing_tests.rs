#![allow(clippy::panic_in_result_fn)]
use super::*;
use crate::sender_test_fixture::{SenderFixture, TestResult};
use fleet_sync_protocol::SqlValue;
use sqlx_core::{executor::Executor, query::query};

async fn compare_page(
    snapshot: &mut WriterSnapshot,
    cursor: Option<SnapshotCursor>,
) -> TestResult<Option<SnapshotCursor>> {
    let expected = snapshot.page_oracle(cursor.clone()).await;
    FULL_MEASURES.with(|count| count.set(0));
    READ_QUERIES.with(|count| count.set(0));
    let actual = snapshot.page(cursor).await;
    let calls = FULL_MEASURES.with(std::cell::Cell::get);
    match (expected, actual) {
        (Ok(expected), Ok(actual)) => {
            // Eight is the adopted I/O contract, independent of the production
            // constant so a regression to one query per row cannot agree with itself.
            let queries = READ_QUERIES.with(std::cell::Cell::get);
            assert!(
                queries <= actual.rows.len() / 8 + 1,
                "page with {} rows used {queries} row queries",
                actual.rows.len()
            );
            assert!(calls <= 2, "growing page was fully measured {calls} times");
            assert_eq!(serde_json::to_vec(&actual)?, serde_json::to_vec(&expected)?);
            assert_eq!(
                fleet_sync_protocol::PageDigest::of(&actual)?,
                fleet_sync_protocol::PageDigest::of(&expected)?
            );
            Ok(actual.next)
        }
        (Err(OpenError::UnitTooLarge(expected)), Err(OpenError::UnitTooLarge(actual))) => {
            assert_eq!(actual, expected, "refusal must preserve exact encoded_size");
            Ok(None)
        }
        (expected, actual) => {
            Err(format!("pager/oracle diverged: {expected:?} / {actual:?}").into())
        }
    }
}

#[tokio::test]
async fn generated_writer_pages_match_oracle_with_constant_full_measures() -> TestResult {
    let fixture = SenderFixture::open().await?;
    let mut write = fixture.database.begin_write().await?;
    (&mut write).execute("DELETE FROM real_records; WITH RECURSIVE numbers(value) AS(VALUES(16) UNION ALL SELECT value+1 FROM numbers WHERE value<315) INSERT INTO text_records SELECT printf('019eef00-0000-7000-8000-%012x',value),value,'placeholder' FROM numbers").await?;
    // Different row widths and JSON escaping, with enough small rows to hit the
    // count boundary. Existing fixture also supplies composite/blob keys/values.
    for index in 16..316 {
        let payload = format!("{index}:{}", "\"\\\n\t雪🦀".repeat((index * 37) % 91));
        query::<Turso>("UPDATE text_records SET label=? WHERE amount=?")
            .bind(payload)
            .bind(i64::try_from(index)?)
            .execute(&mut write)
            .await?;
    }
    write.commit().await?;
    let mut snapshot = fixture.database.snapshot().await?;
    let mut cursor = None;
    let mut pages = 0;
    loop {
        cursor = compare_page(&mut snapshot, cursor).await?;
        pages += 1;
        if cursor.is_none() {
            break;
        }
        assert!(pages < 10);
    }
    assert!(
        pages >= 4,
        "multiple tables, empty table and row cap were not exercised"
    );
    let cursor = SnapshotCursor {
        position: snapshot.position(),
        table: "text_records".into(),
        after_key: vec![SqlValue::Text(
            "019eef00-0000-7000-8000-000000000080".into(),
        )],
    };
    compare_page(&mut snapshot, Some(cursor)).await?;
    // A schema-only oversize refusal must carry exactly the old carrier size.
    snapshot.statements.push("x".repeat(MAX_UNIT_BYTES));
    compare_page(&mut snapshot, None).await?;
    snapshot.close().await?;
    fixture.close().await
}

#[tokio::test]
async fn exact_writer_byte_boundaries_and_overmax_refusals_match_oracle() -> TestResult {
    let fixture = SenderFixture::open().await?;
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute("UPDATE text_records SET label=''")
        .await?;
    write.commit().await?;
    let mut snapshot = fixture.database.snapshot().await?;
    let cursor = SnapshotCursor {
        position: snapshot.position(),
        table: "text_records".into(),
        after_key: vec![],
    };
    let page = snapshot.page_oracle(Some(cursor)).await?;
    let overhead = measure_snapshot_unit(page)?.encoded_size;
    snapshot.close().await?;
    for limit in [TARGET_UNIT_BYTES, MAX_UNIT_BYTES] {
        for offset in [-1_isize, 0, 1] {
            let desired = limit.checked_add_signed(offset).ok_or("bad boundary")?;
            let mut write = fixture.database.begin_write().await?;
            query::<Turso>("UPDATE text_records SET label=?")
                .bind("x".repeat(desired - overhead))
                .execute(&mut write)
                .await?;
            write.commit().await?;
            let mut snapshot = fixture.database.snapshot().await?;
            let cursor = SnapshotCursor {
                position: snapshot.position(),
                table: "text_records".into(),
                after_key: vec![],
            };
            let observed_size = match snapshot.page_oracle(Some(cursor.clone())).await {
                Ok(page) => measure_snapshot_unit(page)?.encoded_size,
                Err(OpenError::UnitTooLarge(UnitTooLarge::Row { encoded_size, .. })) => {
                    encoded_size
                }
                other => return Err(format!("unexpected boundary result: {other:?}").into()),
            };
            assert_eq!(observed_size, desired, "fixture missed exact boundary");
            compare_page(&mut snapshot, Some(cursor)).await?;
            snapshot.close().await?;
        }
    }
    // Two growing rows force target rejection and restoration of the old next.
    let mut write = fixture.database.begin_write().await?;
    query::<Turso>("UPDATE text_records SET label=?")
        .bind("\"\\雪".repeat(90_000))
        .execute(&mut write)
        .await?;
    query::<Turso>("INSERT INTO text_records VALUES('019eef00-0000-7000-8000-000000000010',16,?)")
        .bind("🦀\n".repeat(100_000))
        .execute(&mut write)
        .await?;
    write.commit().await?;
    let mut snapshot = fixture.database.snapshot().await?;
    let cursor = SnapshotCursor {
        position: snapshot.position(),
        table: "text_records".into(),
        after_key: vec![],
    };
    let next = compare_page(&mut snapshot, Some(cursor))
        .await?
        .ok_or("missing rejected-row continuation")?;
    compare_page(&mut snapshot, Some(next)).await?;
    snapshot.close().await?;
    fixture.close().await
}

#[tokio::test]
async fn final_writer_size_mismatch_is_corrupt() -> TestResult {
    let fixture = SenderFixture::open().await?;
    let mut snapshot = fixture.database.snapshot().await?;
    let page = snapshot.page(None).await?;
    assert!(matches!(
        checked_page(page, 0),
        Err(OpenError::Corrupt { .. })
    ));
    snapshot.close().await?;
    fixture.close().await
}
