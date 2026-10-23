#![allow(clippy::panic_in_result_fn)]
use crate::sender_test_fixture::{SenderFixture, TestResult};
use fleet_sync_protocol::{SqlValue, TableDigestBuilder};
use sqlx_core::executor::Executor;
use std::collections::{BTreeMap, BTreeSet};

#[tokio::test]
async fn all_pages_and_manifest_stay_at_one_position_while_live_rows_change() -> TestResult {
    let fixture = SenderFixture::open().await?;
    let mut write = fixture.database.begin_write().await?;
    (&mut write).execute("WITH RECURSIVE numbers(value) AS(VALUES(0) UNION ALL SELECT value+1 FROM numbers WHERE value<299) INSERT INTO composite_records SELECT '019eef00-0000-7000-8000-00000000000c',printf('019eef00-0000-7000-8000-%012x',value),x'80ff' FROM numbers; INSERT INTO text_records VALUES('019eef00-0000-7000-8000-00000000000b','text in integer affinity',x'ff00'); INSERT INTO local_marks VALUES('never in snapshot')").await?;
    write.commit().await?;
    let mut snapshot = fixture.database.snapshot().await?;
    let pinned = snapshot.position();
    let mut write = fixture.database.begin_write().await?;
    (&mut write).execute("DELETE FROM composite_records WHERE account='019eef00-0000-7000-8000-000000000002'; UPDATE text_records SET amount=0 WHERE id='019eef00-0000-7000-8000-000000000001'").await?;
    write.commit().await?;
    let mut cursor = None;
    let mut rows = BTreeMap::<String, Vec<Vec<SqlValue>>>::new();
    let mut pages = 0;
    loop {
        let page = snapshot.page(cursor).await?;
        assert_eq!(page.position, pinned);
        assert!(
            page.schema_steps
                .iter()
                .all(|sql| !sql.contains("local_marks") && !sql.contains("_fleet_sync_"))
        );
        pages += 1;
        rows.entry(page.table).or_default().extend(page.rows);
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(pages, 4, "the composite table must span more than one page");
    assert_eq!(
        rows.keys().map(String::as_str).collect::<Vec<_>>(),
        ["composite_records", "real_records", "text_records"]
    );
    let composite = rows.get("composite_records").ok_or("no composite rows")?;
    assert_eq!(
        composite.len(),
        301,
        "a row deleted after snapshot start still belongs to it"
    );
    let sequence = composite
        .iter()
        .filter(|row| matches!(row.first(),Some(SqlValue::Text(account)) if account=="019eef00-0000-7000-8000-00000000000c"))
        .map(|row| match row.get(1) {
            Some(SqlValue::Text(value)) => Ok(value.clone()),
            _ => Err("bad sequence class"),
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    assert_eq!(
        sequence,
        (0..300)
            .map(|index| format!("019eef00-0000-7000-8000-{index:012x}"))
            .collect()
    );
    assert!(
        composite
            .iter()
            .all(|row| matches!(row.get(2), Some(SqlValue::Blob(_))))
    );
    let text = rows.get("text_records").ok_or("no text rows")?;
    assert!(text.iter().any(|row|matches!(row.as_slice(),[SqlValue::Text(id),SqlValue::Integer(9_007_199_254_740_993),SqlValue::Text(label)] if id=="019eef00-0000-7000-8000-000000000001" && label=="backfilled")));
    assert!(text.iter().any(|row|matches!(row.as_slice(),[SqlValue::Text(id),SqlValue::Text(amount),SqlValue::Blob(label)] if id=="019eef00-0000-7000-8000-00000000000b" && amount=="text in integer affinity" && *label==[255,0])));
    assert!(
        matches!(rows.get("real_records").map(Vec::as_slice),Some([values]) if matches!(values.as_slice(),[SqlValue::Text(id),SqlValue::Real(ratio),SqlValue::Null] if id=="019eef00-0000-7000-8000-000000000004" && *ratio==1.25))
    );
    let manifest = snapshot.manifest().await?;
    assert_eq!(manifest.position, pinned);
    for table in &manifest.tables {
        let values = rows
            .get(table.table.as_str())
            .ok_or("manifest leaked an undeclared table")?;
        let mut digest = TableDigestBuilder::new();
        for row in values {
            digest.push_row(row)?;
        }
        assert_eq!(table.row_count, u64::try_from(values.len())?);
        assert_eq!(table.digest, digest.finish());
    }
    let mut current = fixture.database.snapshot().await?;
    assert!(current.position() > pinned);
    assert_eq!(
        current
            .manifest()
            .await?
            .tables
            .iter()
            .find(|table| table.table.as_str() == "composite_records")
            .ok_or("no current table")?
            .row_count,
        300
    );
    current.close().await?;
    snapshot.close().await?;
    fixture.close().await
}

fn assert_complete_snapshot_units_fit(
    page: &fleet_sync_protocol::SnapshotPage,
    limit: usize,
) -> TestResult {
    use fleet_sync_protocol::*;
    let frame = FrameId::new(durable_steps::Uuid::from_u128(1))?;
    let seeded = SeedPage {
        seed: SeedId {
            generation: u64::MAX,
        },
        page: u32::MAX,
        digest: PageDigest::of(page)?,
        contents: page.clone(),
    };
    let request = ClientFrame::<NoConsumer>::Request {
        frame,
        body: OperationRequest::Replication(ReplicationRequest::SeedPage(Box::new(seeded.clone()))),
    };
    let answer = ServerFrame::<NoConsumer>::Answer {
        frame,
        body: OperationAnswer::Replication(ReplicationAnswer::SnapshotPage(SnapshotRead::Page(
            Box::new(page.clone()),
        ))),
    };
    for (kind, encoded) in [
        ("snapshot page", serde_json::to_vec(page)?),
        ("stored seed page", serde_json::to_vec(&seeded)?),
        ("seed request frame", serde_json::to_vec(&request)?),
        ("snapshot answer frame", serde_json::to_vec(&answer)?),
    ] {
        assert!(
            encoded.len() <= limit,
            "{kind} encoded to {} bytes",
            encoded.len()
        );
    }
    Ok(())
}

#[tokio::test]
async fn byte_short_writer_pages_continue_in_the_same_table_and_preserve_manifest() -> TestResult {
    use fleet_sync_protocol::SqlValue;
    use sqlx_core::query::query;
    use sqlx_turso::Turso;
    let fixture = SenderFixture::open().await?;
    let mut write = fixture.database.begin_write().await?;
    let mut expected = vec![vec![
        SqlValue::Text("019eef00-0000-7000-8000-000000000001".into()),
        SqlValue::Integer(9_007_199_254_740_993),
        SqlValue::Text("backfilled".into()),
    ]];
    for index in 16..64 {
        let key = format!("019eef00-0000-7000-8000-{index:012x}");
        let payload = format!(
            "row {index}:{}{}",
            "\"".repeat(24 * 1024),
            "\\".repeat(8 * 1024)
        );
        query::<Turso>("INSERT INTO text_records(id,amount,label) VALUES(?,?,?)")
            .bind(&key)
            .bind(i64::from(index))
            .bind(&payload)
            .execute(&mut write)
            .await?;
        expected.push(vec![
            SqlValue::Text(key),
            SqlValue::Integer(i64::from(index)),
            SqlValue::Text(payload),
        ]);
    }
    write.commit().await?;
    let mut snapshot = fixture.database.snapshot().await?;
    let pinned = snapshot.position();
    let mut cursor = None;
    let mut received = Vec::new();
    let mut byte_short = false;
    let mut pages = 0;
    let started = std::time::Instant::now();
    loop {
        let replay_cursor = cursor.clone();
        let page = snapshot.page(cursor).await?;
        assert_complete_snapshot_units_fit(&page, 1024 * 1024)?;
        assert_eq!(page.position, pinned);
        // A lost answer at an unchanged cursor must recreate identical bytes.
        assert_eq!(
            serde_json::to_vec(&snapshot.page(replay_cursor).await?)?,
            serde_json::to_vec(&page)?
        );
        if page.table == "text_records" {
            if let Some(next) = &page.next {
                assert_eq!(next.table, "text_records");
                assert!(!page.rows.is_empty());
                assert_eq!(
                    next.after_key,
                    vec![
                        page.rows
                            .last()
                            .and_then(|row| row.first())
                            .ok_or("missing emitted key")?
                            .clone()
                    ]
                );
                byte_short |= page.rows.len() < 256;
            }
            received.extend(page.rows.clone());
        }
        cursor = page.next;
        pages += 1;
        if cursor.is_none() {
            break;
        }
        assert!(pages < 64, "page producer did not make progress");
    }
    let manifest = snapshot.manifest().await?;
    let actual = manifest
        .tables
        .iter()
        .find(|table| table.table.as_str() == "text_records")
        .ok_or("missing text manifest")?;
    let mut digest = TableDigestBuilder::new();
    for row in &expected {
        digest.push_row(row)?;
    }
    assert!(
        byte_short,
        "byte budget did not split below the row-count limit"
    );
    assert_eq!(received, expected);
    assert_eq!(actual.row_count, u64::try_from(expected.len())?);
    assert_eq!(actual.digest, digest.finish());
    eprintln!(
        "writer byte paging: {pages} pages, {} rows, {:?} including replay and manifest",
        expected.len(),
        started.elapsed()
    );
    snapshot.close().await?;
    fixture.close().await
}

#[tokio::test]
async fn oversize_writer_row_is_refused_before_a_page_is_emitted() -> TestResult {
    use fleet_sync_protocol::SnapshotCursor;
    use sqlx_core::query::query;
    use sqlx_turso::Turso;
    let fixture = SenderFixture::open().await?;
    let mut write = fixture.database.begin_write().await?;
    query::<Turso>(
        "UPDATE text_records SET label=? WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .bind("x".repeat(2 * 1024 * 1024))
    .execute(&mut write)
    .await?;
    write.commit().await?;
    let mut snapshot = fixture.database.snapshot().await?;
    let result = snapshot
        .page(Some(SnapshotCursor {
            position: snapshot.position(),
            table: "text_records".into(),
            after_key: Vec::new(),
        }))
        .await;
    assert!(
        matches!(result, Err(crate::OpenError::UnitTooLarge(
        fleet_sync_protocol::unit_budget::UnitTooLarge::Row { table, key, encoded_size }
    )) if table == "text_records" && key == ["019eef00-0000-7000-8000-000000000001"] && encoded_size > 1536 * 1024),
        "an oversize row was emitted or lost its actual table/key context"
    );
    snapshot.close().await?;
    fixture.close().await
}

#[tokio::test]
async fn first_writer_row_above_target_still_progresses_below_the_hard_cap() -> TestResult {
    use fleet_sync_protocol::SnapshotCursor;
    use sqlx_core::query::query;
    use sqlx_turso::Turso;
    let fixture = SenderFixture::open().await?;
    let payload = "x".repeat(1100 * 1024);
    let mut write = fixture.database.begin_write().await?;
    query::<Turso>(
        "UPDATE text_records SET label=? WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .bind(&payload)
    .execute(&mut write)
    .await?;
    query::<Turso>(
        "INSERT INTO text_records VALUES('019eef00-0000-7000-8000-000000000010',16,'next row')",
    )
    .execute(&mut write)
    .await?;
    write.commit().await?;
    let mut snapshot = fixture.database.snapshot().await?;
    let page = snapshot
        .page(Some(SnapshotCursor {
            position: snapshot.position(),
            table: "text_records".into(),
            after_key: Vec::new(),
        }))
        .await?;
    assert_eq!(page.rows.len(), 1);
    assert!(
        matches!(page.rows.first().and_then(|row| row.get(2)), Some(SqlValue::Text(value)) if value == &payload)
    );
    assert!(serde_json::to_vec(&page)?.len() > 1024 * 1024);
    assert_complete_snapshot_units_fit(&page, 1536 * 1024)?;
    let next = page.next.ok_or("large first row did not continue")?;
    assert_eq!(next.table, "text_records");
    assert_eq!(
        next.after_key,
        [SqlValue::Text(
            "019eef00-0000-7000-8000-000000000001".into()
        )]
    );
    let last = snapshot.page(Some(next)).await?;
    assert_eq!(last.rows.len(), 1);
    assert!(last.next.is_none());
    assert_complete_snapshot_units_fit(&last, 1024 * 1024)?;
    snapshot.close().await?;
    fixture.close().await
}
