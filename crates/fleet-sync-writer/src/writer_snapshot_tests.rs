#![allow(clippy::panic_in_result_fn)]
use crate::sender_test_fixture::{SenderFixture, TestResult};
use fleet_sync_protocol::{SqlValue, TableDigestBuilder};
use sqlx_core::executor::Executor;
use std::collections::{BTreeMap, BTreeSet};

#[tokio::test]
async fn all_pages_and_manifest_stay_at_one_position_while_live_rows_change() -> TestResult {
    let fixture = SenderFixture::open().await?;
    let mut write = fixture.database.begin_write().await?;
    (&mut write).execute("WITH RECURSIVE numbers(value) AS(VALUES(0) UNION ALL SELECT value+1 FROM numbers WHERE value<299) INSERT INTO composite_records SELECT 'paged',value,x'80ff' FROM numbers; INSERT INTO text_records VALUES('dynamic-type','text in integer affinity',x'ff00'); INSERT INTO local_marks VALUES('never in snapshot')").await?;
    write.commit().await?;
    let mut snapshot = fixture.database.snapshot().await?;
    let pinned = snapshot.position();
    let mut write = fixture.database.begin_write().await?;
    (&mut write).execute("DELETE FROM composite_records WHERE account='account'; UPDATE text_records SET amount=0 WHERE id='initial'").await?;
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
        ["composite_records", "integer_records", "text_records"]
    );
    let composite = rows.get("composite_records").ok_or("no composite rows")?;
    assert_eq!(
        composite.len(),
        301,
        "a row deleted after snapshot start still belongs to it"
    );
    let sequence = composite
        .iter()
        .filter(|row| matches!(row.first(),Some(SqlValue::Text(account)) if account=="paged"))
        .map(|row| match row.get(1) {
            Some(SqlValue::Integer(value)) => Ok(*value),
            _ => Err("bad sequence class"),
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    assert_eq!(sequence, (0..300).collect());
    assert!(
        composite
            .iter()
            .all(|row| matches!(row.get(2), Some(SqlValue::Blob(_))))
    );
    let text = rows.get("text_records").ok_or("no text rows")?;
    assert!(text.iter().any(|row|matches!(row.as_slice(),[SqlValue::Text(id),SqlValue::Integer(9_007_199_254_740_993),SqlValue::Text(label)] if id=="initial" && label=="backfilled")));
    assert!(text.iter().any(|row|matches!(row.as_slice(),[SqlValue::Text(id),SqlValue::Text(amount),SqlValue::Blob(label)] if id=="dynamic-type" && amount=="text in integer affinity" && *label==[255,0])));
    assert!(
        matches!(rows.get("integer_records").map(Vec::as_slice),Some([values]) if matches!(values.as_slice(),[SqlValue::Integer(10),SqlValue::Real(ratio),SqlValue::Null] if *ratio==1.25))
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
