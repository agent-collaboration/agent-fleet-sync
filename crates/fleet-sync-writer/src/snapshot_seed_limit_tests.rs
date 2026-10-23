#![allow(clippy::panic_in_result_fn)]
use super::*;
use crate::{
    sender_test_fixture::{SenderFixture, TestResult},
    writer_scope::WriterScope,
};
use fleet_sync_protocol::{
    CopyOrigin, CreateCopy, PageDigest, SeedAttemptId, SeedPage, SeedPageAnswer, SqlValue,
};
use fleet_sync_receiver::SeedAuthority;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};

/// With the good estimator the writer refuses first. The deliberate omitted-
/// backslash variant must reach and be refused by the real receiver instead.
#[tokio::test]
async fn a_misestimated_writer_page_cannot_be_staged_by_the_receiver() -> TestResult {
    let fixture = SenderFixture::open().await?;
    let mut write = fixture.database.begin_write().await?;
    query::<Turso>("UPDATE text_records SET label=?")
        .bind("\\".repeat(800 * 1024))
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
        .await;
    let contents = match page {
        Err(OpenError::UnitTooLarge(_)) => {
            println!("good writer independently refused the oversized row");
            snapshot.close().await?;
            return fixture.close().await;
        }
        Ok(contents) => contents,
        Err(error) => return Err(error.into()),
    };
    let actual = measure_snapshot_unit(contents.clone())?.encoded_size;
    assert!(
        actual > MAX_UNIT_BYTES,
        "fault variant must actually exceed the real cap"
    );
    assert!(matches!(
        contents.rows.first().and_then(|row| row.last()),
        Some(SqlValue::Text(_))
    ));
    let mut connection = TursoConnectOptions::new().connect().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    fleet_sync_receiver::RECEIVER_MIGRATOR
        .run_direct(None, &mut *transaction, false)
        .await?;
    fleet_sync_receiver::create_copy(
        &mut WriterScope(&mut transaction),
        &CreateCopy {
            database: contents.database,
            epoch: contents.position.epoch,
            holder: None,
            origin: CopyOrigin::AwaitingSeed {
                expected_position: Some(contents.position),
            },
        },
    )
    .await?;
    let seed = fleet_sync_receiver::seed_begin(
        &mut WriterScope(&mut transaction),
        contents.database,
        SeedAttemptId::new("019eef00-0000-7000-8000-000000000732".parse()?)?,
        &SeedAuthority::Admin,
    )
    .await?;
    let page = SeedPage {
        seed,
        page: 0,
        digest: PageDigest::of(&contents)?,
        contents,
    };
    let result = fleet_sync_receiver::seed_page(
        &mut WriterScope(&mut transaction),
        &page,
        &SeedAuthority::Admin,
    )
    .await?;
    assert_eq!(result, SeedPageAnswer::UnitTooLarge);
    let row = query::<Turso>("SELECT COUNT(*) AS count FROM _fleet_sync_seed_pages")
        .fetch_one(&mut *transaction)
        .await?;
    assert_eq!(row.try_get::<i64, _>("count")?, 0);
    transaction.commit().await?;
    connection.close().await?;
    println!(
        "actual oversized writer page refused by receiver before staging; encoded size={actual}"
    );
    snapshot.close().await?;
    fixture.close().await
}
