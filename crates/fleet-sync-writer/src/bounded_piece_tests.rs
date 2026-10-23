#![allow(clippy::panic_in_result_fn)]
#[path = "bounded_piece_fixture.rs"]
mod fixture;
#[path = "bounded_piece_refusal_tests.rs"]
mod refusal_tests;
use crate::{sender_test_fixture::*, *};
use fixture::*;
use fleet_sync_protocol::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::{
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
};

#[tokio::test]
async fn byte_bounded_parts_stage_real_rows_and_replay_identical_units_after_lost_answer()
-> TestResult {
    let source = SenderFixture::open().await?;
    source.migrate_copies().await?;
    let mut write = source.database.begin_write().await?;
    let mut expected = Vec::new();
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
        expected.push((key, i64::from(index), payload));
    }
    write.commit().await?;
    let (after, captured) = last_capture(source.database.as_ref()).await?;
    let annotator = Arc::new(Annotator { bytes: 32 * 1024 });
    let first = crate::piece_builder::cut_transaction(
        source.database.database(),
        captured.position.epoch,
        after,
        &captured,
        NonZeroUsize::MIN.saturating_add(999),
        annotator.as_ref(),
    )?;
    let repeated = crate::piece_builder::cut_transaction(
        source.database.database(),
        captured.position.epoch,
        after,
        &captured,
        NonZeroUsize::MIN.saturating_add(999),
        annotator.as_ref(),
    )?;
    assert_eq!(
        serde_json::to_vec(&first)?,
        serde_json::to_vec(&repeated)?,
        "recomputed parts changed identity bytes"
    );
    let whole_digest = PieceDigest::of_transaction(&captured.transaction)?;
    let ReplicatedTransaction::Rows { rows: original, .. } = &captured.transaction else {
        return Err("expected real row capture".into());
    };
    assert_eq!(original.len(), expected.len());
    let mut reconstructed = Vec::new();
    for piece in &first {
        let encoded = serde_json::to_vec(piece)?.len();
        assert!(
            encoded <= 1024 * 1024,
            "part payload encoded to {encoded} bytes"
        );
        let part = piece
            .part
            .as_ref()
            .ok_or("byte budget did not create parts")?;
        assert_eq!(part.transaction_digest, whole_digest);
        assert_eq!(piece.recomputed_digest()?, piece.digest);
        assert_eq!(piece.annotation.is_some(), part.index + 1 == part.count);
        let Some(ReplicatedTransaction::Rows { rows, .. }) = piece.transactions.first() else {
            return Err("missing row fragment".into());
        };
        reconstructed.extend(rows.iter().cloned());
    }
    assert_eq!(&reconstructed, original);
    let measured = Arc::new(MeasuredTransport::new(source.transport.clone(), true));
    let sender = ReplicationSender::new(
        source.database.clone(),
        annotator,
        source.marks.clone(),
        measured.clone(),
        SenderOptions {
            max_part_items: NonZeroUsize::MIN.saturating_add(999),
            ..SenderOptions::default()
        },
    );
    assert!(matches!(
        sender.replicate_once(source.first).await,
        Err(SenderError::Send(SendFailure::AnswerLost))
    ));
    let before_retry = measured.observed.lock().await.clone();
    assert!(before_retry.len() > 1);
    assert!(
        before_retry
            .iter()
            .all(|unit| unit.framed_bytes <= 1024 * 1024)
    );
    assert!(
        measured.largest_staged_fields.load(Ordering::SeqCst) > 0,
        "no real part was staged"
    );
    assert!(measured.largest_staged_fields.load(Ordering::SeqCst) + 128 <= 1024 * 1024);
    sender.replicate_once(source.first).await?;
    let all = measured.observed.lock().await;
    let replay = all
        .get(before_retry.len())
        .ok_or("lost answer did not replay a unit")?;
    assert_eq!(
        replay.encoded,
        before_retry.first().ok_or("no original unit")?.encoded
    );
    drop(all);
    let file = source
        .transport
        .files
        .get(&source.first)
        .ok_or("missing copy")?;
    let mut owner = file.connection.lock().await;
    let connection = owner.as_mut().ok_or("closed copy")?;
    let actual = query::<Turso>("SELECT id,amount,label FROM text_records WHERE id>='019eef00-0000-7000-8000-000000000010' ORDER BY id")
        .fetch_all(&mut *connection).await?.into_iter().map(|row| Ok((row.try_get("id")?, row.try_get("amount")?, row.try_get("label")?)))
        .collect::<Result<Vec<(String,i64,String)>, sqlx_core::Error>>()?;
    assert_eq!(actual, expected);
    let annotations = query::<Turso>("SELECT position FROM hook_calls WHERE position LIKE '{%'")
        .fetch_all(&mut *connection)
        .await?;
    assert_eq!(
        annotations.len(),
        1,
        "lost answer repeated the annotation effect"
    );
    let actual: Annotation = serde_json::from_str(
        &annotations
            .first()
            .ok_or("no whole annotation")?
            .try_get::<String, _>("position")?,
    )?;
    assert_eq!(actual.origin, captured.position.sequence.value());
    assert_eq!(actual.text, "a".repeat(32 * 1024));
    drop(owner);
    source.close().await
}

#[tokio::test]
async fn individually_fitting_row_and_annotation_use_an_empty_final_part_when_combined_over_cap()
-> TestResult {
    let source = SenderFixture::open().await?;
    source.migrate_copies().await?;
    let mut write = source.database.begin_write().await?;
    query::<Turso>(
        "UPDATE text_records SET label=? WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .bind("r".repeat(900 * 1024))
    .execute(&mut write)
    .await?;
    write.commit().await?;
    let measured = Arc::new(MeasuredTransport::new(source.transport.clone(), false));
    let sender = ReplicationSender::new(
        source.database.clone(),
        Arc::new(Annotator { bytes: 900 * 1024 }),
        source.marks.clone(),
        measured.clone(),
        SenderOptions::default(),
    );
    sender.replicate_once(source.first).await?;
    let units = measured.observed.lock().await;
    assert_eq!(units.len(), 2);
    let first = units.first().ok_or("missing data part")?;
    let final_part = units.last().ok_or("missing final annotation")?;
    assert_eq!(first.rows, 1);
    assert!(!first.annotated);
    assert_eq!(final_part.rows, 0);
    assert!(final_part.annotated);
    assert!(matches!(first.part.as_ref(), Some(part) if part.index==0 && part.count==2));
    assert!(matches!(final_part.part.as_ref(), Some(part) if part.index==1 && part.count==2));
    drop(units);
    let file = source
        .transport
        .files
        .get(&source.first)
        .ok_or("missing copy")?;
    let mut owner = file.connection.lock().await;
    let connection = owner.as_mut().ok_or("closed copy")?;
    let body: String = query::<Turso>("SELECT label FROM text_records")
        .fetch_one(&mut *connection)
        .await?
        .try_get("label")?;
    assert_eq!(body, "r".repeat(900 * 1024));
    let annotations = query::<Turso>("SELECT position FROM hook_calls WHERE position LIKE '{%'")
        .fetch_all(&mut *connection)
        .await?;
    assert_eq!(annotations.len(), 1);
    let actual: Annotation = serde_json::from_str(
        &annotations
            .first()
            .ok_or("no annotation")?
            .try_get::<String, _>("position")?,
    )?;
    assert_eq!(actual.text, "a".repeat(900 * 1024));
    drop(owner);
    source.close().await
}

#[tokio::test]
async fn individually_oversize_annotation_is_refused_with_its_originating_commit() -> TestResult {
    use fleet_sync_protocol::unit_budget::UnitTooLarge;
    let source = SenderFixture::open().await?;
    let (after, captured) = last_capture(source.database.as_ref()).await?;
    let result = crate::piece_builder::cut_transaction(
        source.database.database(),
        captured.position.epoch,
        after,
        &captured,
        NonZeroUsize::MIN.saturating_add(999),
        &Annotator {
            bytes: 2 * 1024 * 1024,
        },
    );
    if let Ok(parts) = &result {
        eprintln!(
            "oversize annotation produced {} units and {} encoded bytes",
            parts.len(),
            serde_json::to_vec(parts)?.len()
        );
    }
    assert!(
        matches!(result, Err(SenderError::UnitTooLarge(UnitTooLarge::Annotation { commit_id, encoded_size }))
        if commit_id == captured.position.sequence && encoded_size > 1536 * 1024),
        "oversize annotation was emitted or lost its origin"
    );
    source.close().await
}

#[tokio::test]
async fn first_rows_above_target_keep_a_fitting_final_annotation_packed() -> TestResult {
    let source = SenderFixture::open().await?;
    source.migrate_copies().await?;
    let mut write = source.database.begin_write().await?;
    query::<Turso>(
        "UPDATE text_records SET label=? WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .bind("r".repeat(1100 * 1024))
    .execute(&mut write)
    .await?;
    query::<Turso>("INSERT INTO text_records VALUES('019eef00-0000-7000-8000-000000000010',16,?)")
        .bind("s".repeat(1100 * 1024))
        .execute(&mut write)
        .await?;
    write.commit().await?;
    let measured = Arc::new(MeasuredTransport::new(source.transport.clone(), false));
    let sender = ReplicationSender::new(
        source.database.clone(),
        Arc::new(Annotator { bytes: 64 * 1024 }),
        source.marks.clone(),
        measured.clone(),
        SenderOptions::default(),
    );
    sender.replicate_once(source.first).await?;
    let units = measured.observed.lock().await;
    assert_eq!(
        units.len(),
        2,
        "a fitting annotation must stay with its final row"
    );
    for (index, unit) in units.iter().enumerate() {
        assert_eq!(unit.rows, 1);
        assert_eq!(unit.annotated, index == 1);
        assert!(unit.framed_bytes > 1024 * 1024 && unit.framed_bytes <= 1536 * 1024);
        assert!(
            matches!(&unit.part, Some(part) if part.count==2 && usize::try_from(part.index)==Ok(index))
        );
    }
    drop(units);
    let file = source
        .transport
        .files
        .get(&source.first)
        .ok_or("missing copy")?;
    let mut owner = file.connection.lock().await;
    let connection = owner.as_mut().ok_or("closed copy")?;
    let rows = query::<Turso>("SELECT length(label) AS bytes FROM text_records ORDER BY id")
        .fetch_all(&mut *connection)
        .await?;
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(row.try_get::<i64, _>("bytes")?, 1100 * 1024);
    }
    let count: i64 =
        query::<Turso>("SELECT count(*) AS rows FROM hook_calls WHERE position LIKE '{%'")
            .fetch_one(&mut *connection)
            .await?
            .try_get("rows")?;
    assert_eq!(count, 1);
    drop(owner);
    source.close().await
}
