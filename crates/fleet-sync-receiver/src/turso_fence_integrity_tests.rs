//! Fence refusal, durable receipt identity, and immutable historical cutoffs.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use fleet_sync_protocol::*;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_core::row::Row;
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};

fn request(id: u128, epoch: u64, expected: u64, holder: PeerId) -> TestResult<FenceRequest> {
    Ok(FenceRequest {
        database: database()?,
        transfer_id: TransferId::new(uuid::Uuid::from_u128(
            (id) | 0x019eef00000070008000000000000000,
        ))?,
        new_epoch: Epoch::new(epoch)?,
        expect: FenceExpectation {
            epoch: Epoch::new(expected)?,
            acked: position(0)?,
        },
        target_schema: Some(fleet_sync_protocol::SchemaSupport { max_version: 1 }),
        holder,
    })
}
#[tokio::test]
async fn fence_checks_forward_motion_exact_preview_and_schema_before_mutation() -> TestResult {
    let mut connection = connection().await?;
    let target = PeerId::new(uuid::Uuid::from_u128(0x019eef000000700080000000000001f5))?;
    let mut cases = Vec::new();
    cases.push((
        request(501, 1, 1, target)?,
        FenceResult::FenceNotAdvanced {
            current_epoch: Epoch::new(1)?,
        },
    ));
    let mut changed = request(502, 2, 1, target)?;
    changed.expect.acked = position(1)?;
    cases.push((
        changed,
        FenceResult::PreviewStale {
            acked: position(0)?,
        },
    ));
    let mut changed = request(503, 2, 1, target)?;
    changed.target_schema = Some(fleet_sync_protocol::SchemaSupport { max_version: 0 });
    let status = ready_status(copy_status(&mut TursoScope(&mut connection)).await?)?;
    cases.push((
        changed,
        FenceResult::SchemaTooNew {
            store: status.schema.clone(),
            supported: Some(0),
        },
    ));
    for (request, expected) in cases {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        assert_eq!(
            fence(&mut TursoScope(&mut transaction), &request).await?,
            expected
        );
        assert_eq!(
            ready_status(copy_status(&mut TursoScope(&mut transaction)).await?)?,
            status
        );
        transaction.commit().await?;
    }
    Ok(())
}

#[tokio::test]
async fn reopened_copy_keeps_old_fence_receipts_and_cutoffs_after_later_fences() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("copy.db");
    let mut connection = connection_with_options(
        TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true),
    )
    .await?;
    let second = PeerId::new(uuid::Uuid::from_u128(0x019eef000000700080000000000001f6))?;
    let third = PeerId::new(uuid::Uuid::from_u128(0x019eef000000700080000000000001f7))?;
    let first = request(511, 2, 1, second)?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let original = fence(&mut TursoScope(&mut transaction), &first).await?;
    transaction.commit().await?;
    drop(connection);
    let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let later_position = ReplicationPosition::new(Epoch::new(2)?, CommitSequence::new(1));
    let later = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(2)?,
        continues_from: position(0)?,
        through: later_position,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: CommitSequence::new(1),
            rows: vec![RowChange::upsert(
                "records",
                vec![uuid_key(8)],
                vec![uuid_key(8), SqlValue::Text("later epoch".into())],
            )?],
        }],
        part: None,
        annotation: None,
    })?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &later,
        &second,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(2),
    )
    .await?;
    let mut next = request(512, 3, 2, third)?;
    next.expect.acked = later_position;
    assert!(
        matches!(fence(&mut TursoScope(&mut transaction),&next).await?,FenceResult::Fenced{new_epoch,..} if new_epoch==Epoch::new(3)?)
    );
    assert_eq!(
        fence(&mut TursoScope(&mut transaction), &first).await?,
        original
    );
    assert_eq!(
        ready_status(copy_status(&mut TursoScope(&mut transaction)).await?)?.fence,
        Epoch::new(3)?
    );
    let mut changed = first.clone();
    changed.holder = third;
    assert_eq!(
        fence(&mut TursoScope(&mut transaction), &changed)
            .await
            .err(),
        Some(ReceiverError::FenceConflict)
    );
    let incoming = piece(1, 0)?;
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::StaleEpoch {
            newer: Epoch::new(3)?,
            ended_at: position(0)?
        }))
    );
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &second,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::NotWriter {
            current: Some(Epoch::new(3)?)
        }))
    );
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let damaged =
        sqlx_core::query::query::<Turso>("UPDATE _fleet_sync_fence_receipts SET result='{}'")
            .execute(&mut *transaction)
            .await?;
    assert_eq!(damaged.rows_affected(), 2);
    assert!(matches!(
        fence(&mut TursoScope(&mut transaction), &first).await,
        Err(ReceiverError::Storage(StoreError::Corrupt { .. }))
    ));
    transaction.rollback().await?;
    Ok(())
}

enum ReceiptDamage {
    OldEpoch,
    NewEpoch,
    Start,
    NonAdvancing,
    FutureStart,
}

async fn receipt_state(connection: &mut TursoConnection) -> TestResult<Vec<String>> {
    let rows=sqlx_core::query::query::<Turso>("SELECT 'copy:'||record AS stored_value FROM _fleet_sync_copy UNION ALL SELECT 'fence:'||transfer||'|'||request||'|'||old_epoch||'|'||coalesce(old_holder,'')||'|'||result FROM _fleet_sync_fence_receipts UNION ALL SELECT 'row:'||CAST(id AS TEXT)||'|'||body FROM records UNION ALL SELECT 'hook:'||value FROM hook_records UNION ALL SELECT 'receipt:'||identity||'|'||digest FROM _fleet_sync_receipts ORDER BY stored_value").fetch_all(connection).await?;
    Ok(rows
        .iter()
        .map(|row| row.try_get("stored_value"))
        .collect::<Result<_, _>>()?)
}

async fn reject_inconsistent_receipt(damage: ReceiptDamage) -> TestResult {
    let mut connection = connection().await?;
    let incoming = piece(1, 0)?;
    let mut first = request(
        2501,
        2,
        1,
        PeerId::new(uuid::Uuid::from_u128(0x019eef000000700080000000000009c6))?,
    )?;
    first.expect.acked = position(1)?;
    let mut later = request(
        2503,
        3,
        2,
        PeerId::new(uuid::Uuid::from_u128(0x019eef000000700080000000000009c8))?,
    )?;
    later.expect.acked = position(1)?;
    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut tx),
        &incoming,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(1),
    )
    .await?;
    let original = fence(&mut TursoScope(&mut tx), &first).await?;
    assert!(
        matches!(fence(&mut TursoScope(&mut tx),&later).await?,FenceResult::Fenced { new_epoch,.. } if new_epoch==Epoch::new(3)?)
    );
    assert_eq!(
        crate::copy_storage::CopyStorage(&mut TursoScope(&mut tx))
            .epoch_end(Epoch::new(2)?)
            .await?,
        Some((Some(first.holder), position(1)?))
    );
    assert_eq!(
        fence(&mut TursoScope(&mut tx), &later).await?,
        FenceResult::Fenced {
            old_epoch: Epoch::new(2)?,
            new_epoch: Epoch::new(3)?,
            start: position(1)?,
        }
    );
    assert_eq!(fence(&mut TursoScope(&mut tx), &first).await?, original);
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut tx),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(2)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::StaleEpoch {
            newer: Epoch::new(3)?,
            ended_at: position(1)?
        }))
    );
    tx.commit().await?;

    let mut damaged = original;
    let FenceResult::Fenced {
        old_epoch,
        new_epoch,
        start,
    } = &mut damaged
    else {
        return Err("fixture did not fence".into());
    };
    match damage {
        ReceiptDamage::OldEpoch => *old_epoch = Epoch::new(2)?,
        ReceiptDamage::NewEpoch => *new_epoch = Epoch::new(3)?,
        ReceiptDamage::Start => *start = position(10)?,
        ReceiptDamage::NonAdvancing => {
            first.new_epoch = Epoch::new(1)?;
            *new_epoch = Epoch::new(1)?;
        }
        ReceiptDamage::FutureStart => {
            let future = ReplicationPosition::new(Epoch::new(3)?, CommitSequence::new(1));
            first.expect.acked = future;
            *start = future;
        }
    }
    let changed = sqlx_core::query::query::<Turso>(
        "UPDATE _fleet_sync_fence_receipts SET request=?,result=? WHERE old_epoch='1'",
    )
    .bind(serde_json::to_string(&first)?)
    .bind(serde_json::to_string(&damaged)?)
    .execute(&mut connection)
    .await?;
    assert_eq!(changed.rows_affected(), 1);
    let before = receipt_state(&mut connection).await?;
    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        fence(&mut TursoScope(&mut tx), &first).await,
        Err(ReceiverError::Storage(StoreError::Corrupt { .. }))
    ));
    tx.commit().await?;
    assert_eq!(receipt_state(&mut connection).await?, before);

    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        apply_piece(
            &mut TursoScope(&mut tx),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(3)
        )
        .await,
        Err(ApplyFailure::Storage(StoreError::Corrupt { .. }))
    ));
    tx.commit().await?;
    assert_eq!(receipt_state(&mut connection).await?, before);
    Ok(())
}

#[tokio::test]
async fn valid_shaped_wrong_old_epoch_is_corrupt_for_fence_replay_and_stale_apply() -> TestResult {
    reject_inconsistent_receipt(ReceiptDamage::OldEpoch).await
}
#[tokio::test]
async fn valid_shaped_wrong_new_epoch_is_corrupt_for_fence_replay_and_stale_apply() -> TestResult {
    reject_inconsistent_receipt(ReceiptDamage::NewEpoch).await
}
#[tokio::test]
async fn valid_shaped_wrong_start_is_corrupt_for_fence_replay_and_stale_apply() -> TestResult {
    reject_inconsistent_receipt(ReceiptDamage::Start).await
}
#[tokio::test]
async fn nonadvancing_saved_transition_is_corrupt_for_fence_replay_and_stale_apply() -> TestResult {
    reject_inconsistent_receipt(ReceiptDamage::NonAdvancing).await
}
#[tokio::test]
async fn future_epoch_start_is_corrupt_even_when_the_saved_request_agrees() -> TestResult {
    reject_inconsistent_receipt(ReceiptDamage::FutureStart).await
}
