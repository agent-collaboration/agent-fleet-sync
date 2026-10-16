//! Receiver outcomes through public APIs over the real Turso transaction boundary.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use fleet_sync_protocol::*;
use sqlx_core::{connection::Connection, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn repeated_piece_returns_original_position_without_repeating_hook_writes() -> TestResult {
    // Wrong behavior: replaying an earlier piece repeats consumer writes or returns the latest ack.
    let mut connection = connection().await?;
    for commit in 1..=2 {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = apply_piece(
            &mut TursoScope(&mut transaction),
            &piece(commit, commit - 1)?,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(100),
        )
        .await?;
        assert!(
            matches!(result, ApplyDecision::Applied { through, ref follow_ups } if through == position(commit)? && follow_ups == &["deliver notice"])
        );
        transaction.commit().await?;
    }
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let result = apply_piece(
        &mut TursoScope(&mut transaction),
        &piece(1, 0)?,
        &writer()?,
        &Hook { refuse: true },
        SyncTime::from_unix_millis(101),
    )
    .await?;
    assert!(matches!(result, ApplyDecision::AlreadyApplied { through } if through == position(1)?));
    assert_eq!(
        ready_status(copy_status(&mut TursoScope(&mut transaction)).await?)?.acked,
        position(2)?
    );
    transaction.commit().await?;
    assert_eq!(
        counts(&mut connection).await?,
        ("1".into(), "2".into(), "2".into())
    );
    let row = sqlx_core::query::query::<Turso>("SELECT CAST(id AS TEXT) AS id,body FROM records")
        .fetch_one(&mut connection)
        .await?;
    assert_eq!(row.try_get::<String, _>("id")?, "9007199254740993");
    assert_eq!(row.try_get::<String, _>("body")?, "commit 2");
    Ok(())
}

#[tokio::test]
async fn hook_refusal_rolls_back_rows_receipts_and_hook_writes_then_resend_applies() -> TestResult {
    let mut connection = connection().await?;
    let incoming = piece(1, 0)?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let result = apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &writer()?,
        &Hook { refuse: true },
        SyncTime::from_unix_millis(100),
    )
    .await;
    assert!(matches!(
        result,
        Err(ApplyFailure::Refused(PieceRefusal::Hook(_)))
    ));
    transaction.rollback().await?;
    assert_eq!(
        counts(&mut connection).await?,
        ("0".into(), "0".into(), "0".into())
    );
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        ready_status(copy_status(&mut TursoScope(&mut transaction)).await?)?.acked,
        position(0)?
    );
    assert!(matches!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(101)
        )
        .await?,
        ApplyDecision::Applied { .. }
    ));
    transaction.commit().await?;
    assert_eq!(
        counts(&mut connection).await?,
        ("1".into(), "1".into(), "1".into())
    );
    Ok(())
}

#[tokio::test]
async fn wrong_sender_digest_conflict_gap_and_unsupported_schema_leave_no_writes() -> TestResult {
    let mut connection = connection().await?;
    let foreign = PeerId::new("019f0000-0000-7000-8000-000000000013".parse()?)?;
    let mut wrong_digest = piece(1, 0)?;
    wrong_digest.transactions.clear();
    let schema = Piece::new(PieceContents {
        transactions: vec![ReplicatedTransaction::SchemaStep {
            commit_id: CommitSequence::new(1),
            migration_version: 1,
            statements: vec![
                "CREATE TRIGGER bad AFTER INSERT ON records BEGIN DELETE FROM records; END".into(),
            ],
        }],
        ..PieceContents {
            database: database()?,
            epoch: Epoch::new(1)?,
            continues_from: position(0)?,
            through: position(1)?,
            transactions: Vec::new(),
            part: None,
            annotation: None,
        }
    })?;
    let cases = [
        (
            piece(1, 0)?,
            foreign,
            PieceRefusal::NotWriter {
                current: Some(Epoch::new(1)?),
            },
        ),
        (wrong_digest, writer()?, PieceRefusal::PieceConflict),
        (
            piece(2, 1)?,
            writer()?,
            PieceRefusal::Gap {
                acked: position(0)?,
            },
        ),
        (
            schema,
            writer()?,
            PieceRefusal::SchemaUnsupported {
                form: SchemaStatementForm::CreateTrigger,
            },
        ),
    ];
    for (incoming, sender, expected) in cases {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        assert_eq!(
            apply_piece(
                &mut TursoScope(&mut transaction),
                &incoming,
                &sender,
                &Hook { refuse: false },
                SyncTime::from_unix_millis(100)
            )
            .await
            .err(),
            Some(ApplyFailure::Refused(expected))
        );
        transaction.rollback().await?;
    }
    assert_eq!(
        counts(&mut connection).await?,
        ("0".into(), "0".into(), "0".into())
    );
    Ok(())
}

#[tokio::test]
async fn split_transaction_stays_invisible_until_final_and_failed_final_discards_separately()
-> TestResult {
    // Wrong behavior: exposing a prefix, retaining a poisoned partial set, or repeating the final hook.
    let mut connection = connection().await?;
    let whole = ReplicatedTransaction::Rows {
        commit_id: CommitSequence::new(1),
        rows: vec![
            RowChange::upsert(
                "records",
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(1), SqlValue::Text("first".into())],
            )?,
            RowChange::upsert(
                "records",
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(2), SqlValue::Text("second".into())],
            )?,
        ],
    };
    let ReplicatedTransaction::Rows { ref rows, .. } = whole else {
        return Err("expected rows".into());
    };
    let key = TransactionKey {
        epoch: Epoch::new(1)?,
        commit_id: CommitSequence::new(1),
    };
    let mut pieces = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        pieces.push(Piece::<()>::new(PieceContents {
            database: database()?,
            epoch: Epoch::new(1)?,
            continues_from: position(0)?,
            through: position(1)?,
            transactions: vec![ReplicatedTransaction::Rows {
                commit_id: CommitSequence::new(1),
                rows: vec![row.clone()],
            }],
            part: Some(TransactionPart::new(
                key,
                u32::try_from(index)?,
                2,
                PieceDigest::of_transaction(&whole)?,
            )?),
            annotation: None,
        })?);
    }
    let first = pieces.first().ok_or("first part")?;
    let last = pieces.last().ok_or("final part")?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            first,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(100)
        )
        .await?,
        ApplyDecision::PartStaged { index: 0, .. }
    ));
    transaction.commit().await?;
    assert_eq!(
        counts(&mut connection).await?,
        ("0".into(), "0".into(), "0".into())
    );
    let mut broken = last.clone();
    broken.transactions = vec![ReplicatedTransaction::Rows {
        commit_id: CommitSequence::new(1),
        rows: Vec::new(),
    }];
    broken.digest = broken.recomputed_digest()?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(
        matches!(apply_piece(&mut TursoScope(&mut transaction),&broken,&writer()?,&Hook { refuse:false },SyncTime::from_unix_millis(101)).await,Err(ApplyFailure::RefusedDiscardingParts { transaction,refusal:PieceRefusal::Gap { .. } }) if transaction == key)
    );
    transaction.rollback().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    discard_staged_parts(&mut TursoScope(&mut transaction), &key).await?;
    transaction.commit().await?;
    let row = sqlx_core::query::query::<Turso>(
        "SELECT CAST(count(*) AS TEXT) AS parts FROM _fleet_sync_staged_parts",
    )
    .fetch_one(&mut connection)
    .await?;
    assert_eq!(row.try_get::<String, _>("parts")?, "0");
    for incoming in &pieces {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        apply_piece(
            &mut TursoScope(&mut transaction),
            incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(102),
        )
        .await?;
        transaction.commit().await?;
    }
    assert_eq!(
        counts(&mut connection).await?,
        ("2".into(), "1".into(), "2".into())
    );
    for incoming in &pieces {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        assert!(
            matches!(apply_piece(&mut TursoScope(&mut transaction),incoming,&writer()?,&Hook { refuse:true },SyncTime::from_unix_millis(103)).await?,ApplyDecision::AlreadyApplied { through } if through == position(1)?)
        );
        transaction.commit().await?;
    }
    Ok(())
}

#[tokio::test]
async fn fencing_preserves_old_holder_cutoff_and_retirement_dominates_replays() -> TestResult {
    let mut connection = connection().await?;
    let old_writer = writer()?;
    let next_writer = PeerId::new("019f0000-0000-7000-8000-000000000014".parse()?)?;
    let incoming = piece(1, 0)?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &old_writer,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(100),
    )
    .await?;
    transaction.commit().await?;
    let request = FenceRequest {
        database: database()?,
        transfer_id: TransferId::new("019f0000-0000-7000-8000-000000000015".parse()?)?,
        new_epoch: Epoch::new(2)?,
        expect: FenceExpectation {
            epoch: Epoch::new(1)?,
            acked: position(1)?,
        },
        target_schema_max: 0,
        holder: next_writer,
    };
    let expected = FenceResult::Fenced {
        old_epoch: Epoch::new(1)?,
        new_epoch: Epoch::new(2)?,
        start: position(1)?,
    };
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        fence(&mut TursoScope(&mut transaction), &request).await?,
        expected
    );
    assert_eq!(
        fence(&mut TursoScope(&mut transaction), &request).await?,
        expected
    );
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &old_writer,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(101)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::StaleEpoch {
            newer: Epoch::new(2)?,
            ended_at: position(1)?
        }))
    );
    transaction.rollback().await?;
    let retire = RetireCopy {
        database: database()?,
        expect_fence: Epoch::new(2)?,
        expect_acked: position(1)?,
    };
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        retire_copy(&mut TursoScope(&mut transaction), &retire).await?,
        RetireCopyAnswer::Retired
    );
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        fence(&mut TursoScope(&mut transaction), &request).await?,
        FenceResult::Retired
    );
    assert_eq!(
        retire_copy(&mut TursoScope(&mut transaction), &retire).await?,
        RetireCopyAnswer::Retired
    );
    assert_eq!(
        copy_status(&mut TursoScope(&mut transaction)).await?,
        CopyStatusAnswer::Retired
    );
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &old_writer,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(102)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::Retired))
    );
    assert_eq!(
        create_copy(
            &mut TursoScope(&mut transaction),
            &CreateCopy {
                database: database()?,
                epoch: Epoch::new(1)?,
                holder: Some(old_writer),
                origin: CopyOrigin::Empty
            }
        )
        .await?,
        CreateCopyAnswer::Retired
    );
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn snapshot_pages_use_primary_key_cursor_and_stop_when_position_moves() -> TestResult {
    let mut connection = connection().await?;
    let rows = (0..260)
        .rev()
        .map(|index| {
            RowChange::upsert(
                "records",
                vec![SqlValue::Integer(index)],
                vec![
                    SqlValue::Integer(index),
                    SqlValue::Text(format!("row {index}")),
                ],
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: CommitSequence::new(1),
            rows,
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(100),
    )
    .await?;
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut cursor = None;
    let mut received = Vec::new();
    let record_cursor = loop {
        let SnapshotRead::Page(page) =
            snapshot_page(&mut TursoScope(&mut transaction), cursor).await?
        else {
            return Err("unexpected move".into());
        };
        if page.table == "records" {
            assert_eq!(page.rows.len(), 256);
            received.extend(page.rows.iter().filter_map(|row| row.first().cloned()));
            break page.next;
        }
        cursor = page.next;
        if cursor.is_none() {
            return Err("records absent".into());
        }
    };
    let SnapshotRead::Page(page) =
        snapshot_page(&mut TursoScope(&mut transaction), record_cursor.clone()).await?
    else {
        return Err("unexpected move".into());
    };
    received.extend(page.rows.iter().filter_map(|row| row.first().cloned()));
    assert_eq!(
        received,
        (0..260).map(SqlValue::Integer).collect::<Vec<_>>()
    );
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &piece(2, 1)?,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(101),
    )
    .await?;
    assert!(
        matches!(snapshot_page(&mut TursoScope(&mut transaction),record_cursor).await?,SnapshotRead::Moved { position:current } if current == position(2)?)
    );
    transaction.commit().await?;
    Ok(())
}
