//! Persisted integrity and whole-piece rejection through the real receiver boundary.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use fleet_sync_protocol::*;
use sqlx_core::{connection::Connection, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn changed_receipt_content_conflicts_and_missing_receipt_never_acknowledges_a_gap()
-> TestResult {
    let mut connection = connection().await?;
    let incoming = piece(1, 0)?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(1),
    )
    .await?;
    transaction.commit().await?;
    let changed = Piece::new(PieceContents {
        database: incoming.database,
        epoch: incoming.epoch,
        continues_from: incoming.continues_from,
        through: incoming.through,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: incoming.through.sequence,
            rows: vec![RowChange::upsert(
                "records",
                vec![uuid_key(1)],
                vec![uuid_key(1), SqlValue::Text("changed".into())],
            )?],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &changed,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(2)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::PieceConflict))
    );
    transaction.rollback().await?;
    sqlx_core::query::query::<Turso>("DELETE FROM _fleet_sync_receipts")
        .execute(&mut connection)
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(3)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::Gap {
            acked: position(1)?
        }))
    );
    transaction.rollback().await?;
    assert_eq!(
        counts(&mut connection).await?,
        ("1".into(), "1".into(), "0".into())
    );
    Ok(())
}

#[tokio::test]
async fn late_row_constraint_rolls_back_schema_membership_rows_and_position_together() -> TestResult
{
    let mut connection = connection().await?;
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(2)?,
        transactions: vec![
            ReplicatedTransaction::SchemaStep {
                commit_id: position(1)?.sequence,
                migration_version: 2,
                statements: vec![
                    "CREATE TABLE required_rows(id TEXT NOT NULL PRIMARY KEY, body TEXT NOT NULL)"
                        .into(),
                ],
            },
            ReplicatedTransaction::Rows {
                commit_id: position(2)?.sequence,
                rows: vec![
                    RowChange::upsert(
                        "required_rows",
                        vec![uuid_key(1)],
                        vec![uuid_key(1), SqlValue::Text("first".into())],
                    )?,
                    RowChange::upsert(
                        "required_rows",
                        vec![uuid_key(2)],
                        vec![uuid_key(2), SqlValue::Null],
                    )?,
                ],
            },
        ],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1)
        )
        .await,
        Err(ApplyFailure::Storage(StoreError::Constraint { .. }))
    ));
    transaction.rollback().await?;
    let row = sqlx_core::query::query::<Turso>("SELECT CAST((SELECT count(*) FROM sqlite_schema WHERE name='required_rows') AS TEXT) AS tables, CAST((SELECT count(*) FROM _fleet_sync_copy_tables WHERE name='required_rows') AS TEXT) AS members").fetch_one(&mut connection).await?;
    assert_eq!(row.try_get::<String, _>("tables")?, "0");
    assert_eq!(row.try_get::<String, _>("members")?, "0");
    assert_eq!(
        ready_status(copy_status(&mut TursoScope(&mut connection)).await?)?.acked,
        position(0)?
    );
    assert_eq!(
        counts(&mut connection).await?,
        ("0".into(), "0".into(), "1".into())
    );
    Ok(())
}

#[tokio::test]
async fn protected_schema_is_rejected_before_earlier_rows_and_local_tables_cannot_receive_rows()
-> TestResult {
    let mut connection = connection().await?;
    for statement in [
        "CREATE TABLE _FLEET_SYNC_forged(id INTEGER PRIMARY KEY)",
        "CREATE TABLE sqlite_forged(id INTEGER PRIMARY KEY)",
    ] {
        let incoming = Piece::<()>::new(PieceContents {
            database: database()?,
            epoch: Epoch::new(1)?,
            continues_from: position(0)?,
            through: position(2)?,
            transactions: vec![
                piece(1, 0)?.transactions.remove(0),
                ReplicatedTransaction::SchemaStep {
                    commit_id: position(2)?.sequence,
                    migration_version: 2,
                    statements: vec![statement.into()],
                },
            ],
            part: None,
            annotation: None,
        })?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        assert!(matches!(
            apply_piece(
                &mut TursoScope(&mut transaction),
                &incoming,
                &writer()?,
                &Hook { refuse: false },
                SyncTime::from_unix_millis(1)
            )
            .await,
            Err(ApplyFailure::Refused(
                PieceRefusal::SchemaUnsupported { .. }
            ))
        ));
        transaction.rollback().await?;
    }
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: position(1)?.sequence,
            rows: vec![RowChange::upsert(
                "hook_records",
                vec![SqlValue::Text("forged".into())],
                vec![SqlValue::Text("forged".into())],
            )?],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1)
        )
        .await,
        Err(ApplyFailure::Storage(StoreError::Constraint { .. }))
    ));
    transaction.rollback().await?;
    assert_eq!(
        counts(&mut connection).await?,
        ("0".into(), "0".into(), "1".into())
    );
    Ok(())
}

#[tokio::test]
async fn corrupt_copy_records_fail_closed_for_reads_and_apply() -> TestResult {
    let mut connection = connection().await?;
    for record in ["not json", "{}", r#"{"state":"unknown","copy":{}}"#] {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        sqlx_core::query::query::<Turso>("UPDATE _fleet_sync_copy SET record=?")
            .bind(record.to_owned())
            .execute(&mut *transaction)
            .await?;
        assert!(matches!(
            copy_status(&mut TursoScope(&mut transaction)).await,
            Err(ReceiverError::Storage(StoreError::Corrupt { .. }))
        ));
        assert!(matches!(
            apply_piece(
                &mut TursoScope(&mut transaction),
                &piece(1, 0)?,
                &writer()?,
                &Hook { refuse: false },
                SyncTime::from_unix_millis(1)
            )
            .await,
            Err(ApplyFailure::Storage(StoreError::Corrupt { .. }))
        ));
        transaction.rollback().await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_mixed_case_reserved_row_target_cannot_change_copy_bookkeeping() -> TestResult {
    let mut connection = connection().await?;
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: position(1)?.sequence,
            rows: vec![RowChange::upsert(
                "_FLEET_SYNC_copy",
                vec![uuid_key(1)],
                vec![uuid_key(1), SqlValue::Text("forged copy".into())],
            )?],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1)
        )
        .await,
        Err(ApplyFailure::Storage(StoreError::Constraint { .. }))
    ));
    transaction.rollback().await?;
    assert_eq!(
        ready_status(copy_status(&mut TursoScope(&mut connection)).await?)?.acked,
        position(0)?
    );
    assert_eq!(
        counts(&mut connection).await?,
        ("0".into(), "0".into(), "1".into())
    );
    Ok(())
}

#[tokio::test]
async fn absent_copy_and_future_epoch_are_distinct_refusals_with_no_mutation() -> TestResult {
    let mut connection = connection().await?;
    let future = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(2)?,
        continues_from: position(0)?,
        through: ReplicationPosition::new(Epoch::new(2)?, CommitSequence::new(1)),
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: CommitSequence::new(1),
            rows: vec![RowChange::upsert(
                "records",
                vec![uuid_key(1)],
                vec![uuid_key(1), SqlValue::Text("future".into())],
            )?],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &future,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::NotWriter {
            current: Some(Epoch::new(1)?)
        }))
    );
    transaction.rollback().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_core::query::query::<Turso>("DELETE FROM _fleet_sync_copy")
        .execute(&mut *transaction)
        .await?;
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &piece(1, 0)?,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::NotInitialized))
    );
    transaction.rollback().await?;
    assert_eq!(
        counts(&mut connection).await?,
        ("0".into(), "0".into(), "1".into())
    );
    Ok(())
}
