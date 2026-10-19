#![allow(clippy::panic_in_result_fn)]
use super::*;
use crate::turso_rule_fixture::*;
use fleet_sync_protocol::{CopyStatus, SqlValue};
use sqlx_core::{connection::Connection, executor::Executor, query::query};
use sqlx_turso::{Turso, TursoConnection};

async fn compare_page(
    connection: &mut TursoConnection,
    cursor: Option<SnapshotCursor>,
    status: CopyStatus,
) -> TestResult<Option<SnapshotCursor>> {
    let expected = sizing_oracle::snapshot_page_oracle(
        &mut TursoScope(connection),
        cursor.clone(),
        status.clone(),
    )
    .await;
    FULL_MEASURES.with(|count| count.set(0));
    let actual = snapshot_page_at(&mut TursoScope(connection), cursor, status).await;
    let calls = FULL_MEASURES.with(std::cell::Cell::get);
    match (expected, actual) {
        (Ok(SnapshotRead::Page(expected)), Ok(SnapshotRead::Page(actual))) => {
            assert!(
                calls <= 2,
                "growing copy page was fully measured {calls} times"
            );
            assert_eq!(serde_json::to_vec(&actual)?, serde_json::to_vec(&expected)?);
            assert_eq!(
                fleet_sync_protocol::PageDigest::of(&actual)?,
                fleet_sync_protocol::PageDigest::of(&expected)?
            );
            Ok(actual.next)
        }
        (Err(ReceiverError::UnitTooLarge(expected)), Err(ReceiverError::UnitTooLarge(actual))) => {
            assert_eq!(actual, expected, "refusal must preserve exact encoded_size");
            Ok(None)
        }
        (expected, actual) => {
            Err(format!("copy pager/oracle diverged: {expected:?} / {actual:?}").into())
        }
    }
}

#[tokio::test]
async fn generated_copy_pages_match_oracle_with_constant_full_measures() -> TestResult {
    let mut connection = connection().await?;
    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    let status = crate::snapshot_install::readable_status(&mut TursoScope(&mut tx)).await?;
    // Catalog membership is fixed by the fixture; use its real registered table.
    (&mut *tx).execute("WITH RECURSIVE numbers(value) AS(VALUES(0) UNION ALL SELECT value+1 FROM numbers WHERE value<299) INSERT INTO records SELECT printf('019eef00-0000-7000-8000-%012x',value),'placeholder' FROM numbers").await?;
    for index in 0..300 {
        let SqlValue::Text(key) = uuid_key(index) else {
            return Err("bad key".into());
        };
        query::<Turso>("UPDATE records SET body=? WHERE id=?")
            .bind(format!(
                "{index}:{}",
                "\"\\\n\t雪🦀".repeat(usize::try_from((index * 37) % 91)?)
            ))
            .bind(key)
            .execute(&mut *tx)
            .await?;
    }
    let next = compare_page(&mut tx, None, status.clone())
        .await?
        .ok_or("row-count cap not exercised")?;
    compare_page(&mut tx, Some(next), status.clone()).await?;
    compare_page(
        &mut tx,
        Some(SnapshotCursor {
            position: status.acked,
            table: "records".into(),
            after_key: vec![uuid_key(128)],
        }),
        status.clone(),
    )
    .await?;
    (&mut *tx).execute("DELETE FROM records").await?;
    compare_page(&mut tx, None, status.clone()).await?;
    // Exact target/hard boundaries with a last row (next=null).
    let SqlValue::Text(key) = uuid_key(1) else {
        return Err("bad key".into());
    };
    query::<Turso>("INSERT INTO records VALUES(?,'')")
        .bind(key)
        .execute(&mut *tx)
        .await?;
    let SnapshotRead::Page(base) =
        sizing_oracle::snapshot_page_oracle(&mut TursoScope(&mut tx), None, status.clone()).await?
    else {
        return Err("missing base page".into());
    };
    let overhead = measure_snapshot_unit(*base)?.encoded_size;
    for limit in [TARGET_UNIT_BYTES, MAX_UNIT_BYTES] {
        for offset in [-1_isize, 0, 1] {
            let desired = limit.checked_add_signed(offset).ok_or("bad boundary")?;
            query::<Turso>("UPDATE records SET body=?")
                .bind("x".repeat(desired - overhead))
                .execute(&mut *tx)
                .await?;
            let observed_size = match sizing_oracle::snapshot_page_oracle(
                &mut TursoScope(&mut tx),
                None,
                status.clone(),
            )
            .await
            {
                Ok(SnapshotRead::Page(page)) => measure_snapshot_unit(*page)?.encoded_size,
                Err(ReceiverError::UnitTooLarge(UnitTooLarge::Row { encoded_size, .. })) => {
                    encoded_size
                }
                other => return Err(format!("unexpected boundary result: {other:?}").into()),
            };
            assert_eq!(observed_size, desired, "fixture missed exact boundary");
            compare_page(&mut tx, None, status.clone()).await?;
        }
    }
    query::<Turso>("UPDATE records SET body=?")
        .bind("\"\\雪".repeat(90_000))
        .execute(&mut *tx)
        .await?;
    let SqlValue::Text(key) = uuid_key(2) else {
        return Err("bad key".into());
    };
    query::<Turso>("INSERT INTO records VALUES(?,?)")
        .bind(key)
        .bind("🦀\n".repeat(100_000))
        .execute(&mut *tx)
        .await?;
    let next = compare_page(&mut tx, None, status.clone())
        .await?
        .ok_or("missing byte-short continuation")?;
    compare_page(&mut tx, Some(next), status).await?;
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn final_copy_size_mismatch_is_corrupt() -> TestResult {
    let mut connection = connection().await?;
    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    let SnapshotRead::Page(page) = crate::snapshot_page(&mut TursoScope(&mut tx), None).await?
    else {
        return Err("missing page".into());
    };
    assert!(matches!(
        checked_page(*page, 0),
        Err(ReceiverError::Storage(StoreError::Corrupt { .. }))
    ));
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn copy_table_transitions_and_invalid_cursors_match_oracle() -> TestResult {
    use fleet_sync_protocol::*;
    let mut connection = connection().await?;
    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::SchemaStep {
            commit_id: position(1)?.sequence,
            migration_version: 2,
            statements: vec![
                "CREATE TABLE secondary(id TEXT NOT NULL PRIMARY KEY, body TEXT)".into(),
                "CREATE TABLE z_empty(id TEXT NOT NULL PRIMARY KEY)".into(),
            ],
        }],
        part: None,
        annotation: None,
    })?;
    crate::apply_piece(
        &mut TursoScope(&mut tx),
        &incoming,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(2),
    )
    .await?;
    let status = crate::snapshot_install::readable_status(&mut TursoScope(&mut tx)).await?;
    let SqlValue::Text(key) = uuid_key(7) else {
        return Err("bad key".into());
    };
    query::<Turso>("INSERT INTO secondary VALUES(?,?)")
        .bind(key)
        .bind("snow雪\\\"\n")
        .execute(&mut *tx)
        .await?;
    let next = compare_page(&mut tx, None, status.clone())
        .await?
        .ok_or("empty records table did not continue")?;
    assert_eq!(next.table, "secondary");
    let next = compare_page(&mut tx, Some(next), status.clone())
        .await?
        .ok_or("secondary table did not continue")?;
    assert_eq!(next.table, "z_empty");
    assert!(
        compare_page(&mut tx, Some(next), status.clone())
            .await?
            .is_none()
    );
    for cursor in [
        SnapshotCursor {
            position: position(0)?,
            table: "records".into(),
            after_key: vec![],
        },
        SnapshotCursor {
            position: status.acked,
            table: "unavailable".into(),
            after_key: vec![],
        },
        SnapshotCursor {
            position: status.acked,
            table: "records".into(),
            after_key: vec![SqlValue::Null],
        },
    ] {
        let expected = sizing_oracle::snapshot_page_oracle(
            &mut TursoScope(&mut tx),
            Some(cursor.clone()),
            status.clone(),
        )
        .await;
        let actual = snapshot_page_at(&mut TursoScope(&mut tx), Some(cursor), status.clone()).await;
        assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    }
    tx.rollback().await?;
    Ok(())
}
