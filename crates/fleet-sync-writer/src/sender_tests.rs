#![allow(clippy::panic_in_result_fn)]
use crate::{
    ReplicationEvents, SendFailure, SenderError,
    sender_test_fixture::{SenderFixture, TestResult},
};
use sqlx_core::{executor::Executor, query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn capture_off_bookkeeping_executor_refuses_replicated_and_engine_writes() -> TestResult {
    use crate::{MaintenanceTransaction, StatementForm, WriteRefusal};
    use sqlx_core::connection::Connection;
    let fixture = SenderFixture::open().await?;
    {
        let control = fixture.database.control.lock().await;
        let mut connection = fixture.database.maintenance.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut scope =
            MaintenanceTransaction::new(&mut transaction, control.statements.layouts().to_vec());
        for statement in [
            "UPDATE text_records SET amount=99",
            "DELETE FROM text_records",
            "INSERT INTO text_records VALUES('bad',99,'bypass')",
        ] {
            let error = (&mut scope)
                .execute(statement)
                .await
                .err()
                .ok_or("maintenance bypassed capture")?;
            assert!(matches!(
                WriteRefusal::from_sqlx(error),
                WriteRefusal::StatementRefused {
                    form: StatementForm::ReplicatedTableOnMaintenance { .. }
                }
            ));
        }
        for statement in [
            "DELETE FROM _fleet_sync_writer",
            "PRAGMA capture_data_changes_conn('full')",
            "CREATE TABLE bypass(id TEXT PRIMARY KEY)",
            "COMMIT",
        ] {
            assert!(
                (&mut scope).execute(statement).await.is_err(),
                "{statement}"
            );
        }
        query::<Turso>("INSERT INTO local_marks(id) VALUES(?)")
            .bind("allowed local bookkeeping")
            .persistent(false)
            .execute(&mut scope)
            .await?;
        transaction.commit().await?;
    }
    assert_eq!(
        query::<Turso>("SELECT amount FROM text_records")
            .fetch_one(fixture.database.reader())
            .await?
            .try_get::<i64, _>("amount")?,
        9_007_199_254_740_993
    );
    assert!(
        query::<Turso>("SELECT change_id FROM turso_cdc WHERE table_name='local_marks'")
            .fetch_all(fixture.database.reader())
            .await?
            .is_empty()
    );
    fixture.close().await
}

#[tokio::test]
async fn lost_final_answer_replays_parts_without_repeating_rows_hooks_or_bookkeeping() -> TestResult
{
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    let mut write = fixture.database.begin_write().await?;
    (&mut write).execute("UPDATE text_records SET id='changed'; UPDATE composite_records SET account='changed',sequence=2; UPDATE integer_records SET id=11;").await?;
    write.commit().await?;
    *fixture.transport.lose_answer.lock().await = Some(fixture.first);
    assert!(matches!(
        fixture.sender.replicate_once(fixture.first).await,
        Err(SenderError::Send(SendFailure::AnswerLost))
    ));
    let other = fixture.sender.replicate_once(fixture.second).await?;
    assert!(
        other
            .iter()
            .any(|event| matches!(event, ReplicationEvents::Acknowledged { .. }))
    );
    let retry = fixture.sender.replicate_once(fixture.first).await?;
    assert!(
        retry
            .iter()
            .any(|event| matches!(event, ReplicationEvents::Acknowledged { .. }))
    );
    for file in fixture.transport.files.values() {
        let mut connection = file.connection.lock().await;
        let connection = connection.as_mut().ok_or("closed copy")?;
        assert_eq!(
            query::<Turso>("SELECT id FROM text_records")
                .fetch_one(&mut *connection)
                .await?
                .try_get::<String, _>("id")?,
            "changed"
        );
        assert_eq!(
            query::<Turso>("SELECT id FROM integer_records")
                .fetch_one(&mut *connection)
                .await?
                .try_get::<i64, _>("id")?,
            11
        );
        assert_eq!(
            query::<Turso>("SELECT position FROM _fleet_sync_test_hooks")
                .fetch_all(&mut *connection)
                .await?
                .len(),
            3
        );
        assert!(
            query::<Turso>("SELECT id FROM text_records WHERE id='initial'")
                .fetch_optional(&mut *connection)
                .await?
                .is_none()
        );
    }
    assert_eq!(
        query::<Turso>("SELECT id FROM local_marks")
            .fetch_all(fixture.database.reader())
            .await?
            .len(),
        3
    );
    fixture.sender.replicate_once(fixture.first).await?;
    let rows = query::<Turso>("SELECT change_type FROM turso_cdc")
        .fetch_all(fixture.database.reader())
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.first()
            .ok_or("no retained marker")?
            .try_get::<i64, _>("change_type")?,
        2
    );
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute(
            "INSERT INTO text_records VALUES('after prune',-9007199254740993,'retained layout')",
        )
        .await?;
    write.commit().await?;
    for copy in [fixture.first, fixture.second] {
        fixture.sender.replicate_once(copy).await?;
    }
    for file in fixture.transport.files.values() {
        let mut connection = file.connection.lock().await;
        let connection = connection.as_mut().ok_or("closed copy")?;
        let row = query::<Turso>(
            "SELECT amount,typeof(amount) AS class FROM text_records WHERE id='after prune'",
        )
        .fetch_one(&mut *connection)
        .await?;
        assert_eq!(row.try_get::<i64, _>("amount")?, -9_007_199_254_740_993);
        assert_eq!(row.try_get::<String, _>("class")?, "integer");
    }
    fixture.close().await
}

#[tokio::test]
async fn a_pending_network_call_holds_neither_the_writer_nor_another_copy() -> TestResult {
    use crate::sender_test_fixture::BlockedSend;
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute("UPDATE text_records SET label='first pending'")
        .await?;
    write.commit().await?;
    let (entered, observed) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *fixture.transport.blocked.lock().await = Some(BlockedSend {
        copy: fixture.first,
        entered,
        resume: resumed,
    });
    let sender = fixture.sender.clone();
    let first = fixture.first;
    let pending = tokio::spawn(async move { sender.replicate_once(first).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), observed).await??;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut write = fixture.database.begin_write().await?;
        (&mut write)
            .execute("UPDATE text_records SET label='while network waits'")
            .await?;
        write.commit().await?;
        fixture.sender.replicate_once(fixture.second).await?;
        fixture.sender.replicate_once(fixture.second).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    resume.send(()).map_err(|_| "network worker disappeared")?;
    pending.await??;
    fixture.close().await
}

#[tokio::test]
async fn bounded_retention_marks_a_lagging_copy_before_releasing_its_history() -> TestResult {
    use crate::{ReplicationSender, Retention, SenderOptions};
    use fleet_sync_protocol::{CopyRole, ReceiverId};
    use std::{sync::Arc, time::Duration};
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    let before = fixture
        .sender
        .copies()
        .await?
        .into_iter()
        .find(|copy| copy.copy == fixture.first)
        .ok_or("no first copy")?
        .acked;
    let absent = ReceiverId::new(durable_steps::Uuid::from_u128(9))?;
    fixture
        .sender
        .add_copy(absent, CopyRole::Follower, before)
        .await?;
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute("UPDATE text_records SET label='new data'")
        .await?;
    write.commit().await?;
    for copy in [fixture.first, fixture.second] {
        fixture.sender.replicate_once(copy).await?;
    }
    let bounded = ReplicationSender::new(
        fixture.database.clone(),
        Arc::new(()),
        fixture.marks.clone(),
        fixture.transport.clone(),
        SenderOptions {
            retention: Retention::Bounded {
                max_age: Duration::MAX,
                max_changes: 0,
            },
            ..SenderOptions::default()
        },
    );
    let events = bounded.replicate_once(absent).await?;
    assert!(
        events.iter().any(
            |event| matches!(event,ReplicationEvents::CopyNeedsSnapshot{copy} if *copy==absent)
        )
    );
    let copies = bounded.copies().await?;
    assert!(
        copies
            .iter()
            .find(|copy| copy.copy == absent)
            .ok_or("no absent copy")?
            .needs_snapshot
    );
    assert!(
        !copies
            .iter()
            .find(|copy| copy.copy == fixture.first)
            .ok_or("no healthy copy")?
            .needs_snapshot
    );
    let rows = query::<Turso>("SELECT change_type FROM turso_cdc")
        .fetch_all(fixture.database.reader())
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.first()
            .ok_or("no retained marker")?
            .try_get::<i64, _>("change_type")?,
        2
    );
    fixture.close().await
}

#[tokio::test]
async fn fencing_records_the_rejected_tail_and_preserves_it_until_consumer_acknowledgement()
-> TestResult {
    use crate::{
        ReplicationSender, Retention, SenderOptions, WriteRefusal, writer_scope::WriterScope,
    };
    use fleet_sync_protocol::{
        CopyStatusAnswer, Epoch, FenceExpectation, FenceRequest, FenceResult, PeerId, TransferId,
    };
    use sqlx_core::connection::Connection;
    use std::{sync::Arc, time::Duration};
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    let ended_at;
    {
        let mut connection = fixture
            .transport
            .files
            .get(&fixture.first)
            .ok_or("no first file")?
            .connection
            .lock()
            .await;
        let connection = connection.as_mut().ok_or("closed copy")?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let CopyStatusAnswer::Status(status) =
            fleet_sync_receiver::copy_status(&mut WriterScope(&mut transaction)).await?
        else {
            return Err("no copy status".into());
        };
        ended_at = status.acked;
        let result = fleet_sync_receiver::fence(
            &mut WriterScope(&mut transaction),
            &FenceRequest {
                database: fixture.database.database(),
                transfer_id: TransferId::new(durable_steps::Uuid::from_u128(12))?,
                new_epoch: Epoch::new(2)?,
                expect: FenceExpectation {
                    epoch: Epoch::new(1)?,
                    acked: status.acked,
                },
                target_schema_max: 2,
                holder: PeerId::new(durable_steps::Uuid::from_u128(13))?,
            },
        )
        .await?;
        assert!(matches!(result, FenceResult::Fenced { .. }));
        transaction.commit().await?;
    }
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute("UPDATE text_records SET label='rejected tail remains readable'")
        .await?;
    write.commit().await?;
    let events = fixture.sender.replicate_once(fixture.first).await?;
    assert!(events.iter().any(|event|matches!(event,ReplicationEvents::RejectedTail(tail) if tail.ended_at==ended_at && tail.last_commit>ended_at)));
    assert!(matches!(
        fixture.database.begin_write().await,
        Err(WriteRefusal::Superseded { .. })
    ));
    let bounded = Arc::new(ReplicationSender::new(
        fixture.database.clone(),
        Arc::new(()),
        fixture.marks.clone(),
        fixture.transport.clone(),
        SenderOptions {
            retention: Retention::Bounded {
                max_age: Duration::MAX,
                max_changes: 0,
            },
            ..SenderOptions::default()
        },
    ));
    bounded.replicate_once(fixture.second).await?;
    assert!(
        query::<Turso>("SELECT change_id FROM turso_cdc WHERE table_name='text_records'")
            .fetch_optional(fixture.database.reader())
            .await?
            .is_some()
    );
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let (events, mut observed) = tokio::sync::mpsc::channel(16);
    let runner = bounded.clone();
    let task = tokio::spawn(async move { runner.run(stopped, events).await });
    let replayed = tokio::time::timeout(Duration::from_secs(5), observed.recv())
        .await?
        .ok_or("no pending tail")??;
    assert!(matches!(replayed, ReplicationEvents::RejectedTail(_)));
    stop.send(true)?;
    task.await??;
    bounded.acknowledge_rejected_tail(Epoch::new(1)?).await?;
    assert!(bounded.rejected_tails().await?.is_empty());
    fixture.close().await
}

#[tokio::test]
async fn bookkeeping_failure_rolls_back_its_write_and_prevents_any_send() -> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture.marks.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(
        fixture.sender.replicate_once(fixture.first).await,
        Err(SenderError::Bookkeeping(_))
    ));
    assert_eq!(fixture.transport.calls.load(Ordering::SeqCst), 0);
    assert!(
        query::<Turso>("SELECT id FROM local_marks")
            .fetch_all(fixture.database.reader())
            .await?
            .is_empty()
    );
    assert!(
        query::<Turso>("SELECT commit_id FROM _fleet_sync_capture_bookkeeping")
            .fetch_all(fixture.database.reader())
            .await?
            .is_empty()
    );
    fixture.marks.refuse.store(false, Ordering::SeqCst);
    fixture.sender.replicate_once(fixture.first).await?;
    fixture.sender.replicate_once(fixture.second).await?;
    assert_eq!(
        query::<Turso>("SELECT id FROM local_marks")
            .fetch_all(fixture.database.reader())
            .await?
            .len(),
        1
    );
    fixture.close().await
}

#[tokio::test]
async fn a_refusing_copy_changes_nothing_and_does_not_stop_another_copy_or_writer() -> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture
        .transport
        .files
        .get(&fixture.first)
        .ok_or("missing first copy")?
        .hook
        .refuse
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        fixture.sender.replicate_once(fixture.first).await,
        Err(SenderError::Refused {
            reason: fleet_sync_protocol::PieceRefusal::Hook(_),
            ..
        })
    ));
    {
        let mut connection = fixture
            .transport
            .files
            .get(&fixture.first)
            .ok_or("missing first copy")?
            .connection
            .lock()
            .await;
        let connection = connection.as_mut().ok_or("closed copy")?;
        assert!(
            query::<Turso>("SELECT name FROM sqlite_schema WHERE name='text_records'")
                .fetch_optional(&mut *connection)
                .await?
                .is_none()
        );
        assert!(
            query::<Turso>("SELECT position FROM _fleet_sync_test_hooks")
                .fetch_all(&mut *connection)
                .await?
                .is_empty()
        );
    }
    fixture.sender.replicate_once(fixture.second).await?;
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute("INSERT INTO text_records VALUES('during refusal',7,'accepted')")
        .await?;
    write.commit().await?;
    let copies = fixture.sender.copies().await?;
    assert!(
        copies
            .iter()
            .find(|copy| copy.copy == fixture.second)
            .ok_or("missing second")?
            .acked
            > copies
                .iter()
                .find(|copy| copy.copy == fixture.first)
                .ok_or("missing first")?
                .acked
    );
    fixture
        .transport
        .files
        .get(&fixture.first)
        .ok_or("missing first copy")?
        .hook
        .refuse
        .store(false, Ordering::SeqCst);
    fixture.sender.replicate_once(fixture.first).await?;
    fixture.close().await
}
