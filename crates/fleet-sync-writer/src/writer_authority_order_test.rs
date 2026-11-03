#![allow(clippy::panic_in_result_fn)]
use super::tests::{TestResult, clock, renewal};
use super::*;
use crate::{WriteRefusal, writer_journal_tests::LocalHost, writer_release_tests::create};
use durable_steps::{AtomicStore, StoreError};
use fleet_sync_lease::{HolderError, WriterState};
use runtime_primitives::Clock;
use std::sync::Arc;

#[tokio::test]
async fn delayed_epochless_refusal_cannot_override_a_later_renewal() -> TestResult {
    delayed_refusal(false).await
}
#[tokio::test]
async fn delayed_epochless_refusal_cannot_override_a_later_registration_grant() -> TestResult {
    delayed_refusal(true).await
}
async fn delayed_refusal(registration: bool) -> TestResult {
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = clock();
    let database = create(&path, clock.clone()).await?;
    let peer = database.control.lock().await.holder.peer();
    let (answered, answer_ready) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let delayed = {
        let database = database.clone();
        let started = clock.now();
        tokio::spawn(async move {
            answered.send(()).map_err(|_| "answer receiver closed")?;
            released.await?;
            database
                .observe_renewal(RenewAnswer::Refused(LeaseRefusal::NotRegistered), started)
                .await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
    };
    answer_ready.await?;
    clock.0.store(200, Ordering::SeqCst);
    let granted = renewal(database.database(), peer, 1)?;
    if registration {
        let RenewAnswer::Renewed { current, .. } = granted else {
            return Err("fixture grant missing".into());
        };
        database.observe_registration(current, clock.now()).await?;
    } else {
        database.observe_renewal(granted, clock.now()).await?;
    }
    release
        .send(())
        .map_err(|_| "delayed answer receiver closed")?;
    delayed.await??;
    assert_eq!(
        database.writer_state().await?,
        WriterState::Writing {
            epoch: Epoch::new(1)?
        }
    );
    let write = database.begin_write().await?;
    drop(write);
    database.close().await;
    drop(database);
    let reopened = create(&path, clock).await?;
    assert_eq!(
        reopened.writer_state().await?,
        WriterState::Writing {
            epoch: Epoch::new(1)?
        }
    );
    let write = reopened.begin_write().await?;
    drop(write);
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn delayed_same_epoch_grant_cannot_revive_writing_after_a_later_refusal() -> TestResult {
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = clock();
    let database = create(&path, clock.clone()).await?;
    let peer = database.control.lock().await.holder.peer();
    let RenewAnswer::Renewed { current, .. } = renewal(database.database(), peer, 1)? else {
        return Err("fixture grant missing".into());
    };
    database.observe_registration(current, clock.now()).await?;
    let (answered, answer_ready) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let delayed = {
        let database = database.clone();
        let started = clock.now();
        tokio::spawn(async move {
            let answer = renewal(database.database(), peer, 1)?;
            answered.send(()).map_err(|_| "answer receiver closed")?;
            released.await?;
            database.observe_renewal(answer, started).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
    };
    answer_ready.await?;
    clock.0.store(200, Ordering::SeqCst);
    database
        .observe_renewal(
            RenewAnswer::Refused(LeaseRefusal::NotRegistered),
            clock.now(),
        )
        .await?;
    release
        .send(())
        .map_err(|_| "delayed answer receiver closed")?;
    delayed.await??;
    assert!(matches!(
        database.writer_state().await,
        Err(OpenError::Holder {
            error: HolderError::Refused(LeaseRefusal::NotRegistered)
        })
    ));
    assert!(matches!(
        database.begin_write().await,
        Err(WriteRefusal::NotWriter)
    ));
    database.close().await;
    drop(database);
    let reopened = create(&path, clock).await?;
    assert!(matches!(
        reopened.writer_state().await,
        Err(OpenError::Holder {
            error: HolderError::Refused(LeaseRefusal::NotRegistered)
        })
    ));
    assert!(matches!(
        reopened.begin_write().await,
        Err(WriteRefusal::NotWriter)
    ));
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn equal_start_epochless_refusal_wins_both_orders_and_later_renewal_heals() -> TestResult {
    use std::sync::atomic::Ordering;
    for refusal_first in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("writer.db");
        let clock = clock();
        let database = create(&path, clock.clone()).await?;
        let peer = database.control.lock().await.holder.peer();
        let RenewAnswer::Renewed { current, .. } = renewal(database.database(), peer, 1)? else {
            return Err("fixture grant missing".into());
        };
        database.observe_registration(current, clock.now()).await?;
        let first = if refusal_first {
            RenewAnswer::Refused(LeaseRefusal::NotRegistered)
        } else {
            renewal(database.database(), peer, 1)?
        };
        let second = if refusal_first {
            renewal(database.database(), peer, 1)?
        } else {
            RenewAnswer::Refused(LeaseRefusal::NotRegistered)
        };
        database.observe_renewal(first, clock.now()).await?;
        database.observe_renewal(second, clock.now()).await?;
        assert!(
            matches!(
                database.writer_state().await,
                Err(OpenError::Holder {
                    error: HolderError::Refused(LeaseRefusal::NotRegistered)
                })
            ),
            "tie order refusal_first={refusal_first}"
        );
        assert!(matches!(
            database.begin_write().await,
            Err(WriteRefusal::NotWriter)
        ));
        clock.0.store(101, Ordering::SeqCst);
        database
            .observe_renewal(renewal(database.database(), peer, 1)?, clock.now())
            .await?;
        assert_eq!(
            database.writer_state().await?,
            WriterState::Writing {
                epoch: Epoch::new(1)?
            }
        );
        let write = database.begin_write().await?;
        drop(write);
        database.close().await;
        drop(database);
        let reopened = create(&path, clock).await?;
        assert_eq!(
            reopened.writer_state().await?,
            WriterState::Writing {
                epoch: Epoch::new(1)?
            }
        );
        reopened.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn proven_higher_epoch_still_fences_when_its_request_started_earlier() -> TestResult {
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = clock();
    let database = create(&path, clock.clone()).await?;
    let peer = database.control.lock().await.holder.peer();
    let RenewAnswer::Renewed { current, .. } = renewal(database.database(), peer, 1)? else {
        return Err("fixture grant missing".into());
    };
    database.observe_registration(current, clock.now()).await?;
    clock.0.store(200, Ordering::SeqCst);
    database
        .observe_renewal(
            RenewAnswer::Refused(LeaseRefusal::NotRegistered),
            clock.now(),
        )
        .await?;
    database
        .observe_renewal(
            renewal(database.database(), peer, 2)?,
            SyncTime::from_unix_millis(100),
        )
        .await?;
    database.close().await;
    drop(database);
    let reopened = create(&path, clock).await?;
    assert_eq!(
        reopened.writer_state().await?,
        WriterState::Superseded {
            current: Epoch::new(2)?
        }
    );
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn copy_refusal_uses_the_pre_send_start_and_cannot_replace_a_later_renewal() -> TestResult {
    use crate::sender_test_fixture::{BlockedSend, SenderFixture};
    use sqlx_core::{executor::Executor, query::query};
    use sqlx_turso::Turso;
    use std::{sync::atomic::Ordering, time::Duration};
    let clock = clock();
    let fixture = SenderFixture::open_with_clock(clock.clone()).await?;
    fixture.migrate_copies().await?;
    let peer = fixture.database.control.lock().await.holder.peer();
    fixture
        .database
        .observe_renewal(renewal(fixture.database.database(), peer, 1)?, clock.now())
        .await?;
    {
        let mut owner = fixture
            .transport
            .files
            .get(&fixture.first)
            .ok_or("copy missing")?
            .connection
            .lock()
            .await;
        let connection = owner.as_mut().ok_or("copy closed")?;
        // Epoch-one unowned copy is a valid receiver state. Its real refusal
        // must retain the request's start, rather than its delayed receipt time.
        query::<Turso>("UPDATE _fleet_sync_copy SET record=json_set(record,'$.copy.holder',NULL)")
            .execute(connection)
            .await?;
    }
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute("UPDATE text_records SET amount=99")
        .await?;
    write.commit().await?;
    let (entered, reached) = tokio::sync::oneshot::channel();
    let (resume, released) = tokio::sync::oneshot::channel();
    *fixture.transport.blocked.lock().await = Some(BlockedSend {
        copy: fixture.first,
        entered,
        resume: released,
    });
    let sending = {
        let sender = fixture.sender.clone();
        let copy = fixture.first;
        tokio::spawn(async move { sender.replicate_once(copy).await })
    };
    tokio::time::timeout(Duration::from_secs(5), reached).await??;
    clock.0.store(200, Ordering::SeqCst);
    fixture
        .database
        .observe_renewal(renewal(fixture.database.database(), peer, 1)?, clock.now())
        .await?;
    clock.0.store(300, Ordering::SeqCst);
    resume.send(()).map_err(|_| "copy receiver closed")?;
    assert!(
        matches!(sending.await?,Err(crate::SenderError::Refused {reason:fleet_sync_protocol::PieceRefusal::NotWriter {current:Some(epoch)},..}) if epoch==Epoch::new(1)?)
    );
    assert_eq!(
        fixture.database.writer_state().await?,
        WriterState::Writing {
            epoch: Epoch::new(1)?
        }
    );
    let fixture = fixture.restart().await?;
    assert_eq!(
        fixture.database.writer_state().await?,
        WriterState::Writing {
            epoch: Epoch::new(1)?
        }
    );
    fixture.close().await
}

#[tokio::test]
async fn a_later_epochless_refusal_does_not_erase_a_proven_higher_fence() -> TestResult {
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = clock();
    let database = create(&path, clock.clone()).await?;
    let peer = database.control.lock().await.holder.peer();
    database
        .observe_renewal(renewal(database.database(), peer, 2)?, clock.now())
        .await?;
    clock.0.store(200, Ordering::SeqCst);
    database
        .observe_renewal(
            RenewAnswer::Refused(LeaseRefusal::NotRegistered),
            clock.now(),
        )
        .await?;
    clock.0.store(300, Ordering::SeqCst);
    database
        .observe_renewal(renewal(database.database(), peer, 1)?, clock.now())
        .await?;
    database.close().await;
    drop(database);
    let reopened = create(&path, clock).await?;
    assert_eq!(
        reopened.writer_state().await?,
        WriterState::Superseded {
            current: Epoch::new(2)?
        }
    );
    assert!(
        matches!(reopened.begin_write().await,Err(WriteRefusal::Superseded {current}) if current==Epoch::new(2)?)
    );
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn task_scope_applies_the_selected_newer_observation_before_returning() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = clock();
    let database = create(&path, clock.clone()).await?;
    let peer = database.control.lock().await.holder.peer();
    database
        .observe_renewal(
            renewal(database.database(), peer, 1)?,
            SyncTime::from_unix_millis(200),
        )
        .await?;
    let journal = database.journal_store(Arc::new(LocalHost)).await?;
    journal
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .observe_task_renewal(
                        RenewAnswer::Refused(LeaseRefusal::NotRegistered),
                        SyncTime::from_unix_millis(100),
                    )
                    .await
                    .map_err(StoreError::backend)?;
                assert_eq!(
                    scope
                        .holder
                        .writer_state(SyncTime::from_unix_millis(200))
                        .map_err(StoreError::backend)?,
                    WriterState::Writing {
                        epoch: Epoch::new(1).map_err(StoreError::backend)?
                    }
                );
                Ok(())
            })
        })
        .await?;
    drop(journal);
    database.close().await;
    Ok(())
}
