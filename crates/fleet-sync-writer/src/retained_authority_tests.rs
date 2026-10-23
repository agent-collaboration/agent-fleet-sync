#![allow(clippy::panic_in_result_fn)]
use crate::{
    read_transaction_tests::{ReadSchema, writer},
    retained_file_fixture::{ReadOnlyFixture, TestResult, child, child_action, snapshot},
    sender_test_fixture::SenderFixture,
    writer_scope::WriterScope,
    *,
};
use durable_steps::Uuid;
use fleet_sync_protocol::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    query::query,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{path::Path, time::Duration};

async fn grant_sender(fixture: &SenderFixture) -> TestResult {
    let started = fixture.database.clock.now();
    let holder = fixture.database.control.lock().await.holder.peer();
    // The sender fixture grants only its in-memory holder. Restore its initial
    // provisional state before exercising the public, persisted grant path.
    fixture.database.control.lock().await.holder = fleet_sync_lease::LeaseHolder::provisional(
        fixture.database.database(),
        holder,
        fleet_sync_lease::LeasePolicy {
            lease_duration: Duration::from_secs(600),
            renew_interval: Duration::from_secs(10),
        },
    );
    fixture
        .database
        .observe_registration(
            LeaseStatus {
                database: fixture.database.database(),
                holder,
                epoch: Epoch::new(1)?,
                state: LeaseState::Held,
                expires_at: started.after(Duration::from_secs(600)),
                remaining_ms: 600_000,
            },
            started,
        )
        .await?;
    Ok(())
}

async fn fence_authority(fixture: &SenderFixture) -> TestResult<ReplicationPosition> {
    let mut owner = fixture
        .transport
        .files
        .get(&fixture.first)
        .ok_or("missing copy")?
        .connection
        .lock()
        .await;
    let connection = owner.as_mut().ok_or("closed copy")?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let CopyStatusAnswer::Status(status) =
        fleet_sync_receiver::copy_status(&mut WriterScope(&mut transaction)).await?
    else {
        return Err("missing copy status".into());
    };
    let result = fleet_sync_receiver::fence(
        &mut WriterScope(&mut transaction),
        &FenceRequest {
            database: fixture.database.database(),
            transfer_id: TransferId::new(Uuid::now_v7())?,
            new_epoch: Epoch::new(2)?,
            expect: FenceExpectation {
                epoch: Epoch::new(1)?,
                acked: status.acked,
            },
            target_schema: Some(SchemaSupport { max_version: 2 }),
            holder: PeerId::new(Uuid::now_v7())?,
        },
    )
    .await?;
    assert!(matches!(result, FenceResult::Fenced { .. }));
    transaction.commit().await?;
    Ok(status.acked)
}

async fn inspect_fenced(
    path: &Path,
    ended_at: ReplicationPosition,
    expected_tails: Vec<RejectedTail>,
) -> TestResult {
    let retained = RetainedFile::open_read_only(path, SenderFixture::schema()?).await?;
    // A granted registration stays Ready; it must not hide later supersession.
    assert!(matches!(
        retained.registration_outcome(),
        WriterRegistrationOutcome::Ready { .. }
    ));
    assert_eq!(
        retained.authority(),
        Some(RetainedAuthority::Superseded {
            current: Some(Epoch::new(2)?),
            ended_at: Some(ended_at),
        })
    );
    assert_eq!(retained.rejected_tails().await?, expected_tails);
    assert!(
        retained
            .reader()
            .execute("DELETE FROM text_records")
            .await
            .is_err()
    );
    assert!(
        retained
            .reader()
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn retained_fenced_authority_and_tail_survive_process_restart() -> TestResult {
    if let Some((action, path)) = child_action() {
        let (ended_at, tails): (ReplicationPosition, Vec<RejectedTail>) =
            serde_json::from_str(action.to_str().ok_or("invalid expected tail encoding")?)?;
        return inspect_fenced(&path, ended_at, tails).await;
    }
    for has_tail in [true, false] {
        let fixture = SenderFixture::open().await?;
        grant_sender(&fixture).await?;
        fixture.migrate_copies().await?;
        let ended_at = fence_authority(&fixture).await?;
        if has_tail {
            let mut write = fixture.database.begin_write().await?;
            (&mut write)
                .execute("UPDATE text_records SET label='retained after fence'")
                .await?;
            write.commit().await?;
            fixture.sender.replicate_once(fixture.first).await?;
        } else {
            // The registry's refusal after the real copy fence records an empty cutoff.
            fixture
                .database
                .observe_renewal(
                    RenewAnswer::Refused(LeaseRefusal::StaleEpoch {
                        current_epoch: Epoch::new(2)?,
                        epoch_ended_at: ended_at,
                    }),
                    fixture.database.clock.now(),
                )
                .await?;
        }
        let expected = fixture.sender.rejected_tails().await?;
        assert_eq!(expected.len(), usize::from(has_tail));
        if let Some(tail) = expected.first() {
            assert_eq!(tail.ended_at, ended_at);
        }
        let path = fixture.database.path().to_owned();
        fixture.database.close().await;
        for file in fixture.transport.files.values() {
            if let Some(connection) = file.connection.lock().await.take() {
                connection.close().await?;
            }
        }
        let permissions = ReadOnlyFixture::apply(fixture._directory.path())?;
        let before = snapshot(fixture._directory.path())?;
        child(
            "retained_authority_tests::retained_fenced_authority_and_tail_survive_process_restart",
            &serde_json::to_string(&(ended_at, expected))?,
            &path,
        )
        .await?;
        assert_eq!(snapshot(fixture._directory.path())?, before);
        drop(permissions);
        fixture.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn retained_projects_not_writer_and_newer_current_without_reclassifying_registration()
-> TestResult {
    for case in 0..5 {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("retained.db");
        let database = writer(&path).await?;
        let started = database.clock.now();
        let holder = database.control.lock().await.holder.peer();
        let lease = LeaseStatus {
            database: database.database(),
            holder,
            epoch: Epoch::new(1)?,
            state: LeaseState::Held,
            expires_at: started.after(Duration::from_secs(60)),
            remaining_ms: 60_000,
        };
        database
            .observe_registration(lease.clone(), started)
            .await?;
        let expected = match case {
            0 | 1 => {
                let current = if case == 0 {
                    Some(Epoch::new(2)?)
                } else {
                    None
                };
                database
                    .observe_renewal(
                        RenewAnswer::Refused(LeaseRefusal::NotWriter { current }),
                        started,
                    )
                    .await?;
                Some(RetainedAuthority::Superseded {
                    current,
                    ended_at: None,
                })
            }
            2 | 4 => {
                let current_epoch = Epoch::new(if case == 2 { 2 } else { 1 })?;
                let current = LeaseStatus {
                    epoch: current_epoch,
                    holder: PeerId::new(Uuid::now_v7())?,
                    ..lease.clone()
                };
                database
                    .observe_renewal(
                        RenewAnswer::Renewed {
                            recorded: RenewResult {
                                epoch: lease.epoch,
                                expires_at: lease.expires_at,
                            },
                            current,
                            transfer_in_progress: None,
                            replayed: true,
                        },
                        started,
                    )
                    .await?;
                Some(RetainedAuthority::Superseded {
                    current: Some(current_epoch),
                    ended_at: None,
                })
            }
            _ => None,
        };
        database.close().await;
        drop(database);
        let _permissions = ReadOnlyFixture::apply(directory.path())?;
        let before = snapshot(directory.path())?;
        let retained = RetainedFile::open_read_only(&path, ReadSchema).await?;
        assert_eq!(retained.authority(), expected, "case {case}");
        assert_eq!(
            retained.registration_outcome(),
            &WriterRegistrationOutcome::Ready { lease }
        );
        assert!(retained.rejected_tails().await?.is_empty());
        let pool = retained.reader().pool.current()?;
        drop(retained);
        pool.close().await;
        assert_eq!(snapshot(directory.path())?, before);
    }
    Ok(())
}

#[tokio::test]
async fn retained_projects_retired_and_registration_rejection() -> TestResult {
    for retired in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("retained.db");
        let database = writer(&path).await?;
        let expected = if retired {
            database
                .observe_renewal(
                    RenewAnswer::Refused(LeaseRefusal::Retired),
                    database.clock.now(),
                )
                .await?;
            RetainedAuthority::Retired
        } else {
            let reason = GateReason {
                code: GateReasonCode::new("retained_refused")?,
                detail: "retained refusal".into(),
            };
            database.reject_registration(reason.clone()).await?;
            RetainedAuthority::RegistrationRejected { reason }
        };
        database.close().await;
        drop(database);
        let _permissions = ReadOnlyFixture::apply(directory.path())?;
        let before = snapshot(directory.path())?;
        let retained = RetainedFile::open_read_only(&path, ReadSchema).await?;
        assert_eq!(retained.authority(), Some(expected));
        assert!(retained.rejected_tails().await?.is_empty());
        let pool = retained.reader().pool.current()?;
        drop(retained);
        pool.close().await;
        assert_eq!(snapshot(directory.path())?, before);
    }
    Ok(())
}

#[tokio::test]
async fn retained_rejects_corrupt_tail_records_and_positions_without_writing() -> TestResult {
    let database_id = DatabaseId::new(Uuid::now_v7())?;
    let position = ReplicationPosition {
        epoch: Epoch::new(1)?,
        sequence: CommitSequence::new(0),
    };
    let valid = RejectedTail {
        database: database_id,
        epoch: position.epoch,
        ended_at: position,
        last_commit: ReplicationPosition {
            sequence: CommitSequence::new(1),
            ..position
        },
    };
    let invalid_tails = [
        ("1", "{}".to_owned()),
        ("2", serde_json::to_string(&valid)?),
        (
            "1",
            serde_json::to_string(&RejectedTail {
                last_commit: position,
                ..valid.clone()
            })?,
        ),
        (
            "1",
            serde_json::to_string(&RejectedTail {
                last_commit: ReplicationPosition {
                    epoch: Epoch::new(2)?,
                    ..valid.last_commit
                },
                ..valid.clone()
            })?,
        ),
        (
            "1",
            serde_json::to_string(&RejectedTail {
                ended_at: ReplicationPosition {
                    epoch: Epoch::new(2)?,
                    ..position
                },
                ..valid.clone()
            })?,
        ),
    ];
    for (epoch, encoded) in invalid_tails {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("retained.db");
        let database = writer(&path).await?;
        database.close().await;
        drop(database);
        let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
        query::<Turso>("INSERT INTO _fleet_sync_rejected_tails(epoch,record) VALUES(?,?)")
            .bind(epoch)
            .bind(encoded)
            .execute(&mut connection)
            .await?;
        connection.close().await?;
        let _permissions = ReadOnlyFixture::apply(directory.path())?;
        let before = snapshot(directory.path())?;
        let retained = RetainedFile::open_read_only(&path, ReadSchema).await?;
        assert!(matches!(
            retained.rejected_tails().await,
            Err(OpenError::Corrupt { .. })
        ));
        let pool = retained.reader().pool.current()?;
        drop(retained);
        pool.close().await;
        assert_eq!(snapshot(directory.path())?, before);
    }
    Ok(())
}

#[tokio::test]
async fn retained_refuses_a_stale_epoch_cutoff_that_disagrees_with_the_saved_end() -> TestResult {
    use sqlx_core::row::Row;
    for missing_saved_end in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("retained.db");
        let database = writer(&path).await?;
        let ended_at = ReplicationPosition {
            epoch: Epoch::new(1)?,
            sequence: CommitSequence::new(0),
        };
        database
            .observe_renewal(
                RenewAnswer::Refused(LeaseRefusal::StaleEpoch {
                    current_epoch: Epoch::new(2)?,
                    epoch_ended_at: ended_at,
                }),
                database.clock.now(),
            )
            .await?;
        database.close().await;
        drop(database);
        let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
        let row = query::<Turso>("SELECT record FROM _fleet_sync_writer")
            .fetch_one(&mut connection)
            .await?;
        let mut record: crate::writer_file_record::WriterFileRecord =
            serde_json::from_str(&row.try_get::<String, _>("record")?)?;
        if missing_saved_end {
            record.epoch_ended_at = None;
        } else {
            let Some(crate::writer_authority::AuthorityObservation::Renewal {
                answer: RenewAnswer::Refused(LeaseRefusal::StaleEpoch { epoch_ended_at, .. }),
                ..
            }) = &mut record.authority
            else {
                return Err("missing stale-epoch fixture observation".into());
            };
            epoch_ended_at.sequence = CommitSequence::new(1);
        }
        query::<Turso>("UPDATE _fleet_sync_writer SET record=?")
            .bind(serde_json::to_string(&record)?)
            .execute(&mut connection)
            .await?;
        connection.close().await?;
        let _permissions = ReadOnlyFixture::apply(directory.path())?;
        let before = snapshot(directory.path())?;
        assert!(
            matches!(
                RetainedFile::open_read_only(&path, ReadSchema).await,
                Err(OpenError::Corrupt { .. })
            ),
            "accepted disagreeing cutoff, missing_saved_end={missing_saved_end}"
        );
        assert_eq!(snapshot(directory.path())?, before);
    }
    Ok(())
}
