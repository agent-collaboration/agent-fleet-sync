#[path = "support/copy_fixture.rs"]
#[allow(dead_code)]
mod fixture;
#[allow(dead_code, unused_imports)]
mod support;
use fixture::*;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest, CopyFaultRequest};
use fleet_sync_test_consumer::{SampleSchema, TestResult};
use fleet_sync_writer::*;
use serde_json::Value;
use std::{path::Path, sync::Arc};
use support::control_request;

#[tokio::test]
async fn a_damaged_copy_is_typed_while_its_peer_progresses_and_the_same_piece_recovers_once()
-> TestResult {
    let root = tempfile::tempdir()?;
    let damaged = Arc::new(
        ReplicatedDatabase::create(
            &root.path().join("damaged-writer.db"),
            SampleSchema::new()?,
            writer_peer()?,
            lease_policy(),
            WriterOptions::default(),
        )
        .await?,
    );
    let healthy = Arc::new(
        ReplicatedDatabase::create(
            &root.path().join("healthy-writer.db"),
            SampleSchema::new()?,
            writer_peer()?,
            lease_policy(),
            WriterOptions::default(),
        )
        .await?,
    );
    let result = observe_damage_and_repair(root.path(), &damaged, &healthy).await;
    damaged.close().await;
    healthy.close().await;
    result
}

async fn observe_damage_and_repair(
    root: &Path,
    damaged: &Arc<ReplicatedDatabase<SampleSchema>>,
    healthy: &Arc<ReplicatedDatabase<SampleSchema>>,
) -> TestResult {
    let service_root = root.join("service");
    let first = CopyService::start(&service_root, &root.join("initial-client.db")).await?;
    let prepared: TestResult = async {
        for writer in [damaged, healthy] {
            let lease = register_writer(writer, &first.session).await?;
            let sending = replication_sender(writer.clone(), &first.session)?;
            sending
                .add_copy(
                    local_receiver()?,
                    CopyRole::Authoritative,
                    ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
                )
                .await?;
            sending.replicate_once(local_receiver()?).await?;
            write_sample(writer, "before fault").await?;
            sending.replicate_once(local_receiver()?).await?;
        }
        Ok(())
    }
    .await;
    first.stop().await?;
    prepared?;
    let damaged_path = copy_file_path(&service_root, damaged.database())?;
    let healthy_path = copy_file_path(&service_root, healthy.database())?;
    let before_damaged = read_copy_snapshot(&damaged_path).await?;
    let before_healthy = read_copy_snapshot(&healthy_path).await?;
    assert_eq!(before_damaged.rows.len(), 1);
    assert_eq!(before_healthy.rows.len(), 1);
    let marker = before_damaged
        .record
        .as_deref()
        .ok_or("known-good copy marker missing")?;
    replace_marker(&damaged_path, "{invalid-copy-record").await?;
    let faulted = read_copy_snapshot(&damaged_path).await?;

    let mut second = CopyService::start(&service_root, &root.join("fault-client.db")).await?;
    let driven: TestResult<Value> = async {
        write_sample(damaged, "pending while damaged").await?;
        write_sample(healthy, "healthy beside damaged").await?;
        let bad_sender = replication_sender(damaged.clone(), &second.session)?;
        let before_sender = bad_sender.copies().await?;
        let bad = bad_sender.replicate_once(local_receiver()?).await;
        let after_sender = bad_sender.copies().await?;
        let unavailable = second
            .session
            .copy_unavailable_answer(damaged.database())
            .await;
        if unavailable
            .as_ref()
            .is_none_or(|reason| reason["kind"] != "damaged")
        {
            return Err(format!(
                "damaged copy needs a correlated CopyUnavailable(Damaged), got {bad:?}"
            )
            .into());
        }
        if !matches!(
            bad,
            Err(SenderError::CopyUnavailable {
                reason: CopyUnavailableReason::Damaged { .. },
                ..
            })
        ) {
            return Err(
                "typed copy refusal must reach the sender as a wait, not AnswerLost".into(),
            );
        }
        assert_eq!(after_sender, before_sender);
        assert!(matches!(
            control_request(
                &mut second.control,
                ControlRequest::CopyFault(CopyFaultRequest::LoseRollbackReply {
                    database: damaged.database()
                })
            )
            .await?,
            ControlAnswer::Ok
        ));
        let original_piece = second
            .session
            .outgoing_pieces(damaged.database())
            .await
            .first()
            .cloned()
            .ok_or("damaged piece missing")?;
        match second
            .session
            .requests
            .request(OperationRequest::Replication(ReplicationRequest::Piece(
                serde_json::from_value(original_piece.clone())?,
            )))
            .await?
        {
            OperationAnswer::Replication(ReplicationAnswer::OutcomeUnknown {
                reason: OutcomeUnknownReason::StoreBoundary { detail },
            }) => assert_eq!(
                detail.as_str(),
                "storage backend: injected loss of rolled back transaction reply"
            ),
            _ => return Err("lost rollback reply must stay a typed unknown answer".into()),
        }
        let good = replication_sender(healthy.clone(), &second.session)?
            .replicate_once(local_receiver()?)
            .await;
        assert!(
            good?
                .iter()
                .any(|event| matches!(event, ReplicationEvents::Acknowledged { .. }))
        );
        assert_eq!(second.session.connection_count(), 1);
        Ok(original_piece)
    }
    .await;
    second.stop().await?;
    let original_piece = driven?;
    assert_eq!(read_copy_snapshot(&damaged_path).await?, faulted);
    let healthy_after = read_copy_snapshot(&healthy_path).await?;
    assert_eq!(healthy_after.rows.len(), before_healthy.rows.len() + 1);
    assert_eq!(
        healthy_after.receipts.len(),
        before_healthy.receipts.len() + 1
    );

    replace_marker(&damaged_path, marker).await?;
    assert_eq!(
        read_copy_snapshot(&damaged_path).await?.record,
        before_damaged.record
    );
    let third = CopyService::start(&service_root, &root.join("repair-client.db")).await?;
    let recovered: TestResult = async {
        let events = replication_sender(damaged.clone(), &third.session)?
            .replicate_once(local_receiver()?)
            .await?;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ReplicationEvents::Acknowledged { .. }))
        );
        assert_eq!(
            third
                .session
                .outgoing_pieces(damaged.database())
                .await
                .first(),
            Some(&original_piece)
        );
        let piece = serde_json::from_value(original_piece.clone())?;
        assert!(matches!(
            third
                .session
                .requests
                .request(OperationRequest::Replication(ReplicationRequest::Piece(
                    piece
                )))
                .await?,
            OperationAnswer::Replication(ReplicationAnswer::AlreadyApplied { .. })
        ));
        Ok(())
    }
    .await;
    third.stop().await?;
    recovered?;
    let repaired = read_copy_snapshot(&damaged_path).await?;
    assert_eq!(repaired.rows.len(), before_damaged.rows.len() + 1);
    assert_eq!(repaired.receipts.len(), before_damaged.receipts.len() + 1);
    assert_eq!(repaired.hooks.len(), before_damaged.hooks.len() + 1);
    assert!(
        repaired
            .rows
            .iter()
            .any(|(_, label)| label == "pending while damaged")
    );
    Ok(())
}

#[tokio::test]
async fn committed_apply_keeps_its_answer_when_the_actual_copy_wake_fails() -> TestResult {
    let root = tempfile::tempdir()?;
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &root.path().join("writer.db"),
            SampleSchema::new()?,
            writer_peer()?,
            lease_policy(),
            WriterOptions::default(),
        )
        .await?,
    );
    let result = observe_failed_wake(root.path(), &writer).await;
    writer.close().await;
    result
}

async fn observe_failed_wake(
    root: &Path,
    writer: &Arc<ReplicatedDatabase<SampleSchema>>,
) -> TestResult {
    let service_root = root.join("service");
    let mut running = CopyService::start(&service_root, &root.join("client.db")).await?;
    let observed: TestResult = async {
        let lease = register_writer(writer, &running.session).await?;
        let sender = replication_sender(writer.clone(), &running.session)?;
        sender
            .add_copy(
                local_receiver()?,
                CopyRole::Authoritative,
                ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
            )
            .await?;
        sender.replicate_once(local_receiver()?).await?;
        let before = match control_request(
            &mut running.control,
            ControlRequest::WaitEffects { count: 1 },
        )
        .await?
        {
            ControlAnswer::Effects(effects) => effects,
            _ => return Err("initial follow-up did not finish".into()),
        };
        assert_eq!(before.len(), 1);
        control_request(
            &mut running.control,
            ControlRequest::CopyFault(CopyFaultRequest::HoldDrive {
                database: writer.database(),
            }),
        )
        .await?;
        assert!(matches!(
            control_request(
                &mut running.control,
                ControlRequest::CopyFault(CopyFaultRequest::FailNextWake {
                    database: writer.database()
                })
            )
            .await?,
            ControlAnswer::Ok
        ));
        write_sample(writer, "committed before wake failure").await?;
        assert!(
            sender
                .replicate_once(local_receiver()?)
                .await?
                .iter()
                .any(|event| matches!(event, ReplicationEvents::Acknowledged { .. }))
        );
        assert!(matches!(
            control_request(
                &mut running.control,
                ControlRequest::CopyFault(CopyFaultRequest::WakeFailures {
                    database: writer.database()
                })
            )
            .await?,
            ControlAnswer::CopyWakeFailures(1)
        ));
        let after = match control_request(&mut running.control, ControlRequest::Effects).await? {
            ControlAnswer::Effects(effects) => effects,
            _ => return Err("effect readback missing".into()),
        };
        assert_eq!(
            after, before,
            "failed timer hint must leave the follow-up owed"
        );
        assert_eq!(running.session.connection_count(), 1);
        Ok(())
    }
    .await;
    running.kill().await?;
    observed?;
    let committed = read_copy_snapshot(&copy_file_path(&service_root, writer.database())?).await?;
    assert_eq!(committed.rows.len(), 1);
    assert_eq!(committed.hooks.len(), 2);
    assert_eq!(committed.receipts.len(), 2);
    let owed = pending_follow_ups(&copy_file_path(&service_root, writer.database())?).await?;
    assert_eq!(
        owed.len(),
        1,
        "the committed apply must retain one real pending follow-up before restart"
    );
    let mut restarted =
        CopyService::start(&service_root, &root.join("restarted-client.db")).await?;
    let recovered: TestResult = async {
        let effects = match control_request(&mut restarted.control, ControlRequest::WaitEffects { count: 2 }).await? {
            ControlAnswer::Effects(effects) => effects,
            _ => return Err("owed follow-up did not recover at startup".into()),
        };
        assert_eq!(effects.len(), 2);
        assert_ne!(effects[0].saved.call, effects[1].saved.call);
        assert!(effects.iter().any(|effect| effect.saved.call.operation == owed[0]));
        // An explicit second wake cannot repeat the recovered effect.
        control_request(&mut restarted.control, ControlRequest::CopyFault(
            CopyFaultRequest::Wake { database: writer.database() })).await?;
        assert!(replication_sender(writer.clone(), &restarted.session)?.replicate_once(local_receiver()?).await?.is_empty());
        assert!(matches!(control_request(&mut restarted.control, ControlRequest::Effects).await?, ControlAnswer::Effects(current) if current == effects));
        Ok(())
    }.await;
    restarted.stop().await?;
    recovered?;
    assert_eq!(
        read_copy_snapshot(&copy_file_path(&service_root, writer.database())?).await?,
        committed
    );
    Ok(())
}

#[tokio::test]
async fn an_unknown_commit_reconciles_the_identical_piece_without_a_second_effect() -> TestResult {
    for corrupt_reason in [false, true] {
        let root = tempfile::tempdir()?;
        let writer = Arc::new(
            ReplicatedDatabase::create(
                &root.path().join("writer.db"),
                SampleSchema::new()?,
                writer_peer()?,
                lease_policy(),
                WriterOptions::default(),
            )
            .await?,
        );
        let healthy = Arc::new(
            ReplicatedDatabase::create(
                &root.path().join("healthy-writer.db"),
                SampleSchema::new()?,
                writer_peer()?,
                lease_policy(),
                WriterOptions::default(),
            )
            .await?,
        );
        let mut running =
            CopyService::start(&root.path().join("service"), &root.path().join("client.db"))
                .await?;
        let observed: TestResult = async {
        let lease = register_writer(&writer, &running.session).await?;
        let sender = replication_sender(writer.clone(), &running.session)?;
        sender.add_copy(local_receiver()?, CopyRole::Authoritative,
            ReplicationPosition::new(lease.epoch, CommitSequence::new(0))).await?;
        sender.replicate_once(local_receiver()?).await?;
        let healthy_lease = register_writer(&healthy, &running.session).await?;
        let healthy_sender = replication_sender(healthy.clone(), &running.session)?;
        healthy_sender.add_copy(local_receiver()?, CopyRole::Authoritative,
            ReplicationPosition::new(healthy_lease.epoch, CommitSequence::new(0))).await?;
        healthy_sender.replicate_once(local_receiver()?).await?;
        let before = sender.copies().await?;
        control_request(&mut running.control, ControlRequest::CopyFault(
            CopyFaultRequest::LoseCommitReply { database: writer.database() })).await?;
        if corrupt_reason { running.session.inject_unknown_reason(); }
        write_sample(&writer, "committed but answer uncertain").await?;
        let lost = sender.replicate_once(local_receiver()?).await;
        assert!(matches!(lost, Err(SenderError::Send(SendFailure::AnswerLost))), "actual lost-commit result: {lost:?}");
        assert_eq!(sender.copies().await?, before);
        if corrupt_reason { running.session.wait_welcomes(2).await?; }
        write_sample(&healthy, "healthy after uncertain sibling commit").await?;
        assert!(healthy_sender.replicate_once(local_receiver()?).await?.iter().any(|event|
            matches!(event, ReplicationEvents::Acknowledged { .. })));
        let offered = running.session.outgoing_pieces(writer.database()).await;
        let original = offered.last().ok_or("no uncertain piece")?.clone();
        assert!(sender.replicate_once(local_receiver()?).await?.iter().any(|event|
            matches!(event, ReplicationEvents::Acknowledged { .. })));
        assert_eq!(running.session.outgoing_pieces(writer.database()).await.last(), Some(&original));
        assert!(matches!(running.session.requests.request(OperationRequest::Replication(
            ReplicationRequest::Piece(serde_json::from_value(original.clone())?))).await?,
            OperationAnswer::Replication(ReplicationAnswer::AlreadyApplied { .. })));
        assert_eq!(running.session.connection_count(), if corrupt_reason { 2 } else { 1 });
        if let Ok(path) = std::env::var("COPY_CERTAINTY_FIXTURE") {
            let pieces = running.session.outgoing_pieces(writer.database()).await;
            let fixture = serde_json::json!({
                "create": CreateCopy { database: writer.database(), epoch: lease.epoch, holder: Some(writer_peer()?), origin: CopyOrigin::Empty },
                "sender": writer_peer()?,
                "pieces": pieces.iter().take(2).map(serde_json::to_string).collect::<Result<Vec<_>,_>>()?,
            });
            tokio::fs::write(path, serde_json::to_vec_pretty(&fixture)?).await?;
        }
        control_request(&mut running.control, ControlRequest::CopyFault(
            CopyFaultRequest::Wake { database: writer.database() })).await?;
        assert!(matches!(control_request(&mut running.control, ControlRequest::WaitEffects { count: 4 }).await?,
            ControlAnswer::Effects(effects) if effects.len() == 4));
        Ok(())
    }.await;
        running.stop().await?;
        writer.close().await;
        healthy.close().await;
        observed?;
        let copy = read_copy_snapshot(&copy_file_path(
            &root.path().join("service"),
            writer.database(),
        )?)
        .await?;
        assert_eq!(copy.rows.len(), 1);
        assert_eq!(copy.hooks.len(), 2);
        assert_eq!(copy.receipts.len(), 2);
        let peer = read_copy_snapshot(&copy_file_path(
            &root.path().join("service"),
            healthy.database(),
        )?)
        .await?;
        assert_eq!(peer.rows.len(), 1);
        assert_eq!(peer.receipts.len(), 2);
        assert_eq!(peer.hooks.len(), 2);
    }
    Ok(())
}
