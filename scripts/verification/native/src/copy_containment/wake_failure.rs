//! Lose the real post-commit timer hint; preserve Applied and recover owed work.
use super::{fault_controls::*, *};
use crate::support::control_request;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest, CopyFaultRequest};

async fn pending_followups(path: &Path) -> TestResult<Vec<durable_steps::OperationId>> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let rows = query::<Turso>("SELECT operation_id FROM _fleet_sync_operations WHERE kind='fleet-sync.apply-follow-up' AND state='running' ORDER BY operation_id")
        .fetch_all(&mut connection).await?;
    let mut pending = Vec::new();
    for row in rows {
        pending.push(durable_steps::OperationId::try_from(
            row.try_get::<String, _>("operation_id")?,
        )?);
    }
    connection.close().await?;
    Ok(pending)
}

pub(super) async fn observe(root: &Path, observations: &mut Observations) -> TestResult {
    let root = root.join("wake-failure");
    tokio::fs::create_dir_all(&root).await?;
    let service_root = root.join("service");
    let peer = new_peer().await?;
    let receiver = new_receiver().await?;
    let writer = new_writer(&root.join("writer.db"), peer, false).await?;
    let result: TestResult = async {
        let mut initial = RunningCopy::start(
            &service_root,
            &root.join("initial-client.db"),
            peer,
            receiver,
        )
        .await?;
        let prepared: TestResult = async {
            bootstrap(&writer, peer, receiver, &initial.session).await?;
            match control_request(
                &mut initial.control,
                ControlRequest::WaitEffects { count: 1 },
            )
            .await?
            {
                ControlAnswer::Effects(effects) if effects.len() == 1 => Ok(()),
                _ => Err("initial follow-up did not finish".into()),
            }
        }
        .await;
        initial.stop().await?;
        prepared?;
        let path = copy_path(&service_root.join("service"), receiver, writer.database());
        if !pending_followups(&path).await?.is_empty() {
            return Err("wake fault requires a quiescent initial copy journal".into());
        }
        // With no earlier copy task left, the next wake belongs to the new apply.
        let mut running =
            RunningCopy::start(&service_root, &root.join("fault-client.db"), peer, receiver)
                .await?;
        let driven: TestResult = async {
            let before = match control_request(
                &mut running.control,
                ControlRequest::WaitEffects { count: 1 },
            )
            .await?
            {
                ControlAnswer::Effects(effects) => effects,
                _ => return Err("initial follow-up missing".into()),
            };
            running
                .fault(CopyFaultRequest::HoldDrive {
                    database: writer.database(),
                })
                .await?;
            running
                .fault(CopyFaultRequest::FailNextWake {
                    database: writer.database(),
                })
                .await?;
            write_sample(&writer, "committed before failed wake").await?;
            let sent = sender(writer.clone(), receiver, &running.session)
                .replicate_once(receiver)
                .await;
            let failures = running
                .fault(CopyFaultRequest::WakeFailures {
                    database: writer.database(),
                })
                .await?;
            let after = match control_request(&mut running.control, ControlRequest::Effects).await?
            {
                ControlAnswer::Effects(effects) => effects,
                _ => return Err("follow-up receipts missing".into()),
            };
            let wire = running.session.wire.frames.lock().await.clone();
            let piece = sent_pieces(&wire)
                .last()
                .cloned()
                .ok_or("wake-fault Piece missing")?;
            let frame = wire
                .iter()
                .find(|row| {
                    row["direction"] == "out"
                        && row["frame"]["value"]["body"]["value"]["value"] == piece
                })
                .map(|row| row["frame"]["value"]["frame"].clone())
                .ok_or("wake-fault frame missing")?;
            let applied = wire.iter().any(|row| {
                row["direction"] == "in"
                    && row["frame"]["value"]["frame"] == frame
                    && row["frame"]["value"]["body"]["value"]["kind"] == "applied"
            });
            observations.check(
                "failed-wake-preserves-committed-applied",
                acknowledged(&sent)
                    && applied
                    && matches!(failures, ControlAnswer::CopyWakeFailures(1)),
                json!({"answer":"applied","wakeFailures":1}),
                json!({"sender":format!("{sent:?}"),"failures":failures,"wire":wire}),
            );
            observations.check(
                "failed-wake-leaves-followup-owed",
                before.len() == 1 && after == before,
                json!({"completedFollowUps":1,"newFollowUp":"owed"}),
                json!({"before":before,"after":after}),
            );
            Ok(())
        }
        .await;
        running.kill().await?;
        driven?;
        let path = copy_path(&service_root.join("service"), receiver, writer.database());
        let committed = read_snapshot(&path).await?;
        let owed = pending_followups(&path).await?;
        observations.check(
            "failed-wake-commit-is-durable",
            committed.rows.len() == 1
                && committed.receipts.len() == 2
                && committed.hook_events.len() == 2
                && owed.len() == 1,
            json!({"rows":1,"receipts":2,"hookEvents":2,"owedOperations":1}),
            json!({"copy":committed,"owedOperations":owed}),
        );
        let mut recovered = RunningCopy::start(
            &service_root,
            &root.join("restarted-client.db"),
            peer,
            receiver,
        )
        .await?;
        let restarted: TestResult = async {
            let effects = match control_request(
                &mut recovered.control,
                ControlRequest::WaitEffects { count: 2 },
            )
            .await?
            {
                ControlAnswer::Effects(effects) => effects,
                _ => return Err("owed follow-up did not recover".into()),
            };
            let distinct = effects.len() == 2
                && effects[0].saved.call != effects[1].saved.call
                && owed.first().is_some_and(|operation| {
                    effects
                        .iter()
                        .any(|effect| effect.saved.call.operation == *operation)
                });
            observations.check(
                "failed-wake-followup-recovers-once",
                distinct,
                json!({"completedFollowUps":2,"distinctCalls":true}),
                json!(effects),
            );
            recovered
                .fault(CopyFaultRequest::Wake {
                    database: writer.database(),
                })
                .await?;
            let empty = sender(writer.clone(), receiver, &recovered.session)
                .replicate_once(receiver)
                .await?;
            let after = control_request(&mut recovered.control, ControlRequest::Effects).await?;
            let unchanged = matches!(&after,ControlAnswer::Effects(current) if current==&effects);
            observations.check(
                "second-wake-adds-no-followup-or-apply",
                empty.is_empty() && unchanged,
                json!({"effects":effects,"additionalPieces":0}),
                json!({"effects":after,"sender":format!("{empty:?}")}),
            );
            Ok(())
        }
        .await;
        recovered.stop().await?;
        restarted?;
        let after = read_snapshot(&path).await?;
        observations.check(
            "wake-recovery-does-not-repeat-apply",
            after == committed,
            json!(committed),
            json!(after),
        );
        Ok(())
    }
    .await;
    writer.close().await;
    result
}
