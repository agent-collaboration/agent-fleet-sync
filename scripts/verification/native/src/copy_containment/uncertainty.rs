//! Actual commit/rollback reply losses stay unknown and reconcile one Piece.
use super::{fault_controls::*, *};
use fleet_sync_test_consumer::CopyFaultRequest;
use std::sync::atomic::Ordering;

async fn case(
    root: &Path,
    observations: &mut Observations,
    rollback: bool,
    corrupt_reason: bool,
) -> TestResult {
    let label = if rollback {
        "rollback"
    } else if corrupt_reason {
        "unknown-reason"
    } else {
        "commit"
    };
    let root = root.join(format!("uncertainty-{label}"));
    tokio::fs::create_dir_all(&root).await?;
    let service_root = root.join("service");
    let peer = new_peer().await?;
    let receiver = new_receiver().await?;
    let damaged = new_writer(&root.join("writer-a.db"), peer, false).await?;
    let healthy = new_writer(&root.join("writer-b.db"), peer, false).await?;
    let result: TestResult = async {
        let first = RunningCopy::start(&service_root,&root.join("initial-client.db"),peer,receiver).await?;
        let prepared: TestResult = async {
            bootstrap(&damaged,peer,receiver,&first.session).await?;
            bootstrap(&healthy,peer,receiver,&first.session).await?;
            Ok(())
        }.await;
        first.stop().await?;
        prepared?;
        let damaged_path = copy_path(&service_root.join("service"),receiver,damaged.database());
        let healthy_path = copy_path(&service_root.join("service"),receiver,healthy.database());
        let before = read_snapshot(&damaged_path).await?;
        let before_healthy = read_snapshot(&healthy_path).await?;
        if rollback {write_record(&damaged_path,"{damaged-for-rollback").await?;}
        let baseline = read_snapshot(&damaged_path).await?;
        let mut running = RunningCopy::start(&service_root,&root.join("fault-client.db"),peer,receiver).await?;
        let observed: TestResult<Value> = async {
            let sending = sender(damaged.clone(),receiver,&running.session);
            let prior = sending.copies().await?;
            write_sample(&damaged,"piece with uncertain answer").await?;
            running.fault(if rollback {
                CopyFaultRequest::LoseRollbackReply {database:damaged.database()}
            } else {
                CopyFaultRequest::LoseCommitReply {database:damaged.database()}
            }).await?;
            running.session.wire.corrupt_next_unknown_reason.store(corrupt_reason,Ordering::SeqCst);
            let answer = sending.replicate_once(receiver).await;
            let after = sending.copies().await?;
            let good = if corrupt_reason {None} else {
                write_sample(&healthy,"healthy beside unknown outcome").await?;
                Some(sender(healthy.clone(),receiver,&running.session).replicate_once(receiver).await)
            };
            let frames = running.session.wire.frames.lock().await.clone();
            let pieces = sent_pieces(&frames);
            let original = pieces.iter().find(|piece|piece["database"]==json!(damaged.database())).cloned().ok_or("uncertain Piece missing")?;
            let offered_frame = frames.iter().find(|row|row["direction"]=="out" && row["frame"]["value"]["body"]["value"]["value"]==original).map(|row|row["frame"]["value"]["frame"].clone()).ok_or("uncertain frame ID missing")?;
            let response = frames.iter().find(|row|row["direction"]=="in" && row["frame"]["kind"]=="answer" && row["frame"]["value"]["frame"]==offered_frame).map(|row|row["frame"].clone()).ok_or("correlated unknown response missing")?;
            let expected_detail = if rollback {"storage backend: injected loss of rolled back transaction reply"} else {"storage backend: injected loss of committed transaction reply"};
            let unknown = response["value"]["body"]["kind"]=="replication"
                && response["value"]["body"]["value"]["kind"]=="outcomeUnknown"
                && response["value"]["body"]["value"]["value"]["reason"]["kind"]=="storeBoundary"
                && response["value"]["body"]["value"]["value"]["reason"]["value"]["detail"]==expected_detail;
            let injected = frames.iter().any(|row|row["event"]=="unknown-reason-injected");
            observations.check(&format!("unknown-{label}-wire-and-sender"),unknown && matches!(answer,Err(SenderError::Send(SendFailure::AnswerLost))) && prior==after && injected==corrupt_reason,
                json!({"answer":"outcomeUnknown","detail":expected_detail,"sender":"AnswerLost","acknowledgement":"unchanged","unknownReasonInjected":corrupt_reason}),
                json!({"answer":response,"sender":format!("{answer:?}"),"beforeAck":prior,"afterAck":after,"wire":frames}));
            if let Some(good)=good {
                observations.check(&format!("healthy-beside-unknown-{label}"),acknowledged(&good) && running.session.wire.connections.load(Ordering::SeqCst)==1,
                    json!({"acknowledged":true,"connections":1}),json!({"result":format!("{good:?}"),"connections":running.session.wire.connections.load(Ordering::SeqCst)}));
            }
            Ok(original)
        }.await;
        running.stop().await?;
        let original = observed?;
        let unknown_snapshot = read_snapshot(&damaged_path).await?;
        let actual_healthy = read_snapshot(&healthy_path).await?;
        let known_state = if rollback {unknown_snapshot==baseline} else {
            unknown_snapshot.rows.len()==before.rows.len()+1 && unknown_snapshot.receipts.len()==before.receipts.len()+1 && unknown_snapshot.hook_events.len()==before.hook_events.len()+1
        };
        observations.check(&format!("unknown-{label}-independent-storage-readback"),known_state,
            json!({"realCommit":!rollback,"newEffects":if rollback {0}else{1}}),json!(unknown_snapshot));
        if !corrupt_reason {
            observations.check(&format!("healthy-{label}-durable-progress"),actual_healthy.rows.len()==before_healthy.rows.len()+1 && actual_healthy.receipts.len()==before_healthy.receipts.len()+1,
                json!({"rows":before_healthy.rows.len()+1,"receipts":before_healthy.receipts.len()+1}),json!(actual_healthy));
        }
        if rollback {write_record(&damaged_path,before.record.as_deref().ok_or("original marker missing")?).await?;}
        let resumed = RunningCopy::start(&service_root,&root.join("resume-client.db"),peer,receiver).await?;
        let replayed: TestResult = async {
            let result=sender(damaged.clone(),receiver,&resumed.session).replicate_once(receiver).await;
            let wire=resumed.session.wire.frames.lock().await.clone();
            let pieces=sent_pieces(&wire);
            let answer_kind=if rollback {"applied"} else {"alreadyApplied"};
            let received=wire.iter().any(|row|row["direction"]=="in" && row["frame"]["value"]["body"]["kind"]=="replication" && row["frame"]["value"]["body"]["value"]["kind"]==answer_kind);
            observations.check(&format!("unknown-{label}-reconciles-original-piece"),acknowledged(&result) && pieces.first()==Some(&original) && pieces.len()==1 && received,
                json!({"piece":original,"answer":answer_kind}),json!({"pieces":pieces,"result":format!("{result:?}"),"wire":wire}));
            let duplicate=resumed.session.requests.request(OperationRequest::Replication(ReplicationRequest::Piece(serde_json::from_value(original.clone())?))).await?;
            observations.check(&format!("unknown-{label}-duplicate-is-receipted"),matches!(duplicate,OperationAnswer::Replication(ReplicationAnswer::AlreadyApplied {..})),json!({"answer":"alreadyApplied"}),json!(duplicate));
            Ok(())
        }.await;
        resumed.stop().await?;
        replayed?;
        let final_copy=read_snapshot(&damaged_path).await?;
        let exactly_once=if rollback {
            final_copy.rows.len()==before.rows.len()+1 && final_copy.receipts.len()==before.receipts.len()+1 && final_copy.hook_events.len()==before.hook_events.len()+1
        } else {final_copy==unknown_snapshot};
        observations.check(&format!("unknown-{label}-reconciles-once"),exactly_once,json!({"newEffects":1}),json!(final_copy));
        Ok(())
    }.await;
    damaged.close().await;
    healthy.close().await;
    result
}

pub(super) async fn observe(root: &Path, observations: &mut Observations) -> TestResult {
    case(root, observations, false, false).await?;
    case(root, observations, true, false).await?;
    case(root, observations, false, true).await
}
