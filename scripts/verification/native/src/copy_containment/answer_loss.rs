//! The dropped committed answer is a sender control, distinct from wake failure.
use super::*;
use std::sync::atomic::Ordering;

pub(super) async fn observe(
    root: &Path,
    service_root: &Path,
    peer: PeerId,
    receiver: ReceiverId,
    writer: &Arc<ReplicatedDatabase<SampleSchema>>,
    copy_file: &Path,
    observations: &mut Observations,
) -> TestResult {
    let before = read_snapshot(copy_file).await?;
    let service = ServiceProcess::start_as(service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let session = ObservedSession::start(
        &format!("ws://127.0.0.1:{}/session", service.port),
        peer,
        &root.join("answer-loss-client.db"),
    )
    .await?;
    let sending = sender(writer.clone(), receiver, &session);
    let prior_acknowledgement = sending.copies().await?;
    write_sample(writer, "committed-answer-lost").await?;
    session.wire.lose_next_applied.store(true, Ordering::SeqCst);
    let sent = sending.replicate_once(receiver).await;
    let after_acknowledgement = sending.copies().await?;
    let wire = session.stop().await?;
    service.stop(&mut control).await?;
    let committed = read_snapshot(copy_file).await?;
    let pieces = sent_pieces(&wire);
    let piece = pieces
        .first()
        .ok_or("answer-loss control sent no Piece")?
        .clone();
    let dropped = wire
        .iter()
        .any(|row| row["event"] == "committed-answer-dropped");
    observations.check(
        "committed-answer-loss-reaches-sender",
        dropped && matches!(sent, Err(SenderError::Send(SendFailure::AnswerLost)))
            && after_acknowledgement == prior_acknowledgement,
        json!({"answer":"AnswerLost","acknowledgement":"unchanged"}),
        json!({"result":format!("{sent:?}"),"wire":wire,"beforeAck":prior_acknowledgement,"afterAck":after_acknowledgement}),
    );
    observations.check(
        "lost-answer-committed-once",
        committed.rows.len() == before.rows.len() + 1
            && committed.receipts.len() == before.receipts.len() + 1
            && committed.hook_events.len() == before.hook_events.len() + 1,
        json!({"rows":before.rows.len()+1,"receipts":before.receipts.len()+1,"hookEvents":before.hook_events.len()+1}),
        json!(committed),
    );

    let service = ServiceProcess::start_as(service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let session = ObservedSession::start(
        &format!("ws://127.0.0.1:{}/session", service.port),
        peer,
        &root.join("answer-loss-replay-client.db"),
    )
    .await?;
    let replayed = sender(writer.clone(), receiver, &session)
        .replicate_once(receiver)
        .await;
    let wire = session.stop().await?;
    service.stop(&mut control).await?;
    let after_replay = read_snapshot(copy_file).await?;
    let resent = sent_pieces(&wire);
    let receipt_answer = wire.iter().any(|row| {
        row["direction"] == "in"
            && row["frame"]["value"]["body"]["kind"] == "replication"
            && row["frame"]["value"]["body"]["value"]["kind"] == "alreadyApplied"
    });
    let acknowledged = replayed.as_ref().is_ok_and(|events| {
        events
            .iter()
            .any(|event| matches!(event, ReplicationEvents::Acknowledged { .. }))
    });
    observations.check(
        "lost-answer-reconciles-identical-piece",
        pieces.len() == 1
            && resent.len() == 1
            && resent.first() == Some(&piece)
            && receipt_answer
            && acknowledged,
        json!({"piece":piece,"answer":"alreadyApplied","acknowledged":true}),
        json!({"pieces":resent,"result":format!("{replayed:?}"),"wire":wire}),
    );
    observations.check(
        "lost-answer-replay-has-no-effect",
        after_replay == committed,
        json!(committed),
        json!(after_replay),
    );
    Ok(())
}
