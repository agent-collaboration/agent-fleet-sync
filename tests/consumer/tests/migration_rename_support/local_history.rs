use super::*;

pub(super) async fn check_copy_and_record(
    control: &mut WebSocketConnection,
    fixtures: &Path,
    checkpoint: &str,
) -> TestResult {
    // The fixture hook reads the catalog through the copy's owning transaction.
    let checks = match control_request(control, ControlRequest::PrivateHistoryChecks).await? {
        ControlAnswer::PrivateHistoryChecks(checks) => checks,
        _ => return Err("copy catalog observation missing".into()),
    };
    let path = fixtures.join(format!("checkpoint-{checkpoint}.json"));
    let mut record: serde_json::Value = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    let pieces = record["pieceCount"].as_u64().ok_or("piece count missing")?;
    assert!(
        checks >= pieces && checks > 0,
        "copy catalog not checked for each received piece at {checkpoint}"
    );
    record["nativeCatalogChecks"] = serde_json::json!(checks);
    record["absentTables"] = serde_json::json!(["private_history"]);
    record["nativeCatalogAbsence"] = serde_json::json!(true);
    tokio::fs::write(path, serde_json::to_vec_pretty(&record)?).await?;
    Ok(())
}

pub(super) async fn check_received_pieces(fixtures: &Path) -> TestResult {
    let mut entries = tokio::fs::read_dir(fixtures).await?;
    let mut count = 0;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_name().to_string_lossy().starts_with("piece-") {
            let received = tokio::fs::read_to_string(entry.path()).await?;
            assert!(
                !received.contains("private_history"),
                "local row or schema in received Piece"
            );
            assert!(
                !received.contains("private-local-only"),
                "local payload in received Piece"
            );
            count += 1;
        }
    }
    assert!(count > 0, "no received pieces were inspected");
    tokio::fs::write(
        fixtures.join("local-piece-absence.json"),
        serde_json::to_vec_pretty(
            &serde_json::json!({"receivedPieces":count,"privateTableAbsent":true}),
        )?,
    )
    .await?;
    Ok(())
}
