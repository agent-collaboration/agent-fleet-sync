use super::{ServiceProcess, control_request, copy_rows};
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::{ReplicatedDatabase, WriterOptions};
use std::{path::Path, time::Duration};

pub async fn import_backup(
    service: &ServiceProcess,
    control: &mut WebSocketConnection,
    path: &Path,
) -> TestResult<(Import, ReplicationPosition)> {
    let writer = ReplicatedDatabase::create(
        path,
        SampleSchema::new()?,
        PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(600),
            renew_interval: Duration::from_secs(30),
        },
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let mut write = writer.begin_write().await?;
    insert_sample(
        &mut write,
        "019eef00-0000-7000-8000-000000000001".into(),
        "imported backup".into(),
        9_007_199_254_740_993,
        vec![0, 255, 128],
    )
    .await?;
    write.commit().await?;
    let mut snapshot = writer.snapshot().await?;
    let manifest = snapshot.manifest().await?;
    let import = Import {
        attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
        database: writer.database(),
        digest: ImportDigest::of_canonical(&manifest)?,
    };
    assert!(matches!(
        control_request(control, ControlRequest::Import(import.clone())).await?,
        ControlAnswer::Import(ImportAnswer::Imported)
    ));
    let seed = match control_request(
        control,
        ControlRequest::SeedBegin {
            database: writer.database(),
            attempt: SeedAttemptId::new(durable_steps::Uuid::now_v7())?,
        },
    )
    .await?
    {
        ControlAnswer::Replication(answer) => match *answer {
            ReplicationAnswer::SeedStarted { seed } => seed,
            _ => return Err("import seed did not begin".into()),
        },
        _ => return Err("import seed answer missing".into()),
    };
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let contents = snapshot.page(cursor).await?;
        let next = contents.next.clone();
        let page = SeedPage {
            seed,
            page: pages,
            digest: PageDigest::of(&contents)?,
            contents,
        };
        assert!(
            matches!(control_request(control, ControlRequest::SeedPage(Box::new(page))).await?, ControlAnswer::Replication(answer) if matches!(*answer, ReplicationAnswer::SeedPage(SeedPageAnswer::Staged)))
        );
        pages += 1;
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert!(
        matches!(control_request(control, ControlRequest::SeedComplete(SeedComplete {
        database: writer.database(), seed, position: manifest.position, pages, schema: manifest.schema,
        table_digests: manifest.tables.into_iter().map(|table| (table.table, table.digest)).collect(),
    })).await?, ControlAnswer::Replication(answer) if matches!(*answer, ReplicationAnswer::SeedComplete(SeedAnswer::Published { position }) if position==manifest.position))
    );
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let rows = copy_rows(&mut reader, writer.database()).await?;
    assert_eq!(rows.get("samples").map(Vec::len), Some(1));
    assert!(rows.get("samples").ok_or("samples missing")?.iter().all(|row| matches!(row.as_slice(),[SqlValue::Text(_),SqlValue::Text(label),SqlValue::Integer(9_007_199_254_740_993),SqlValue::Blob(bytes)] if label=="imported backup" && bytes==&[0,255,128])));
    reader.close(CloseReason::ClientClosed).await;
    snapshot.close().await?;
    writer.close().await;
    Ok((import, manifest.position))
}

pub async fn assert_post_claim_imports(
    control: &mut WebSocketConnection,
    original: &Import,
) -> TestResult {
    let before = serde_json::to_value(
        control_request(
            control,
            ControlRequest::CopyStatus {
                database: original.database,
            },
        )
        .await?,
    )?;
    let mut repeated = original.clone();
    repeated.attempt = RequestId::new(durable_steps::Uuid::now_v7())?;
    assert_eq!(
        import_answer(control, repeated).await?,
        ImportAnswer::AlreadyImported
    );
    let offered = ImportDigest::of_canonical(&"different post-claim backup")?;
    let mut conflicting = original.clone();
    conflicting.attempt = RequestId::new(durable_steps::Uuid::now_v7())?;
    conflicting.digest = offered.clone();
    assert_eq!(
        import_answer(control, conflicting).await?,
        ImportAnswer::ImportConflict {
            existing: Some(original.digest.clone()),
            offered
        }
    );
    assert_eq!(
        import_answer(control, original.clone()).await?,
        ImportAnswer::Imported
    );
    assert_eq!(
        serde_json::to_value(
            control_request(
                control,
                ControlRequest::CopyStatus {
                    database: original.database
                }
            )
            .await?
        )?,
        before
    );
    Ok(())
}

async fn import_answer(
    control: &mut WebSocketConnection,
    request: Import,
) -> TestResult<ImportAnswer> {
    match control_request(control, ControlRequest::Import(request)).await? {
        ControlAnswer::Import(answer) => Ok(answer),
        _ => Err("import answer missing".into()),
    }
}
