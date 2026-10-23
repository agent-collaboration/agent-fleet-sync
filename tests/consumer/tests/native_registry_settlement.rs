use fleet_sync_lease::{LeasePolicy, RegistryConfig};
use fleet_sync_protocol::{DatabaseId, Import, ImportAnswer, ImportDigest, ReceiverId, RequestId};
use fleet_sync_service::{CopyCallKind, FleetService, ServiceError, ServiceOptions};
use fleet_sync_test_consumer::{
    Consumer, ConsumerStore, FixtureAuthenticator, SampleProtocol, TestResult,
};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::watch, time::timeout};

async fn wait_import_checkpoint(
    connection: &mut TursoConnection,
    dispatched: bool,
) -> TestResult<String> {
    timeout(Duration::from_secs(5), async {
        loop {
            let row = query::<Turso>(
                "SELECT operation_id, drive_token, EXISTS(SELECT 1 FROM _fleet_sync_steps s WHERE s.operation_id=o.operation_id AND step_name='create_copy' AND may_have_reached=1) AS dispatched FROM _fleet_sync_operations o WHERE kind='fleet-sync.import' AND state='running'",
            )
            .fetch_optional(&mut *connection)
            .await?;
            if let Some(row) = row {
                let has_lease = row.try_get::<Option<String>, _>("drive_token")?.is_some();
                if !dispatched || (has_lease && row.try_get::<i64, _>("dispatched")? == 1) {
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        row.try_get("operation_id")?,
                    );
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
}

type NativeService = FleetService<SampleProtocol, Consumer, FixtureAuthenticator>;

async fn open_registry_service(directory: &Path) -> TestResult<(NativeService, ConsumerStore)> {
    let store = ConsumerStore::open(&directory.join("consumer.db")).await?;
    let receiver = ReceiverId::new(durable_steps::Uuid::now_v7())?;
    let service_path = directory.join("service");
    let service = FleetService::<SampleProtocol, _, _>::open(
        ServiceOptions {
            directory: service_path.clone(),
            delivery: None,
            receiver,
            registry: RegistryConfig {
                lease: LeasePolicy {
                    lease_duration: Duration::from_secs(600),
                    renew_interval: Duration::from_secs(30),
                },
                authoritative_receiver: receiver,
                release_unsent_for: Duration::from_secs(5),
                move_needs_renewal_within: None,
            },
            call_timeout: Duration::from_secs(10),
            socket_timeout: Duration::from_secs(5),
        },
        Arc::new(Consumer::new(store.clone())?),
        Arc::new(FixtureAuthenticator::default()),
    )
    .await?;
    Ok((service, store))
}

#[tokio::test]
async fn import_waits_for_its_runner_owned_operation_to_complete() -> TestResult {
    // Arrange: persist admission while all registry execution slots are held,
    // then cancel only that request future, before any drive can claim it.
    let directory = tempfile::tempdir()?;
    let service_path = directory.path().join("service");
    let (service, store) = open_registry_service(directory.path()).await?;
    let handle = service.handle();
    let request = Import {
        attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
        database: DatabaseId::new(durable_steps::Uuid::now_v7())?,
        digest: ImportDigest::of_canonical(&"runner-owned import")?,
    };
    let registry_hold = handle.hold_registry_drive().await;
    handle
        .hold_copy_call_for_test(request.database, CopyCallKind::Create)
        .await?;
    let mut journal = TursoConnectOptions::new()
        .filename(service_path.join("registry.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    let mut admission = Box::pin(handle.import(request.clone()));
    let operation = tokio::select! {
        result = &mut admission => return Err(format!("admission passed the held drive: {result:?}").into()),
        operation = wait_import_checkpoint(&mut journal, false) => operation?,
    };
    drop(admission);
    drop(registry_hold);

    // Only the service's joined registry runner can now own the admitted task.
    // Its real CopyCreate dispatch is recorded, but copy execution cannot finish.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let (shutdown, stopped) = watch::channel(false);
    let serving = tokio::spawn(service.serve(listener, axum::Router::new(), stopped));
    let call = timeout(
        Duration::from_secs(5),
        handle.wait_copy_call_for_test(request.database),
    )
    .await??;
    assert_eq!(call.operation.as_uuid().to_string(), operation);
    assert_eq!(wait_import_checkpoint(&mut journal, true).await?, operation);

    // Act: the real service request must join this operation's completion.
    // On the old build this returns the exact ServiceError::Pending immediately.
    let mut repeated = Box::pin(handle.import(request.clone()));
    let premature = timeout(Duration::from_secs(1), &mut repeated).await;
    if let Ok(Err(error)) = &premature {
        eprintln!("premature import answer: {error}");
    }
    let returned_early = premature.is_ok();
    handle.release_copy_call_for_test(request.database).await?;
    let answer = match premature {
        Ok(answer) => answer,
        Err(_) => timeout(Duration::from_secs(5), &mut repeated).await?,
    };
    drop(repeated);
    shutdown.send_replace(true);
    timeout(Duration::from_secs(5), serving).await???;

    // Assert against durable completion, not merely the caller's answer.
    let row = query::<Turso>("SELECT state FROM _fleet_sync_operations WHERE operation_id=?")
        .bind(operation)
        .fetch_one(&mut journal)
        .await?;
    assert_eq!(row.try_get::<String, _>("state")?, "completed");
    journal.close().await?;
    store.close().await;
    assert_eq!(answer?, ImportAnswer::Imported);
    assert!(
        !returned_early,
        "request returned before its held operation settled"
    );
    Ok(())
}

#[tokio::test]
async fn import_execution_corruption_preserves_the_service_storage_error() -> TestResult {
    // Admission commits normally. Corrupt only its durable execution input,
    // while DriveHold prevents the first journal claim or external dispatch.
    let directory = tempfile::tempdir()?;
    let (service, store) = open_registry_service(directory.path()).await?;
    let handle = service.handle();
    let held = handle.hold_registry_drive().await;
    let mut journal = TursoConnectOptions::new()
        .filename(directory.path().join("service/registry.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    let request = Import {
        attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
        database: DatabaseId::new(durable_steps::Uuid::now_v7())?,
        digest: ImportDigest::of_canonical(&"execution error mapping")?,
    };
    let mut importing = tokio::spawn(async move { handle.import(request).await });
    let corrupted = timeout(Duration::from_secs(5), async {
        let operation = wait_import_checkpoint(&mut journal, false).await?;
        query::<Turso>("UPDATE _fleet_sync_operations SET input_digest=? WHERE operation_id=?")
            .bind(vec![0_u8; 32])
            .bind(operation)
            .execute(&mut journal)
            .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await;
    drop(held);
    let result = match timeout(Duration::from_secs(5), &mut importing).await {
        Ok(result) => result?,
        Err(error) => {
            importing.abort();
            let _joined = importing.await;
            return Err(error.into());
        }
    };
    journal.close().await?;
    drop(service);
    store.close().await;
    corrupted??;
    assert!(
        matches!(&result, Err(ServiceError::Storage(durable_steps::StoreError::Corrupt { field, .. })) if field=="input_digest"),
        "execution corruption lost its Storage classification: {result:?}"
    );
    Ok(())
}
