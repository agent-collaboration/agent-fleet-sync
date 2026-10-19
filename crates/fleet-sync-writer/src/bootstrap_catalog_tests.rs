#![allow(clippy::panic_in_result_fn)]
use super::*;
use crate::schema_catalog_tests::{CatalogSchema, TABLES, TOO_LARGE, catalog_state_on};
use fleet_sync_protocol::*;
use std::{sync::atomic::AtomicBool, time::Duration};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn publication_transaction_refuses_over_bound_imported_catalog_and_rolls_back() -> TestResult
{
    let root = tempfile::tempdir()?;
    let now = SyncTime::from_unix_millis(10);
    let database = DatabaseId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000001,
    ))?;
    let holder = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let position = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(20));
    let target = ReplicatedDatabase::seeding(
        &root.path().join("target.db"),
        CatalogSchema {
            expanded: Arc::new(AtomicBool::new(true)),
            tables: TABLES.as_ref().map_err(Clone::clone)?,
        },
        BootstrapGrant {
            lease: LeaseStatus {
                database,
                holder,
                epoch: Epoch::new(2)?,
                state: LeaseState::Held,
                expires_at: now.after(Duration::from_secs(600)),
                remaining_ms: 600_000,
            },
            request_started: now,
            start: position,
        },
        LeasePolicy {
            lease_duration: Duration::from_secs(600),
            renew_interval: Duration::from_secs(60),
        },
    )
    .await?;
    let record = {
        let mut connection = target.maintenance.acquire().await?;
        read_file_record(&mut connection)
            .await?
            .ok_or("missing seeding record")?
    };
    target.close().await;

    // An imported file can predate admission bounds. Exercise the real owned
    // conversion transaction directly so prefix preflight cannot mask its gate.
    let mut staged = TursoConnectOptions::new()
        .filename(root.path().join("staged.db"))
        .create_if_missing(true)
        .connect()
        .await?;
    let mut bookkeeping = sqlx::migrate!("./migrations");
    bookkeeping.dangerous_set_table_name("_fleet_sync_writer_migrations");
    bookkeeping.run_direct(None, &mut staged, false).await?;
    Migrator::with_migrations(TOO_LARGE.clone())
        .run_direct(None, &mut staged, false)
        .await?;
    // Received rows have no applied consumer prefix yet. Conversion must add
    // those SQLx records inside its publication transaction and roll them back.
    staged.execute("DELETE FROM _sqlx_migrations").await?;
    write_file_record(&mut staged, &record).await?;
    staged
        .execute("PRAGMA capture_data_changes_conn('full')")
        .await?;
    staged
        .execute("PRAGMA capture_data_changes_conn('off')")
        .await?;
    let entries = crate::schema_inspection::schema_entries(&mut staged).await?;
    let schema = SchemaIdentity::new(
        2,
        schema_fingerprint(
            &entries
                .into_iter()
                .filter(|(entry, _)| entry.name == "records")
                .map(|(entry, _)| entry)
                .collect::<Vec<_>>(),
        )?,
    )?;
    let install = SnapshotInstall {
        install_id: InstallId::new(durable_steps::Uuid::now_v7())?,
        database,
        position,
        schema,
    };
    let before = catalog_state_on(&mut staged).await?;

    let result = ReplicatedDatabase::<CatalogSchema>::convert_staged_bootstrap(
        &mut staged,
        &install,
        TOO_LARGE.clone(),
        TABLES.as_ref().map_err(Clone::clone)?,
    )
    .await;
    assert!(
        matches!(result, Err(BootstrapError::Writer(OpenError::SchemaUnsupported {
        form: crate::SchemaForm::SnapshotSchema { encoded_size },
    })) if encoded_size > 256 * 1024),
        "publication transaction did not retain its own size gate: {result:?}"
    );
    assert_eq!(catalog_state_on(&mut staged).await?, before);
    staged.close().await?;
    Ok(())
}
