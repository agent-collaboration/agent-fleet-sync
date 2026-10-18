#![allow(clippy::panic_in_result_fn)]
use crate::{
    read_transaction_tests::{ReadSchema, writer},
    retained_file_fixture::*,
    *,
};
use fleet_sync_protocol::{GateReason, GateReasonCode};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    row::Row,
};
use sqlx_turso::TursoConnectOptions;

async fn read_and_check_guard(path: &std::path::Path) -> TestResult {
    let retained = RetainedFile::open_read_only(path, ReadSchema).await?;
    assert_eq!(
        retained.registration_outcome(),
        &WriterRegistrationOutcome::Refused {
            reason: fleet_sync_protocol::RegistrationRefusal::DatabaseRejected {
                reason: GateReason {
                    code: GateReasonCode::new("fixture_rejected")?,
                    detail: "retain after rejection".into(),
                },
            },
        }
    );
    let readers = retained.reader();
    assert_eq!(mark_count(readers).await?, 1);
    let mut read = readers.begin_read().await?;
    assert_eq!(
        (&mut read)
            .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
            .await?
            .try_get::<i64, _>("count")?,
        1
    );
    for sql in [
        "INSERT INTO local_marks VALUES('forbidden')",
        "CREATE TABLE forbidden(id TEXT)",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "PRAGMA wal_checkpoint(TRUNCATE)",
    ] {
        assert!(readers.execute(sql).await.is_err(), "pool admitted {sql}");
        assert!(
            (&mut read).execute(sql).await.is_err(),
            "transaction admitted {sql}"
        );
    }
    assert_eq!(
        (&mut read)
            .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
            .await?
            .try_get::<i64, _>("count")?,
        1
    );
    read.close().await?;
    assert_eq!(mark_count(readers).await?, 1);
    drop(retained);
    Ok(())
}

#[tokio::test]
async fn retained_moved_file_survives_restart_without_changing_any_file() -> TestResult {
    if let Some((action, path)) = child_action() {
        assert_eq!(action, "read-moved");
        return read_and_check_guard(&path).await;
    }
    // Arrange a real rejected registration, actual move_aside and process restart.
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("writer.db");
    let retained = directory.path().join("retained.db");
    let database = writer(&source).await?;
    add_mark(&database, "retained").await?;
    database
        .reject_registration(GateReason {
            code: GateReasonCode::new("fixture_rejected")?,
            detail: "retain after rejection".into(),
        })
        .await?;
    database.move_aside(&retained).await?;
    drop(database);
    let _permissions = ReadOnlyFixture::apply(directory.path())?;
    let before = snapshot(directory.path())?;

    // Act: only the public retained opener and guarded reads in a fresh process.
    child(
        "retained_file_tests::retained_moved_file_survives_restart_without_changing_any_file",
        "read-moved",
        &retained,
    )
    .await?;

    // Assert every entry, size, SHA-256, mtime, mode and the directory mtime.
    assert_eq!(snapshot(directory.path())?, before);
    assert!(!source.exists());
    Ok(())
}

#[tokio::test]
async fn retained_missing_file_is_refused_without_creating_an_entry() -> TestResult {
    let directory = tempfile::tempdir()?;
    let _permissions = ReadOnlyFixture::apply(directory.path())?;
    let before = snapshot(directory.path())?;
    assert!(
        RetainedFile::open_read_only(&directory.path().join("missing.db"), ReadSchema)
            .await
            .is_err()
    );
    assert_eq!(snapshot(directory.path())?, before);
    Ok(())
}

#[tokio::test]
async fn retained_unknown_bookkeeping_is_refused_and_preserved() -> TestResult {
    for mutation in [
        "UPDATE _fleet_sync_writer_engine_history SET version=0",
        "UPDATE _fleet_sync_writer_engine_history SET version=2",
        "DELETE FROM _fleet_sync_writer_engine_history",
        "UPDATE _fleet_sync_writer_engine_history SET checksum=x'00'",
        "UPDATE _fleet_sync_writer_engine_history SET success=0",
    ] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("retained.db");
        let database = writer(&path).await?;
        database.close().await;
        drop(database);
        let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
        connection.execute(mutation).await?;
        connection.close().await?;
        let _permissions = ReadOnlyFixture::apply(directory.path())?;
        let before = snapshot(directory.path())?;
        assert!(
            matches!(
                RetainedFile::open_read_only(&path, ReadSchema).await,
                Err(OpenError::RetainedFormatUnsupported)
            ),
            "accepted {mutation}"
        );
        assert_eq!(snapshot(directory.path())?, before, "{mutation}");
    }
    Ok(())
}

#[tokio::test]
async fn retained_application_history_is_checked_without_running_migrations() -> TestResult {
    for mutation in [
        "UPDATE _fleet_sync_local_migrations SET version=9000",
        "UPDATE _fleet_sync_local_migrations SET checksum=x'00'",
        "DELETE FROM _fleet_sync_local_migrations",
    ] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("retained.db");
        let database = writer(&path).await?;
        database.close().await;
        drop(database);
        let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
        connection.execute(mutation).await?;
        connection.close().await?;
        let _permissions = ReadOnlyFixture::apply(directory.path())?;
        let before = snapshot(directory.path())?;
        assert!(
            matches!(
                RetainedFile::open_read_only(&path, ReadSchema).await,
                Err(OpenError::RetainedFormatUnsupported)
            ),
            "accepted {mutation}"
        );
        assert_eq!(snapshot(directory.path())?, before, "{mutation}");
    }
    Ok(())
}

#[tokio::test]
async fn retained_grant_reports_the_exact_persisted_ready_outcome() -> TestResult {
    use fleet_sync_protocol::{Epoch, LeaseState, LeaseStatus};
    use runtime_primitives::Clock;
    use std::time::Duration;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("retained.db");
    let database = writer(&path).await?;
    let identity = WriterIdentity::observe(&database).await;
    let started = runtime_primitives::SystemClock.now();
    let lease = LeaseStatus {
        database: identity.database,
        holder: identity.peer,
        epoch: Epoch::new(1)?,
        state: LeaseState::Held,
        expires_at: started.after(Duration::from_secs(60)),
        remaining_ms: 60_000,
    };
    // Persist the real public writer observation; no authority row is forged.
    // Registry transport and central admission remain separate integration proof.
    database
        .observe_registration(lease.clone(), started)
        .await?;
    database.close().await;
    drop(database);
    let _permissions = ReadOnlyFixture::apply(directory.path())?;
    let before = snapshot(directory.path())?;

    let retained = RetainedFile::open_read_only(&path, ReadSchema).await?;
    assert_eq!(
        retained.registration_outcome(),
        &WriterRegistrationOutcome::Ready { lease }
    );
    assert_eq!(mark_count(retained.reader()).await?, 0);
    let pool = retained.reader().pool.current()?;
    drop(retained);
    pool.close().await;
    drop(pool);
    assert_eq!(snapshot(directory.path())?, before);
    Ok(())
}

#[tokio::test]
async fn retained_unknown_or_invalid_authority_is_refused_without_repair() -> TestResult {
    for authority in [
        serde_json::json!({"kind": "futureAuthority"}),
        serde_json::json!({"kind": "registrationRejected"}),
    ] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("retained.db");
        let database = writer(&path).await?;
        database.close().await;
        drop(database);
        let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
        let row =
            sqlx_core::query::query::<sqlx_turso::Turso>("SELECT record FROM _fleet_sync_writer")
                .fetch_one(&mut connection)
                .await?;
        let mut record: serde_json::Value =
            serde_json::from_str(&row.try_get::<String, _>("record")?)?;
        record["authority"] = authority;
        sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_writer SET record=?")
            .bind(serde_json::to_string(&record)?)
            .execute(&mut connection)
            .await?;
        connection.close().await?;
        let _permissions = ReadOnlyFixture::apply(directory.path())?;
        let before = snapshot(directory.path())?;

        assert!(matches!(
            RetainedFile::open_read_only(&path, ReadSchema).await,
            Err(OpenError::Corrupt { field }) if field == "writer.record"
        ));
        assert_eq!(snapshot(directory.path())?, before);
    }
    Ok(())
}

#[tokio::test]
async fn retained_authority_for_another_database_is_refused_without_repair() -> TestResult {
    use fleet_sync_protocol::{
        DatabaseId, Epoch, LeaseState, LeaseStatus, RenewAnswer, RenewResult, SyncTime,
    };
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("retained.db");
    let database = writer(&path).await?;
    let identity = WriterIdentity::observe(&database).await;
    let other_database = DatabaseId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000c13,
    ))?;
    assert_ne!(other_database, identity.database);
    database.close().await;
    drop(database);
    let authority = crate::writer_authority::AuthorityObservation::Renewal {
        answer: RenewAnswer::Renewed {
            recorded: RenewResult {
                epoch: Epoch::new(1)?,
                expires_at: SyncTime::from_unix_millis(60_000),
            },
            current: LeaseStatus {
                database: other_database,
                holder: identity.peer,
                epoch: Epoch::new(1)?,
                state: LeaseState::Held,
                expires_at: SyncTime::from_unix_millis(60_000),
                remaining_ms: 60_000,
            },
            transfer_in_progress: None,
            replayed: false,
        },
        request_started: SyncTime::from_unix_millis(0),
    };
    let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
    let row = sqlx_core::query::query::<sqlx_turso::Turso>("SELECT record FROM _fleet_sync_writer")
        .fetch_one(&mut connection)
        .await?;
    let mut record: serde_json::Value = serde_json::from_str(&row.try_get::<String, _>("record")?)?;
    record["authority"] = serde_json::to_value(authority)?;
    sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_writer SET record=?")
        .bind(serde_json::to_string(&record)?)
        .execute(&mut connection)
        .await?;
    connection.close().await?;
    let _permissions = ReadOnlyFixture::apply(directory.path())?;
    let before = snapshot(directory.path())?;

    match RetainedFile::open_read_only(&path, ReadSchema).await {
        Err(OpenError::Holder {
            error: fleet_sync_lease::HolderError::LeaseMismatch,
        }) => {}
        Err(error) => return Err(format!("wrong authority refusal: {error:?}").into()),
        Ok(retained) => {
            return Err(format!(
                "authority for another database became {:?}",
                retained.registration_outcome()
            )
            .into());
        }
    }
    assert_eq!(snapshot(directory.path())?, before);
    Ok(())
}
