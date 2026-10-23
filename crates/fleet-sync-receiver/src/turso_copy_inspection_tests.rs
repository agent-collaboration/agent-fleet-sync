//! Operations diagnostics through the real receiver's persisted states.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_rule_fixture::{self as fixture, TestResult, TursoScope},
    *,
};
use fleet_sync_protocol::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::{collections::BTreeMap, path::Path, time::SystemTime};

async fn migrated_connection() -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    RECEIVER_MIGRATOR
        .run_direct(None, &mut connection, false)
        .await?;
    Ok(connection)
}

#[tokio::test]
async fn absent_and_complete_empty_corpora_are_uninitialized_without_creating_copy_state()
-> TestResult {
    let mut connection = TursoConnectOptions::new().connect().await?;
    assert_eq!(
        inspect_copy(&mut TursoScope(&mut connection)).await,
        CopyInspection::Uninitialized
    );
    let count: i64 = sqlx_core::query::query::<Turso>(
        "SELECT count(*) AS count FROM sqlite_schema WHERE name LIKE '_fleet_sync_%'",
    )
    .fetch_one(&mut connection)
    .await?
    .try_get("count")?;
    assert_eq!(
        count, 0,
        "inspection must not migrate absent receiver storage"
    );
    let mut connection = migrated_connection().await?;
    assert_eq!(
        inspect_copy(&mut TursoScope(&mut connection)).await,
        CopyInspection::Uninitialized
    );
    let count: i64 =
        sqlx_core::query::query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_copy")
            .fetch_one(&mut connection)
            .await?
            .try_get("count")?;
    assert_eq!(count, 0, "inspection must not create a copy record");
    Ok(())
}

#[tokio::test]
async fn partial_wrong_shape_and_unreadable_records_answer_damaged() -> TestResult {
    for damage in ["partial", "wrong type", "view", "record"] {
        let mut connection = migrated_connection().await?;
        let sql = match damage {
            "partial" => "DROP TABLE _fleet_sync_receipts",
            "wrong type" => {
                "DROP TABLE _fleet_sync_receipts; CREATE TABLE _fleet_sync_receipts(identity TEXT NOT NULL PRIMARY KEY,digest BLOB NOT NULL)"
            }
            "view" => {
                "DROP TABLE _fleet_sync_receipts; CREATE VIEW _fleet_sync_receipts AS SELECT 1 AS identity, 'x' AS digest"
            }
            _ => "INSERT INTO _fleet_sync_copy(singleton,record) VALUES(1,'invalid JSON')",
        };
        sqlx_core::raw_sql::raw_sql(sql)
            .execute(&mut connection)
            .await?;
        let answer = inspect_copy(&mut TursoScope(&mut connection)).await;
        assert!(
            matches!(answer, CopyInspection::Damaged { detail } if !detail.is_empty()),
            "{damage} was not reported as damage"
        );
    }
    Ok(())
}

#[tokio::test]
async fn live_inspection_preserves_each_persisted_stage_and_reports_actual_fingerprint()
-> TestResult {
    let mut starting = migrated_connection().await?;
    let position = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(7));
    let declared = SchemaIdentity::new(3, "declared source fingerprint".into())?;
    let source = CopyStatus {
        database: fixture::database()?,
        role: CopyRole::Authoritative,
        fence: Epoch::new(1)?,
        holder: Some(fixture::writer()?),
        start: position,
        acked: position,
        schema: declared.clone(),
    };
    let install = SnapshotInstall {
        install_id: InstallId::new(uuid::Uuid::from_u128(0x019eef00000070008000000000000300))?,
        database: source.database,
        position,
        schema: declared,
    };
    SnapshotInstallRules(&mut TursoScope(&mut starting))
        .prepare(&install, &source)
        .await?;
    match inspect_copy(&mut TursoScope(&mut starting)).await {
        CopyInspection::Live {
            stage: CopyStage::Starting,
            status,
            fingerprint,
            staged_parts,
        } => {
            assert_eq!(status.schema, source.schema);
            assert_eq!(fingerprint, schema_fingerprint(&[])?);
            assert_ne!(
                fingerprint, status.schema.fingerprint,
                "a mid-install difference is diagnostic, not damage"
            );
            assert!(staged_parts.is_empty());
        }
        answer => return Err(format!("expected Starting facts: {answer:?}").into()),
    }
    assert_eq!(
        copy_status(&mut TursoScope(&mut starting)).await?,
        CopyStatusAnswer::NotInitialized
    );

    let mut seeding = migrated_connection().await?;
    create_copy(
        &mut TursoScope(&mut seeding),
        &CreateCopy {
            database: fixture::database()?,
            epoch: Epoch::new(1)?,
            holder: Some(fixture::writer()?),
            origin: CopyOrigin::AwaitingSeed {
                expected_position: None,
            },
        },
    )
    .await?;
    // Inspection projects persisted replicated membership, not every local table.
    // Construct that schema-bearing Seeding state without claiming seed publication.
    sqlx_core::raw_sql::raw_sql(concat!(
        "CREATE TABLE seeding_records(id TEXT NOT NULL PRIMARY KEY, body TEXT);",
        "INSERT INTO _fleet_sync_copy_tables(name) VALUES('seeding_records');"
    ))
    .execute(&mut seeding)
    .await?;
    let recorded_before: String =
        sqlx_core::query::query::<Turso>("SELECT record FROM _fleet_sync_copy")
            .fetch_one(&mut seeding)
            .await?
            .try_get("record")?;
    let expected_seeding = schema_fingerprint(&[SchemaEntry {
        kind: SchemaObjectKind::Table,
        name: "seeding_records".into(),
        sql: "CREATE TABLE seeding_records(id TEXT NOT NULL PRIMARY KEY, body TEXT)".into(),
    }])?;
    match inspect_copy(&mut TursoScope(&mut seeding)).await {
        CopyInspection::Live {
            stage: CopyStage::Seeding,
            status,
            fingerprint,
            staged_parts,
        } => {
            assert_eq!(fingerprint, expected_seeding);
            assert_eq!(status.schema.fingerprint, schema_fingerprint(&[])?);
            assert_ne!(
                fingerprint, status.schema.fingerprint,
                "a mid-seed difference is diagnostic, not damage"
            );
            assert!(staged_parts.is_empty());
        }
        answer => return Err(format!("expected Seeding facts: {answer:?}").into()),
    }
    let recorded_after: String =
        sqlx_core::query::query::<Turso>("SELECT record FROM _fleet_sync_copy")
            .fetch_one(&mut seeding)
            .await?
            .try_get("record")?;
    assert_eq!(recorded_after, recorded_before);
    assert_eq!(
        copy_status(&mut TursoScope(&mut seeding)).await?,
        CopyStatusAnswer::NotInitialized
    );

    let mut following = fixture::connection().await?;
    let expected = schema_fingerprint(&[SchemaEntry {
        kind: SchemaObjectKind::Table,
        name: "records".into(),
        sql: "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT)".into(),
    }])?;
    match inspect_copy(&mut TursoScope(&mut following)).await {
        CopyInspection::Live {
            stage: CopyStage::Following,
            status,
            fingerprint,
            ..
        } => {
            assert_eq!(fingerprint, expected);
            assert_eq!(status.schema.fingerprint, expected);
        }
        answer => return Err(format!("expected Following facts: {answer:?}").into()),
    }
    Ok(())
}

#[tokio::test]
async fn retired_inspection_keeps_only_the_recorded_retirement_time() -> TestResult {
    let mut connection = fixture::connection().await?;
    let CopyStatusAnswer::Status(status) = copy_status(&mut TursoScope(&mut connection)).await?
    else {
        return Err("copy is not following".into());
    };
    assert_eq!(
        retire_copy(
            &mut TursoScope(&mut connection),
            &RetireCopy {
                database: status.database,
                expect_fence: status.fence,
                expect_acked: status.acked
            },
            SyncTime::from_unix_millis(42)
        )
        .await?,
        RetireCopyAnswer::Retired
    );
    assert_eq!(
        inspect_copy(&mut TursoScope(&mut connection)).await,
        CopyInspection::Retired {
            retired_at: SyncTime::from_unix_millis(42)
        }
    );
    Ok(())
}

fn read_store_bytes_and_times(
    directory: &Path,
) -> TestResult<BTreeMap<String, (Vec<u8>, SystemTime)>> {
    let mut result = BTreeMap::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            result.insert(
                entry.file_name().to_string_lossy().into_owned(),
                (std::fs::read(entry.path())?, entry.metadata()?.modified()?),
            );
        }
    }
    Ok(result)
}

#[tokio::test]
async fn interrupted_transaction_parts_keep_individual_times_and_inspection_writes_no_bytes()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let mut connection = fixture::connection_with_options(
        TursoConnectOptions::new()
            .filename(directory.path().join("copy.db"))
            .create_if_missing(true),
    )
    .await?;
    let rows = (1..=3)
        .map(|identity| {
            RowChange::upsert(
                "records",
                vec![fixture::uuid_key(identity)],
                vec![
                    fixture::uuid_key(identity),
                    SqlValue::Text(format!("row {identity}")),
                ],
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let whole = ReplicatedTransaction::Rows {
        commit_id: fixture::position(1)?.sequence,
        rows: rows.clone(),
    };
    let transaction_key = TransactionKey {
        epoch: Epoch::new(1)?,
        commit_id: whole.commit_id(),
    };
    for (index, row) in rows.into_iter().take(2).enumerate() {
        let piece = Piece::<()>::new(PieceContents {
            database: fixture::database()?,
            epoch: Epoch::new(1)?,
            continues_from: fixture::position(0)?,
            through: fixture::position(1)?,
            transactions: vec![ReplicatedTransaction::Rows {
                commit_id: whole.commit_id(),
                rows: vec![row],
            }],
            part: Some(TransactionPart::new(
                transaction_key,
                u32::try_from(index)?,
                3,
                PieceDigest::of_transaction(&whole)?,
            )?),
            annotation: None,
        })?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        assert!(matches!(
            apply_piece(
                &mut TursoScope(&mut transaction),
                &piece,
                &fixture::writer()?,
                &fixture::Hook { refuse: false },
                SyncTime::from_unix_millis(100 + i64::try_from(index)?)
            )
            .await?,
            ApplyDecision::PartStaged { .. }
        ));
        transaction.commit().await?;
    }
    let before = read_store_bytes_and_times(directory.path())?;
    for _ in 0..2 {
        match inspect_copy(&mut TursoScope(&mut connection)).await {
            CopyInspection::Live {
                stage: CopyStage::Following,
                staged_parts,
                ..
            } => assert_eq!(
                staged_parts,
                vec![StagedTransaction {
                    transaction_key,
                    parts: vec![
                        StagedPart {
                            index: 0,
                            staged_at: SyncTime::from_unix_millis(100)
                        },
                        StagedPart {
                            index: 1,
                            staged_at: SyncTime::from_unix_millis(101)
                        }
                    ]
                }]
            ),
            answer => return Err(format!("expected staged transaction facts: {answer:?}").into()),
        }
    }
    assert!(
        read_store_bytes_and_times(directory.path())? == before,
        "inspection changed database/sidecar bytes or modification times"
    );
    connection.close().await?;
    let mut reopened = TursoConnectOptions::new()
        .filename(directory.path().join("copy.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    assert!(
        matches!(inspect_copy(&mut TursoScope(&mut reopened)).await, CopyInspection::Live { staged_parts, .. } if staged_parts.len()==1 && staged_parts.first().is_some_and(|transaction| transaction.parts.len()==2))
    );
    Ok(())
}

#[test]
fn host_migration_pending_keeps_the_shared_retry_and_malformed_marker_precedence() {
    let reason = CopyUnavailableReason::MigrationPending { version: 7 };
    assert_eq!(
        classify_copy_store_error(copy_rollback(reason.clone())),
        CopyTransactionFailure::Unavailable(reason.clone())
    );
    assert!(matches!(
        retryable_copy(reason),
        durable_steps::CallFailure::Retryable { .. }
    ));
    let malformed = durable_steps::StoreError::StepRollback {
        failure: Box::new(durable_steps::OperationFailure::Refused {
            code: "copy_migration_pending".into(),
            detail: "invalid version".into(),
        }),
    };
    match classify_copy_store_error(malformed) {
        CopyTransactionFailure::OutcomeUnknown(OutcomeUnknownReason::StoreBoundary { detail }) => {
            assert!(detail.as_str().contains("copy rollback reason"))
        }
        answer => panic!("malformed version was not corrupt: {answer:?}"),
    }
}
