//! The actual native replication host owns staged cleanup and its failure answer.
#[path = "support/copy_fixture.rs"]
#[allow(dead_code)]
mod fixture;
#[allow(dead_code, unused_imports)]
mod support;
use fixture::*;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{SampleSchema, TestResult};
use fleet_sync_writer::{ReplicatedDatabase, WriterOptions};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{path::Path, sync::Arc};

struct StagedOffer {
    database: DatabaseId,
    acked: ReplicationPosition,
    key: TransactionKey,
    first: Piece<String>,
    invalid_final: Piece<String>,
}

async fn send_piece(session: &CopySession, piece: Piece<String>) -> TestResult<ReplicationAnswer> {
    match session
        .requests
        .request(OperationRequest::Replication(ReplicationRequest::Piece(
            piece,
        )))
        .await?
    {
        OperationAnswer::Replication(answer) => Ok(answer),
        _ => Err("replication request returned another answer family".into()),
    }
}

async fn prepare_copy(root: &Path, service_root: &Path) -> TestResult<StagedOffer> {
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &root.join("writer.db"),
            SampleSchema::new()?,
            writer_peer()?,
            lease_policy(),
            WriterOptions::default(),
        )
        .await?,
    );
    let service = CopyService::start(service_root, &root.join("prepare-client.db")).await?;
    let prepared = async {
        let lease = register_writer(&writer, &service.session).await?;
        let sender = replication_sender(Arc::clone(&writer), &service.session)?;
        sender
            .add_copy(
                local_receiver()?,
                CopyRole::Authoritative,
                ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
            )
            .await?;
        sender.replicate_once(local_receiver()?).await?;
        let status = match service
            .session
            .requests
            .request(OperationRequest::Replication(
                ReplicationRequest::CopyStatus {
                    database: writer.database(),
                },
            ))
            .await?
        {
            OperationAnswer::Replication(ReplicationAnswer::CopyStatus(
                CopyStatusAnswer::Status(status),
            )) => status,
            _ => return Err("prepared copy is not following".into()),
        };
        let sequence = CommitSequence::new(
            status
                .acked
                .sequence
                .value()
                .checked_add(1)
                .ok_or("sequence exhausted")?,
        );
        let key = TransactionKey {
            epoch: status.fence,
            commit_id: sequence,
        };
        let rows = ["first", "second"]
            .into_iter()
            .map(|label| {
                let id = durable_steps::Uuid::now_v7().to_string();
                RowChange::upsert(
                    "samples",
                    vec![SqlValue::Text(id.clone())],
                    vec![
                        SqlValue::Text(id),
                        SqlValue::Text(label.into()),
                        SqlValue::Integer(42),
                        SqlValue::Blob(vec![0, 255]),
                    ],
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let whole = ReplicatedTransaction::Rows {
            commit_id: sequence,
            rows: rows.clone(),
        };
        let digest = PieceDigest::of_transaction(&whole)?;
        let through = ReplicationPosition::new(status.fence, sequence);
        let first = Piece::new(PieceContents {
            database: writer.database(),
            epoch: status.fence,
            continues_from: status.acked,
            through,
            transactions: vec![ReplicatedTransaction::Rows {
                commit_id: sequence,
                rows: rows.into_iter().take(1).collect(),
            }],
            part: Some(TransactionPart::new(key, 0, 2, digest.clone())?),
            annotation: None,
        })?;
        let invalid_final = Piece::new(PieceContents {
            database: writer.database(),
            epoch: status.fence,
            continues_from: status.acked,
            through,
            transactions: vec![ReplicatedTransaction::Rows {
                commit_id: sequence,
                rows: Vec::new(),
            }],
            part: Some(TransactionPart::new(key, 1, 2, digest)?),
            annotation: None,
        })?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(StagedOffer {
            database: writer.database(),
            acked: status.acked,
            key,
            first,
            invalid_final,
        })
    }
    .await;
    let stopped = service.kill().await;
    writer.close().await;
    stopped?;
    prepared
}

async fn staged_parts(path: &Path) -> TestResult<Vec<(String, String, String)>> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let result = sqlx_core::query::query::<Turso>(
        "SELECT transaction_key, CAST(part_index AS TEXT) AS part_index, payload FROM _fleet_sync_staged_parts ORDER BY transaction_key, part_index",
    ).fetch_all(&mut connection).await?.into_iter().map(|row| {
        Ok((row.try_get("transaction_key")?, row.try_get("part_index")?, row.try_get("payload")?))
    }).collect::<Result<_, sqlx_core::Error>>();
    connection.close().await?;
    Ok(result?)
}

async fn make_cleanup_read_only(path: &Path) -> TestResult {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    for sql in [
        "ALTER TABLE _fleet_sync_staged_parts RENAME TO _fleet_sync_staged_parts_backing",
        "CREATE VIEW _fleet_sync_staged_parts AS SELECT transaction_key,part_index,staged_at,payload FROM _fleet_sync_staged_parts_backing",
    ] {
        sqlx_core::query::query::<Turso>(sql)
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    connection.close().await?;
    Ok(())
}

async fn repair_cleanup_table(path: &Path) -> TestResult {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    for sql in [
        "DROP VIEW _fleet_sync_staged_parts",
        "ALTER TABLE _fleet_sync_staged_parts_backing RENAME TO _fleet_sync_staged_parts",
    ] {
        sqlx_core::query::query::<Turso>(sql)
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test]
async fn native_discarding_refusal_clears_staging_without_publishing_rows() -> TestResult {
    let root = tempfile::tempdir()?;
    let service_root = root.path().join("service");
    let offer = prepare_copy(root.path(), &service_root).await?;
    let path = copy_file_path(&service_root, offer.database)?;
    let before = read_copy_snapshot(&path).await?;
    assert!(staged_parts(&path).await?.is_empty());
    let service = CopyService::start(&service_root, &root.path().join("discard-client.db")).await?;
    let observed = async {
        let first = send_piece(&service.session, offer.first).await?;
        let refused = send_piece(&service.session, offer.invalid_final).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((first, refused))
    }
    .await;
    service.kill().await?;
    let (first, refused) = observed?;
    assert_eq!(
        first,
        ReplicationAnswer::PartStaged {
            transaction: offer.key,
            index: 0,
            acked: offer.acked
        }
    );
    assert_eq!(
        refused,
        ReplicationAnswer::Refused(PieceRefusal::Gap { acked: offer.acked })
    );
    assert!(
        staged_parts(&path).await?.is_empty(),
        "native refusal retained staged parts"
    );
    assert_eq!(read_copy_snapshot(&path).await?, before);
    Ok(())
}

#[tokio::test]
async fn native_cleanup_sql_failure_is_unknown_then_identical_resend_recovers() -> TestResult {
    let root = tempfile::tempdir()?;
    let service_root = root.path().join("service");
    let offer = prepare_copy(root.path(), &service_root).await?;
    let path = copy_file_path(&service_root, offer.database)?;
    let initial = CopyService::start(&service_root, &root.path().join("stage-client.db")).await?;
    let staged = send_piece(&initial.session, offer.first.clone()).await;
    initial.kill().await?;
    assert_eq!(
        staged?,
        ReplicationAnswer::PartStaged {
            transaction: offer.key,
            index: 0,
            acked: offer.acked
        }
    );
    let before = read_copy_snapshot(&path).await?;
    let staging = staged_parts(&path).await?;
    assert_eq!(staging.len(), 1);
    // All service and client owners are stopped. The projection stays readable;
    // only the real native cleanup DELETE is prevented by the fixture's view.
    make_cleanup_read_only(&path).await?;
    assert_eq!(staged_parts(&path).await?, staging);
    let faulted = CopyService::start(&service_root, &root.path().join("fault-client.db")).await?;
    let observed = async {
        let duplicate = send_piece(&faulted.session, offer.first).await?;
        let refused = send_piece(&faulted.session, offer.invalid_final.clone()).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((duplicate, refused))
    }
    .await;
    faulted.kill().await?;
    let (duplicate, answer) = observed?;
    assert_eq!(
        duplicate,
        ReplicationAnswer::PartStaged {
            transaction: offer.key,
            index: 0,
            acked: offer.acked
        }
    );
    assert!(
        matches!(&answer, ReplicationAnswer::OutcomeUnknown {
        reason: OutcomeUnknownReason::StoreBoundary { detail },
        } if detail.as_str().contains("no such table: _fleet_sync_staged_parts")),
        "unexpected native cleanup answer: {answer:?}"
    );
    assert_eq!(staged_parts(&path).await?, staging);
    assert_eq!(read_copy_snapshot(&path).await?, before);
    repair_cleanup_table(&path).await?;
    let repaired =
        CopyService::start(&service_root, &root.path().join("repaired-client.db")).await?;
    let recovered = send_piece(&repaired.session, offer.invalid_final).await;
    repaired.kill().await?;
    assert_eq!(
        recovered?,
        ReplicationAnswer::Refused(PieceRefusal::Gap { acked: offer.acked })
    );
    assert!(staged_parts(&path).await?.is_empty());
    assert_eq!(read_copy_snapshot(&path).await?, before);
    Ok(())
}
