use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::*;
use fleet_sync_receiver::SnapshotInstall;
use fleet_sync_test_consumer::{SampleSchema, TestResult};
use fleet_sync_tokio_runner::StagingStore;
use fleet_sync_writer::{BootstrapGrant, ReplicatedDatabase, WriteRefusal, WriterOptions};
use runtime_primitives::{Clock, SystemClock};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::time::Duration;

const KEY: &str = "019eef00-0000-7000-8000-000000000001";
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(600),
        renew_interval: Duration::from_secs(30),
    }
}
fn peer(value: u128) -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::from_u128(value))?)
}

enum InvalidWrite {
    KeyAssignment,
    InvalidInsert,
}

#[tokio::test]
async fn warm_published_prefix_refuses_key_assignment() -> TestResult {
    prefix_admission(false, InvalidWrite::KeyAssignment).await
}
#[tokio::test]
async fn reopened_published_prefix_refuses_key_assignment() -> TestResult {
    prefix_admission(true, InvalidWrite::KeyAssignment).await
}
#[tokio::test]
async fn warm_published_prefix_refuses_invalid_insert() -> TestResult {
    prefix_admission(false, InvalidWrite::InvalidInsert).await
}
#[tokio::test]
async fn reopened_published_prefix_refuses_invalid_insert() -> TestResult {
    prefix_admission(true, InvalidWrite::InvalidInsert).await
}

async fn prefix_admission(reopen: bool, attempted: InvalidWrite) -> TestResult {
    let root = tempfile::tempdir()?;
    let source = ReplicatedDatabase::create(
        &root.path().join("source.db"),
        SampleSchema::rename_prefix()?,
        peer(2)?,
        policy(),
        WriterOptions::default(),
    )
    .await?;
    let staging = StagingStore::open(root.path().join("staging")).await?;
    let install = stage(&staging, &source).await?;
    let path = root.path().join("target.db");
    let now = SystemClock.now();
    let epoch = Epoch::new(2)?;
    let mut target = ReplicatedDatabase::seeding(
        &path,
        SampleSchema::rename_bundle()?,
        BootstrapGrant {
            lease: LeaseStatus {
                database: source.database(),
                holder: peer(4)?,
                epoch,
                state: LeaseState::Held,
                expires_at: now.after(Duration::from_secs(600)),
                remaining_ms: 600_000,
            },
            request_started: now,
            start: install.position,
        },
        policy(),
    )
    .await?;
    staging.publish_writer(&install, &target).await?;
    if reopen {
        target.close().await;
        target = ReplicatedDatabase::open(
            &path,
            install.database,
            SampleSchema::rename_bundle()?,
            LeaseHolder::seeding(install.database, peer(4)?, epoch, policy()),
        )
        .await?;
    }
    let result = async {
        if !matches!(target.begin_write().await, Err(WriteRefusal::Seeding)) {
            return Err("published prefix admitted a write before Ready".into());
        }
        let marker = target
            .installed_snapshot()
            .await?
            .ok_or("published marker missing")?;
        if marker.ready || marker.install != install {
            return Err("wrong publication state".into());
        }
        target
            .mark_bootstrap_ready(
                &install,
                ReceiverId::new(durable_steps::Uuid::from_u128(3))?,
            )
            .await?;
        let before = stored_state(&target).await?;
        let mut write = target.begin_write().await?;
        match attempted {
            InvalidWrite::KeyAssignment => {
                let answer = query::<Turso>("UPDATE records SET id='not-a-uuid'")
                    .execute(&mut write)
                    .await;
                let refused = matches!(answer.map_err(WriteRefusal::from_sqlx),
                    Err(WriteRefusal::KeyChange { table }) if table == "records");
                write.rollback().await?;
                if !refused {
                    return Err(
                        "prefix key assignment was not refused at statement admission".into(),
                    );
                }
            }
            InvalidWrite::InvalidInsert => {
                query::<Turso>("INSERT INTO records VALUES('not-a-uuid','invalid')")
                    .execute(&mut write)
                    .await?;
                let answer = write.commit().await;
                if !matches!(answer, Err(WriteRefusal::InvalidKey { table }) if table == "records")
                {
                    return Err("prefix invalid-key insertion was not refused before commit".into());
                }
            }
        }
        if stored_state(&target).await? != before {
            return Err("refused write changed rows or capture".into());
        }
        if before.rows != vec![(KEY.into(), "before".into())] {
            return Err("published prefix lost its source row".into());
        }
        Ok(())
    }
    .await;
    target.close().await;
    source.close().await;
    result
}

async fn stage(
    staging: &StagingStore,
    source: &ReplicatedDatabase<SampleSchema>,
) -> TestResult<SnapshotInstall> {
    let mut snapshot = source.snapshot().await?;
    let install = SnapshotInstall {
        install_id: durable_steps::Uuid::now_v7(),
        database: source.database(),
        position: snapshot.position(),
        schema: snapshot.schema().clone(),
    };
    // The source status comes from this actual snapshot; registry acquisition is
    // outside this storage/admission regression. The WebSocket journey covers it.
    staging
        .prepare(
            &install,
            &CopyStatus {
                database: install.database,
                role: CopyRole::Authoritative,
                fence: install.position.epoch,
                holder: Some(peer(2)?),
                start: ReplicationPosition::new(install.position.epoch, CommitSequence::new(0)),
                acked: install.position,
                schema: install.schema.clone(),
            },
        )
        .await?;
    let mut cursor = None;
    let mut page = 0;
    loop {
        let contents = snapshot.page(cursor.clone()).await?;
        staging
            .stage_page(&install, page, cursor.as_ref(), &contents)
            .await?;
        cursor = contents.next;
        page += 1;
        if cursor.is_none() {
            break;
        }
    }
    staging
        .verify(&install, &snapshot.manifest().await?)
        .await?;
    snapshot.close().await?;
    Ok(install)
}

#[derive(Debug, PartialEq)]
struct StoredState {
    rows: Vec<(String, String)>,
    capture: Vec<CapturedRow>,
}
#[derive(Debug, PartialEq)]
struct CapturedRow {
    change_id: i64,
    transaction_id: String,
    changed_at: String,
    kind: i64,
    table: Option<String>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
}
async fn stored_state(writer: &ReplicatedDatabase<SampleSchema>) -> TestResult<StoredState> {
    let rows = query::<Turso>("SELECT id,value FROM records ORDER BY id")
        .fetch_all(writer.reader())
        .await?
        .into_iter()
        .map(|row| Ok((row.try_get("id")?, row.try_get("value")?)))
        .collect::<Result<Vec<_>, sqlx_core::Error>>()?;
    let capture = query::<Turso>("SELECT change_id,CAST(change_txn_id AS TEXT) AS transaction_id,CAST(change_time AS TEXT) AS changed_at,change_type,table_name,before,after FROM turso_cdc ORDER BY change_id")
        .fetch_all(writer.reader()).await?.into_iter()
        .map(|row| Ok(CapturedRow {
            change_id: row.try_get("change_id")?, transaction_id: row.try_get("transaction_id")?,
            changed_at: row.try_get("changed_at")?, kind: row.try_get("change_type")?,
            table: row.try_get("table_name")?, before: row.try_get("before")?, after: row.try_get("after")?,
        })).collect::<Result<Vec<_>,sqlx_core::Error>>()?;
    Ok(StoredState { rows, capture })
}
