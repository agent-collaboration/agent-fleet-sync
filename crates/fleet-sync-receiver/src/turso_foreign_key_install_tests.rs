//! A matching manifest cannot certify an orphaned staging database.
#![allow(clippy::panic_in_result_fn)]
use crate::turso_seed_tests::{TursoScope, database, position};
use crate::*;
use fleet_sync_protocol::*;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_turso::TursoConnectOptions;
type TestResult = Result<(), Box<dyn std::error::Error>>;
#[tokio::test]
async fn snapshot_manifest_match_still_refuses_orphans_before_verified_or_ready() -> TestResult {
    let (pages, done) =
        crate::turso_foreign_key_tests::seeded_pages(SeedId { generation: 1 }, false)?;
    let install = SnapshotInstall {
        install_id: InstallId::new("019f0000-0000-7000-8000-000000000051".parse()?)?,
        database: database()?,
        position: position()?,
        schema: done.schema.clone(),
    };
    let source = CopyStatus {
        database: database()?,
        role: CopyRole::Authoritative,
        fence: Epoch::new(1)?,
        holder: Some(crate::turso_rule_fixture::writer()?),
        start: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
        acked: position()?,
        schema: done.schema.clone(),
    };
    let manifest = SnapshotManifest {
        database: database()?,
        position: position()?,
        schema: done.schema,
        tables: done
            .table_digests
            .into_iter()
            .map(|(table, digest)| TableManifest {
                row_count: u64::from(table.as_str() == "a_child"),
                table,
                digest,
            })
            .collect(),
    };
    let mut connection = TursoConnectOptions::new()
        .foreign_keys(false)
        .connect()
        .await?;
    RECEIVER_MIGRATOR
        .run_direct(None, &mut connection, false)
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    SnapshotInstallRules(&mut TursoScope(&mut transaction))
        .prepare(&install, &source)
        .await?;
    transaction.commit().await?;
    let mut cursor = None;
    for page in pages {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        SnapshotInstallRules(&mut TursoScope(&mut transaction))
            .stage_page(&install, page.page, cursor.as_ref(), &page.contents)
            .await?;
        cursor = page.contents.next;
        transaction.commit().await?;
    }
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let refused = SnapshotInstallRules(&mut TursoScope(&mut transaction))
        .verify(&install, &manifest)
        .await;
    assert!(
        matches!(refused,Err(SnapshotInstallError::Rules(ReceiverError::InvalidInput{field})) if field=="foreign key a_child references z_parent")
    );
    transaction.rollback().await?;
    let state = SnapshotInstallRules(&mut TursoScope(&mut connection))
        .state()
        .await?
        .ok_or("install marker")?;
    assert!(!state.verified && !state.ready);
    connection.close().await?;
    Ok(())
}
