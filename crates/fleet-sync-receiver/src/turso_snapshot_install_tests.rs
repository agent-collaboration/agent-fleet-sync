#![allow(clippy::panic_in_result_fn)]
use crate::turso_rule_fixture::TursoScope;
use crate::*;
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use uuid::Uuid;

#[tokio::test]
async fn page_chain_has_no_gaps_and_snapshot_replaces_migration_created_rows() -> TestResult {
    let mut connection = staging().await?;
    let (install, status, mut first, manifest) = source()?;
    first.schema_steps.push(
        "INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001','migration-only')"
            .into(),
    );
    let mut second = first.clone();
    first.rows = first.rows.get(..1).ok_or("no first row")?.to_vec();
    first.next = Some(SnapshotCursor {
        position: install.position,
        table: first.table.clone(),
        after_key: vec![crate::turso_rule_fixture::uuid_key(2)],
    });
    second.rows = second.rows.get(1..).ok_or("no second row")?.to_vec();
    second.next = None;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    SnapshotInstallRules(&mut scope)
        .prepare(&install, &status)
        .await?;
    assert!(matches!(
        SnapshotInstallRules(&mut scope)
            .stage_page(&install, 1, first.next.as_ref(), &second)
            .await,
        Err(SnapshotInstallError::PageConflict { page: 1 })
    ));
    SnapshotInstallRules(&mut scope)
        .stage_page(&install, 0, None, &first)
        .await?;
    assert!(matches!(
        SnapshotInstallRules(&mut scope)
            .verify(&install, &manifest)
            .await,
        Err(SnapshotInstallError::Incomplete)
    ));
    SnapshotInstallRules(&mut scope)
        .stage_page(&install, 1, first.next.as_ref(), &second)
        .await?;
    SnapshotInstallRules(&mut scope)
        .verify(&install, &manifest)
        .await?;
    transaction.commit().await?;
    let SnapshotRead::Page(installed) =
        snapshot_page(&mut TursoScope(&mut connection), None).await?
    else {
        return Err("snapshot moved".into());
    };
    assert_eq!(
        installed.rows.len(),
        2,
        "historical migration DML leaked into rows"
    );
    assert!(
        installed
            .rows
            .iter()
            .all(|row| row.first() != Some(&crate::turso_rule_fixture::uuid_key(1)))
    );
    Ok(())
}

#[tokio::test]
async fn retirement_erases_install_receipts_and_marker() -> TestResult {
    let mut connection = staging().await?;
    let (install, status, page, manifest) = source()?;
    prepared(&mut connection, &install, &status, &page, &manifest).await?;
    ready(&mut connection, &install).await?;
    let result = retire_copy(
        &mut TursoScope(&mut connection),
        &RetireCopy {
            database: install.database,
            expect_fence: status.fence,
            expect_acked: install.position,
        },
        SyncTime::from_unix_millis(42),
    )
    .await?;
    assert_eq!(result, RetireCopyAnswer::Retired);
    assert!(
        SnapshotInstallRules(&mut TursoScope(&mut connection))
            .state()
            .await?
            .is_none()
    );
    assert_eq!(
        query::<Turso>("SELECT COUNT(*) AS count FROM _fleet_sync_install_pages")
            .fetch_one(&mut connection)
            .await?
            .try_get::<i64, _>("count")?,
        0
    );
    assert!(matches!(
        SnapshotInstallRules(&mut TursoScope(&mut connection))
            .read_old_fence()
            .await,
        Err(SnapshotInstallError::Retired)
    ));
    Ok(())
}

struct InstallHook {
    refuse: bool,
}
impl ReceiverHook<TursoScope<'_>> for InstallHook {
    type Protocol = NoConsumer;
    type FollowUp = String;
    fn local_migrations(&self) -> &'static [sqlx_core::migrate::Migration] {
        static LOCAL: std::sync::LazyLock<Vec<sqlx_core::migrate::Migration>> =
            std::sync::LazyLock::new(|| {
                vec![sqlx_core::migrate::Migration::new(
                    1,
                    "install hook".into(),
                    sqlx_core::migrate::MigrationType::Simple,
                    sqlx_core::sql_str::SqlSafeStr::into_sql_str(
                        "CREATE TABLE installed_marks(position TEXT PRIMARY KEY)",
                    ),
                    false,
                )]
            });
        &LOCAL
    }
    async fn on_applied(
        &self,
        _: &mut TursoScope<'_>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<String>, HookRefusal> {
        Ok(Vec::new())
    }
    async fn on_snapshot_installed(
        &self,
        scope: &mut TursoScope<'_>,
        installed: &InstalledSnapshot,
    ) -> Result<Vec<String>, HookRefusal> {
        query::<Turso>("INSERT INTO installed_marks(position) VALUES(?1)")
            .bind(installed.position.sequence.value().to_string())
            .execute(scope.executor())
            .await
            .map_err(|error| HookRefusal {
                code: crate::turso_rule_fixture::fixture_refusal_code("storage"),
                detail: error.to_string(),
            })?;
        if self.refuse {
            return Err(HookRefusal {
                code: crate::turso_rule_fixture::fixture_refusal_code("policy"),
                detail: "refused after local write".into(),
            });
        }
        Ok(vec!["snapshot published".into()])
    }
    async fn follow_up(
        &self,
        _: &String,
        _: durable_steps::CallId,
    ) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}
async fn prepared(
    connection: &mut TursoConnection,
    install: &SnapshotInstall,
    status: &CopyStatus,
    page: &SnapshotPage,
    manifest: &SnapshotManifest,
) -> TestResult {
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    initialize_hook(&mut scope, &InstallHook { refuse: false }).await?;
    let mut rules = SnapshotInstallRules(&mut scope);
    rules.prepare(install, status).await?;
    rules.stage_page(install, 0, None, page).await?;
    rules.verify(install, manifest).await?;
    transaction.commit().await?;
    Ok(())
}
async fn ready(connection: &mut TursoConnection, install: &SnapshotInstall) -> TestResult {
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(
        matches!(SnapshotInstallRules(&mut TursoScope(&mut transaction)).mark_ready(install,&InstallHook{refuse:false}).await?,SnapshotReady::Ready{follow_ups,..} if follow_ups==["snapshot published"])
    );
    transaction.commit().await?;
    Ok(())
}
#[tokio::test]
async fn pages_verify_before_readability_and_ready_hook_is_atomic_and_once() -> TestResult {
    let mut connection = staging().await?;
    let (install, status, page, manifest) = source()?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    initialize_hook(&mut scope, &InstallHook { refuse: false }).await?;
    SnapshotInstallRules(&mut scope)
        .prepare(&install, &status)
        .await?;
    assert!(matches!(
        SnapshotInstallRules(&mut scope)
            .mark_ready(&install, &InstallHook { refuse: false })
            .await,
        Err(SnapshotInstallError::NotVerified)
    ));
    let receipt = SnapshotInstallRules(&mut scope)
        .stage_page(&install, 0, None, &page)
        .await?;
    assert_eq!(receipt.rows, 2);
    assert_eq!(
        SnapshotInstallRules(&mut scope)
            .stage_page(&install, 0, None, &page)
            .await?,
        receipt
    );
    let mut conflict = page.clone();
    conflict.rows.clear();
    assert!(matches!(
        SnapshotInstallRules(&mut scope)
            .stage_page(&install, 0, None, &conflict)
            .await,
        Err(SnapshotInstallError::PageConflict { page: 0 })
    ));
    assert!(matches!(
        snapshot_page(&mut scope, None).await,
        Err(ReceiverError::NotInitialized)
    ));
    let mut wrong = manifest.clone();
    wrong
        .tables
        .first_mut()
        .ok_or("no manifest table")?
        .row_count = 99;
    assert!(matches!(
        SnapshotInstallRules(&mut scope)
            .verify(&install, &wrong)
            .await,
        Err(SnapshotInstallError::ManifestMismatch)
    ));
    assert!(
        !SnapshotInstallRules(&mut scope)
            .state()
            .await?
            .ok_or("no marker")?
            .verified
    );
    SnapshotInstallRules(&mut scope)
        .verify(&install, &manifest)
        .await?;
    assert!(
        matches!(snapshot_page(&mut scope,None).await?,SnapshotRead::Page(actual) if actual.rows==page.rows)
    );
    assert_eq!(
        copy_status(&mut scope).await?,
        CopyStatusAnswer::NotInitialized,
        "verified is not Following"
    );
    transaction.commit().await?;

    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        SnapshotInstallRules(&mut TursoScope(&mut transaction))
            .mark_ready(&install, &InstallHook { refuse: true })
            .await,
        Err(SnapshotInstallError::Hook { .. })
    ));
    transaction.rollback().await?;
    assert!(
        !SnapshotInstallRules(&mut TursoScope(&mut connection))
            .state()
            .await?
            .ok_or("no marker")?
            .ready
    );
    assert_eq!(
        query::<Turso>("SELECT COUNT(*) AS count FROM installed_marks")
            .fetch_one(&mut connection)
            .await?
            .try_get::<i64, _>("count")?,
        0,
        "failed hook local write leaked"
    );
    ready(&mut connection, &install).await?;
    assert!(
        matches!(copy_status(&mut TursoScope(&mut connection)).await?,CopyStatusAnswer::Status(status) if status.role==CopyRole::Follower && status.acked==install.position)
    );
    assert!(matches!(
        SnapshotInstallRules(&mut TursoScope(&mut connection))
            .mark_ready(&install, &InstallHook { refuse: false })
            .await?,
        SnapshotReady::AlreadyReady { .. }
    ));
    assert_eq!(
        query::<Turso>("SELECT COUNT(*) AS count FROM installed_marks")
            .fetch_one(&mut connection)
            .await?
            .try_get::<i64, _>("count")?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn target_fence_and_saved_transfer_survive_snapshot_and_older_snapshot_is_refused()
-> TestResult {
    let mut old = staging().await?;
    let (install, status, page, manifest) = source()?;
    prepared(&mut old, &install, &status, &page, &manifest).await?;
    ready(&mut old, &install).await?;
    let transfer = FenceRequest {
        database: install.database,
        transfer_id: TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000051))?,
        new_epoch: Epoch::new(2)?,
        holder: PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000009))?,
        expect: FenceExpectation {
            epoch: Epoch::new(1)?,
            acked: install.position,
        },
        target_schema: Some(fleet_sync_protocol::SchemaSupport { max_version: 10 }),
    };
    let fenced = fence(&mut TursoScope(&mut old), &transfer).await?;
    assert!(matches!(fenced, FenceResult::Fenced { .. }));
    let carrier = SnapshotInstallRules(&mut TursoScope(&mut old))
        .read_old_fence()
        .await?
        .ok_or("no old fence")?;

    let mut replacement = staging().await?;
    let mut newer_install = install.clone();
    newer_install.install_id = InstallId::new(Uuid::from_u128(0x019eef00000070008000000000000052))?;
    prepared(&mut replacement, &newer_install, &status, &page, &manifest).await?;
    SnapshotInstallRules(&mut TursoScope(&mut replacement))
        .merge_old_fence(&newer_install, Some(&carrier))
        .await?;
    ready(&mut replacement, &newer_install).await?;
    let current = crate::turso_rule_fixture::ready_status(
        copy_status(&mut TursoScope(&mut replacement)).await?,
    )?;
    assert_eq!(current.fence, Epoch::new(2)?);
    assert_eq!(current.holder, Some(transfer.holder));
    assert_eq!(current.acked, install.position);
    assert_eq!(
        fence(&mut TursoScope(&mut replacement), &transfer).await?,
        fenced,
        "fence replay lost its original request/result"
    );
    assert_eq!(
        crate::copy_storage::CopyStorage(&mut TursoScope(&mut replacement))
            .epoch_end(Epoch::new(1)?)
            .await?,
        Some((status.holder, install.position))
    );

    let mut stale = staging().await?;
    let mut stale_install = install.clone();
    stale_install.install_id = InstallId::new(Uuid::from_u128(0x019eef00000070008000000000000053))?;
    stale_install.position = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(6));
    let mut stale_status = status.clone();
    stale_status.acked = stale_install.position;
    let mut stale_page = page.clone();
    stale_page.position = stale_install.position;
    let mut stale_manifest = manifest;
    stale_manifest.position = stale_install.position;
    prepared(
        &mut stale,
        &stale_install,
        &stale_status,
        &stale_page,
        &stale_manifest,
    )
    .await?;
    assert!(
        matches!(SnapshotInstallRules(&mut TursoScope(&mut stale)).merge_old_fence(&stale_install,Some(&carrier)).await,Err(SnapshotInstallError::BelowFence{minimum,..}) if minimum==install.position)
    );
    Ok(())
}
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn staging() -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new()
        .foreign_keys(false)
        .connect()
        .await?;
    crate::RECEIVER_MIGRATOR
        .run_direct(None, &mut connection, false)
        .await?;
    Ok(connection)
}
pub(super) fn source() -> TestResult<(SnapshotInstall, CopyStatus, SnapshotPage, SnapshotManifest)>
{
    let (page, done) = crate::turso_seed_tests::snapshot(SeedId { generation: 1 })?;
    let install = SnapshotInstall {
        install_id: InstallId::new(Uuid::from_u128(0x019eef00000070008000000000000050))?,
        database: done.database,
        position: done.position,
        schema: done.schema.clone(),
    };
    let source = CopyStatus {
        database: done.database,
        role: CopyRole::Authoritative,
        fence: done.position.epoch,
        holder: Some(PeerId::new(Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?),
        start: ReplicationPosition::new(done.position.epoch, CommitSequence::new(0)),
        acked: done.position,
        schema: done.schema.clone(),
    };
    let manifest = SnapshotManifest {
        database: done.database,
        position: done.position,
        schema: done.schema,
        tables: done
            .table_digests
            .into_iter()
            .map(|(table, digest)| TableManifest {
                table,
                digest,
                row_count: 2,
            })
            .collect(),
    };
    Ok((install, source, page.contents, manifest))
}
#[tokio::test]
async fn preparation_records_starting_without_publishing_a_following_copy() -> TestResult {
    let mut connection = staging().await?;
    let (install, source, _, _) = source()?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    SnapshotInstallRules(&mut TursoScope(&mut transaction))
        .prepare(&install, &source)
        .await?;
    transaction.commit().await?;
    let state = SnapshotInstallRules(&mut TursoScope(&mut connection))
        .state()
        .await?
        .ok_or("no install marker")?;
    assert_eq!(state.install, install);
    assert!(!state.verified && !state.ready);
    assert_eq!(
        copy_status(&mut TursoScope(&mut connection)).await?,
        CopyStatusAnswer::NotInitialized
    );
    Ok(())
}
