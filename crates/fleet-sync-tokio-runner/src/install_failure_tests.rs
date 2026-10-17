#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{
    AtomicStore, CallFailure, CallTarget, Engine, OperationState, StoreError, Uuid,
    storage::TransactionScope,
};
use fleet_sync_protocol::*;
use runtime_primitives::SystemClock;
use sqlx_core::{executor::Executor, query::query, row::Row};
use std::sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use tokio::sync::Notify;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type RaceHost = TokioHost<RaceRouter>;
struct InstallHook {
    code: HookRefusalCode,
}
impl<H: durable_steps::Host>
    fleet_sync_receiver::ReceiverHook<durable_steps::turso_store::TursoScope<'_, H>>
    for InstallHook
{
    type Protocol = NoConsumer;
    type FollowUp = String;
    async fn on_applied(
        &self,
        _: &mut durable_steps::turso_store::TursoScope<'_, H>,
        _: &fleet_sync_receiver::AppliedPiece<'_, ()>,
    ) -> Result<Vec<String>, HookRefusal> {
        Ok(Vec::new())
    }
    async fn on_snapshot_installed(
        &self,
        scope: &mut durable_steps::turso_store::TursoScope<'_, H>,
        installed: &fleet_sync_receiver::InstalledSnapshot,
    ) -> Result<Vec<String>, HookRefusal> {
        query::<sqlx_turso::Turso>("INSERT INTO hook_hits(position) VALUES(?)")
            .bind(installed.position.sequence.value().to_string())
            .execute(scope.executor())
            .await
            .map_err(|_| HookRefusal {
                code: self.code.clone(),
                detail: "hook write failed".into(),
            })?;
        Ok(Vec::new())
    }
    async fn follow_up(
        &self,
        _: &String,
        _: durable_steps::CallId,
    ) -> Result<fleet_sync_receiver::FollowUpDone, fleet_sync_receiver::FollowUpFailure> {
        Ok(fleet_sync_receiver::FollowUpDone)
    }
}
struct RaceRouter {
    coordinator: OnceLock<Weak<InstallCoordinator<RaceHost>>>,
    target: OnceLock<Weak<TokioObject<RaceHost>>>,
    source: CopyStatus,
    page: SnapshotPage,
    manifest: SnapshotManifest,
    mode: AtomicU8,
    moved: AtomicBool,
    page_started: Notify,
    release_failure: Notify,
}
impl CallRouter for RaceRouter {
    async fn dispatch(&self, call: CallEnvelope) -> Result<serde_json::Value, CallFailure> {
        if call.kind == InstallSource::KIND {
            let mut source = self.source.clone();
            if self.moved.load(Ordering::SeqCst) {
                source.acked.sequence = CommitSequence::new(30);
            }
            return serde_json::to_value(Ok::<_, InstallRefusal>(source))
                .map_err(|_| CallFailure::AnswerLost);
        }
        let file: InstallFile =
            serde_json::from_value(call.target).map_err(|_| CallFailure::AnswerLost)?;
        let request: InstallFileRequest =
            serde_json::from_value(call.request).map_err(|_| CallFailure::AnswerLost)?;
        let owner = self
            .coordinator
            .get()
            .and_then(Weak::upgrade)
            .ok_or(CallFailure::AnswerLost)?;
        let answer = match request {
            InstallFileRequest::Page { .. } if self.mode.load(Ordering::SeqCst) == 0 => {
                self.page_started.notify_one();
                self.release_failure.notified().await;
                Err(InstallRefusal {
                    code: "page_conflict".into(),
                    detail: "permanent source refusal after supersession".into(),
                })
            }
            request => {
                let result = match request {
                    InstallFileRequest::Prepare { source } => {
                        owner.prepare(file.install_id, &source).await
                    }
                    InstallFileRequest::Page { page, cursor } => {
                        let mut contents = self.page.clone();
                        let moved = self.mode.load(Ordering::SeqCst) == 3
                            && !self.moved.swap(true, Ordering::SeqCst);
                        if self.moved.load(Ordering::SeqCst) {
                            contents.position.sequence = CommitSequence::new(30);
                        }
                        owner
                            .fetch_page(
                                file.install_id,
                                page,
                                cursor.as_ref(),
                                move || async move {
                                    Ok(if moved {
                                        SnapshotRead::Moved {
                                            position: contents.position,
                                        }
                                    } else {
                                        SnapshotRead::Page(Box::new(contents))
                                    })
                                },
                            )
                            .await
                    }
                    InstallFileRequest::Verify => {
                        let mut manifest = self.manifest.clone();
                        if self.moved.load(Ordering::SeqCst) {
                            manifest.position.sequence = CommitSequence::new(30);
                        }
                        if self.mode.load(Ordering::SeqCst) == 2
                            && let Some(table) = manifest.tables.first_mut()
                        {
                            table.row_count += 1;
                        }
                        owner
                            .verify_from(file.install_id, move || async move {
                                Ok(SnapshotManifestAnswer::Manifest(manifest))
                            })
                            .await
                    }
                    InstallFileRequest::Publish => {
                        let target = self
                            .target
                            .get()
                            .and_then(Weak::upgrade)
                            .ok_or(CallFailure::AnswerLost)?;
                        owner
                            .publish(file.install_id, &target)
                            .await
                            .map(InstallFileResult::Outcome)
                    }
                    InstallFileRequest::Ready => {
                        let target = self
                            .target
                            .get()
                            .and_then(Weak::upgrade)
                            .ok_or(CallFailure::AnswerLost)?;
                        let hook = InstallHook {
                            code: HookRefusalCode::new("hook_sql")
                                .map_err(|_| CallFailure::AnswerLost)?,
                        };
                        owner
                            .mark_ready_with(file.install_id, &target, move |scope, snapshot| {
                                Box::pin(async move {
                                    fleet_sync_receiver::SnapshotInstallRules(scope)
                                        .mark_ready(&snapshot, &hook)
                                        .await?;
                                    Ok(())
                                })
                            })
                            .await
                            .map(InstallFileResult::Outcome)
                    }
                };
                result.map_err(|error| InstallRefusal {
                    code: "snapshot_file_refused".into(),
                    detail: error.to_string(),
                })
            }
        };
        serde_json::to_value(answer).map_err(|_| CallFailure::AnswerLost)
    }
}
struct Fixture {
    objects: ObjectStore<RaceHost>,
    target: Arc<TokioObject<RaceHost>>,
    coordinator: Arc<InstallCoordinator<RaceHost>>,
    engine: Engine<TokioObject<RaceHost>, RaceHost>,
    router: Arc<RaceRouter>,
}
impl Fixture {
    async fn open(root: &std::path::Path) -> TestResult<Self> {
        let (_, source, page, manifest) = crate::staging_store_tests::snapshot()?;
        let router = Arc::new(RaceRouter {
            coordinator: OnceLock::new(),
            target: OnceLock::new(),
            source,
            page,
            manifest,
            mode: AtomicU8::new(0),
            moved: AtomicBool::new(false),
            page_started: Notify::new(),
            release_failure: Notify::new(),
        });
        let host = Arc::new(TokioHost::new(router.clone(), SystemClock));
        let objects = ObjectStore::open(root.join("objects")).await?;
        let target = objects
            .object(
                ObjectKey::new("copy")?,
                ObjectOptions {
                    foreign_keys: false,
                },
                host.clone(),
            )
            .await?;
        target
            .transaction(|scope| {
                Box::pin(async move {
                    let mut migrations = sqlx::migrate!("../fleet-sync-receiver/migrations");
                    migrations.dangerous_set_table_name("_fleet_sync_copy_migrations");
                    migrations
                        .run_direct(None, scope.executor(), false)
                        .await
                        .map_err(StoreError::backend)?;
                    scope
                        .executor()
                        .execute("CREATE TABLE IF NOT EXISTS hook_hits(position TEXT NOT NULL)")
                        .await?;
                    Ok(())
                })
            })
            .await?;
        let control = objects
            .object(
                ObjectKey::new("control")?,
                ObjectOptions::default(),
                host.clone(),
            )
            .await?;
        let coordinator = Arc::new(
            InstallCoordinator::open(
                control.clone(),
                Arc::new(StagingStore::open(root.join("staging")).await?),
            )
            .await?,
        );
        router
            .coordinator
            .set(Arc::downgrade(&coordinator))
            .map_err(|_| "router already initialized")?;
        router
            .target
            .set(Arc::downgrade(&target))
            .map_err(|_| "target already initialized")?;
        let mut engine = Engine::new(control, host.clone());
        engine.register(InstallSnapshot::new(host))?;
        Ok(Self {
            objects,
            target,
            coordinator,
            engine,
            router,
        })
    }
    fn request(&self) -> TestResult<InstallRequest> {
        Ok(InstallRequest {
            attempt: Uuid::now_v7(),
            target: self.target.key().clone(),
            database: self.router.source.database,
            source: ReceiverId::new(Uuid::from_u128(10))?,
        })
    }
}

#[tokio::test]
async fn permanent_page_failure_cannot_replace_superseded_outcome_even_when_engine_fails_across_restart()
-> TestResult {
    let root = tempfile::tempdir()?;
    let mut previous = None;
    for _ in 0..2 {
        let fixture = Fixture::open(root.path()).await?;
        if let Some((install, operation, superseding)) = previous {
            assert!(
                matches!(fixture.coordinator.record(install).await?.status, InstallStatus::Superseded { by, .. } if by == superseding)
            );
            assert!(matches!(
                fixture.engine.status(&operation).await?.state,
                OperationState::Failed { .. }
            ));
        }
        let request = fixture.request()?;
        let install = request.attempt;
        let handle = fixture.coordinator.start(request, &fixture.target).await?;
        let operation = handle.operation;
        let replacement = fixture.request()?;
        let superseding = replacement.attempt;
        let (drive, admission) = tokio::join!(fixture.engine.drive_due(), async {
            fixture.router.page_started.notified().await;
            let admitted = fixture
                .coordinator
                .start(replacement, &fixture.target)
                .await;
            fixture.router.release_failure.notify_one();
            admitted
        });
        admission?;
        let report = drive?;
        assert_eq!(report.failed.len(), 1);
        assert!(matches!(
            fixture.engine.status(&operation).await?.state,
            OperationState::Failed { .. }
        ));
        assert!(
            matches!(handle.status().await?.status, InstallStatus::Superseded { by, .. } if by == superseding)
        );
        assert!(
            !fixture
                .coordinator
                .needs_snapshot(fixture.target.key())
                .await?,
            "supersession is not an install failure"
        );
        previous = Some((install, operation, superseding));
        fixture.objects.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn manifest_failure_leaves_live_bytes_unchanged_and_needs_snapshot_until_a_new_install_is_ready()
-> TestResult {
    let root = tempfile::tempdir()?;
    let fixture = Fixture::open(root.path()).await?;
    fixture.router.mode.store(1, Ordering::SeqCst);
    let initial = fixture
        .coordinator
        .start(fixture.request()?, &fixture.target)
        .await?;
    assert_eq!(
        fixture.engine.drive_due().await?.completed,
        vec![initial.operation]
    );
    assert!(matches!(
        initial.status().await?.status,
        InstallStatus::Ready { .. }
    ));
    let live_before = file_images(fixture.target.path()).await?;
    let failed_request = fixture.request()?;
    let failed_id = failed_request.attempt;
    fixture.router.mode.store(2, Ordering::SeqCst);
    let failed = fixture
        .coordinator
        .start(failed_request, &fixture.target)
        .await?;
    assert_eq!(fixture.engine.drive_due().await?.failed.len(), 1);
    assert!(matches!(
        failed.status().await?.status,
        InstallStatus::Failed { .. }
    ));
    assert!(
        fixture
            .coordinator
            .needs_snapshot(fixture.target.key())
            .await?
    );
    assert_eq!(file_images(fixture.target.path()).await?, live_before);
    fixture.objects.close().await?;
    drop(fixture);

    let restarted = Fixture::open(root.path()).await?;
    assert!(matches!(
        restarted.coordinator.record(failed_id).await?.status,
        InstallStatus::Failed { .. }
    ));
    assert!(
        restarted
            .coordinator
            .needs_snapshot(restarted.target.key())
            .await?
    );
    let next = restarted
        .coordinator
        .start(restarted.request()?, &restarted.target)
        .await?;
    assert!(
        restarted
            .coordinator
            .needs_snapshot(restarted.target.key())
            .await?,
        "admission alone cannot clear failure"
    );
    restarted.router.mode.store(1, Ordering::SeqCst);
    assert_eq!(
        restarted.engine.drive_due().await?.completed,
        vec![next.operation]
    );
    let ready = next.status().await?;
    assert!(matches!(ready.status, InstallStatus::Ready { .. }));
    assert!(
        !restarted
            .coordinator
            .needs_snapshot(restarted.target.key())
            .await?
    );
    let replay = restarted
        .coordinator
        .mark_ready_with(ready.install_id, &restarted.target, |_, _| {
            Box::pin(async { Err(StoreError::backend("ready replay must not run hook").into()) })
        })
        .await?;
    assert!(matches!(replay, InstallFileAnswer::Ready { .. }));
    let hook_hits: i64 = restarted
        .target
        .transaction(|scope| {
            Box::pin(async move {
                Ok(
                    query::<sqlx_turso::Turso>("SELECT COUNT(*) AS count FROM hook_hits")
                        .fetch_one(scope.executor())
                        .await?
                        .try_get("count")?,
                )
            })
        })
        .await?;
    assert_eq!(
        hook_hits, 2,
        "one hook run for each ready install, none on failure or replay"
    );
    restarted.objects.close().await?;
    Ok(())
}

async fn file_images(path: &std::path::Path) -> TestResult<(Vec<u8>, Vec<u8>)> {
    let wal = std::path::PathBuf::from(format!("{}-wal", path.display()));
    let wal = match tokio::fs::read(wal).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    Ok((tokio::fs::read(path).await?, wal))
}

#[tokio::test]
async fn moved_source_restarts_with_a_new_generation_and_installs_its_new_pinned_position()
-> TestResult {
    let root = tempfile::tempdir()?;
    let fixture = Fixture::open(root.path()).await?;
    fixture.router.mode.store(3, Ordering::SeqCst);
    let request = fixture.request()?;
    let old_id = request.attempt;
    let handle = fixture.coordinator.start(request, &fixture.target).await?;
    let report = fixture.engine.drive_due().await?;
    assert_eq!(report.completed, vec![handle.operation]);
    let installed = handle.status().await?;
    assert_eq!(installed.generation, 2);
    assert_ne!(installed.install_id, old_id);
    assert!(
        matches!(installed.status, InstallStatus::Ready { snapshot } if snapshot.position.sequence == CommitSequence::new(30))
    );
    assert!(
        matches!(fixture.coordinator.record(old_id).await?.status, InstallStatus::Superseded { by, .. } if by == installed.install_id)
    );
    let status = fixture
        .target
        .transaction(|scope| {
            Box::pin(async move {
                fleet_sync_receiver::copy_status(scope)
                    .await
                    .map_err(StoreError::backend)
            })
        })
        .await?;
    assert!(
        matches!(status, CopyStatusAnswer::Status(status) if status.acked.sequence == CommitSequence::new(30))
    );
    fixture.objects.close().await?;
    Ok(())
}
