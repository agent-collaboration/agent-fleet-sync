#![allow(clippy::panic_in_result_fn)]
use crate::{writer_bootstrap_fixture::*, *};
use durable_steps::{CallId, OperationId, StepName, Uuid};
use fleet_sync_lease::WriterState;
use fleet_sync_protocol::*;
use fleet_sync_writer::{BootstrapGrant, OpenError, ReplicatedDatabase, WriteRefusal};
use runtime_primitives::SystemClock;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{path::Path, sync::Arc};

fn schema() -> Schema {
    Schema {
        count: 2,
        different: false,
    }
}
async fn installed_writer(
    root: &Path,
) -> TestResult<(Arc<ReplicatedDatabase<Schema>>, BootstrapGrant)> {
    let source = source(&root.join("source.db")).await?;
    let staging = Arc::new(StagingStore::open(root.join("staging")).await?);
    let (install, _) = stage(&staging, &source).await?;
    let granted = grant(&install)?;
    let target = Arc::new(
        ReplicatedDatabase::seeding(&root.join("target.db"), schema(), granted.clone(), policy())
            .await?,
    );
    let objects = ObjectStore::open(root.join("objects")).await?;
    let control = objects
        .object(
            ObjectKey::new("control")?,
            ObjectOptions::default(),
            Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock)),
        )
        .await?;
    let coordinator = Arc::new(InstallCoordinator::open(control, staging).await?);
    let key = ObjectKey::new("idle-writer")?;
    let owned = WriterInstallTarget::new(key.clone(), target.clone());
    coordinator
        .start(
            InstallRequest {
                attempt: InstallAttemptId::new(install.install_id.as_uuid())?,
                target: key,
                database: install.database,
                source: ReceiverId::new(Uuid::from_u128(0x019eef00000070008000000000000009))?,
            },
            &owned,
        )
        .await?;
    coordinator.pin(install.clone()).await?;
    coordinator.publish(install.install_id, &owned).await?;
    coordinator
        .mark_writer_ready(install.install_id, &owned)
        .await?;
    source.close().await;
    assert_eq!(
        target.writer_state().await?,
        WriterState::Writing {
            epoch: Epoch::new(2)?
        }
    );
    let snapshot = target.snapshot().await?;
    assert_eq!(
        snapshot.position(),
        granted.start,
        "seeding must not invent an epoch-two commit"
    );
    snapshot.close().await?;
    Ok((target, granted))
}
fn request(grant: &BootstrapGrant) -> TestResult<(ReleaseRequest, CallId)> {
    Ok((
        ReleaseRequest {
            database: grant.lease.database,
            epoch: grant.lease.epoch,
            transfer: TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000010))?,
        },
        CallId {
            operation: OperationId::new(Uuid::from_u128(0x019eef00000070008000000000000011))?,
            step: StepName::new("idle-release")?,
        },
    ))
}

#[tokio::test]
async fn seeded_idle_writer_releases_inherited_position_and_restores_it_after_reopen() -> TestResult
{
    let root = tempfile::tempdir()?;
    let (target, grant) = installed_writer(root.path()).await?;
    let (request, call) = request(&grant)?;
    let expected = ReleaseAnswer::Released {
        final_position: grant.start,
    };
    assert_eq!(
        target.request_release(&request, call.clone()).await?,
        expected
    );
    assert_eq!(
        target.writer_state().await?,
        WriterState::Released {
            epoch: grant.lease.epoch,
            final_position: grant.start,
        }
    );
    assert_eq!(
        target.request_release(&request, call.clone()).await?,
        expected
    );
    target.close().await;
    let target = ReplicatedDatabase::seeding(
        &root.path().join("target.db"),
        schema(),
        grant.clone(),
        policy(),
    )
    .await?;
    assert_eq!(
        target.writer_state().await?,
        WriterState::Released {
            epoch: grant.lease.epoch,
            final_position: grant.start,
        }
    );
    assert_eq!(target.request_release(&request, call).await?, expected);
    assert!(matches!(
        target.begin_write().await,
        Err(WriteRefusal::Released)
    ));
    println!(
        "idle native release restored: epoch={}, inherited={:?}",
        grant.lease.epoch.value(),
        grant.start
    );
    target.close().await;
    Ok(())
}

#[tokio::test]
async fn seeded_release_refuses_future_epoch_and_positions_below_inherited_start() -> TestResult {
    for future in [true, false] {
        let root = tempfile::tempdir()?;
        let (target, grant) = installed_writer(root.path()).await?;
        let (request, call) = request(&grant)?;
        target.request_release(&request, call).await?;
        let final_position = if future {
            ReplicationPosition::new(Epoch::new(3)?, CommitSequence::new(0))
        } else {
            assert!(
                grant.start.sequence.value() > 0,
                "fixture needs a lower inherited position"
            );
            ReplicationPosition::new(
                grant.start.epoch,
                CommitSequence::new(grant.start.sequence.value() - 1),
            )
        };
        let path = root.path().join("target.db");
        let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
        query::<Turso>("UPDATE _fleet_sync_release SET record=json_set(record,'$.state.answer.value.final',json(?1))")
            .bind(serde_json::to_string(&final_position)?).execute(&mut connection).await?;
        connection.close().await?;
        assert!(
            matches!(target.writer_state().await, Err(OpenError::Corrupt { field, .. }) if field=="release.identity")
        );
        target.close().await;
        assert!(
            matches!(ReplicatedDatabase::seeding(&path,schema(),grant,policy()).await,
            Err(OpenError::Corrupt { field, .. }) if field=="release.identity")
        );
    }
    Ok(())
}
