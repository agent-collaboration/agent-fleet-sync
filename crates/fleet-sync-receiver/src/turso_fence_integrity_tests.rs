//! Fence refusal, durable receipt identity, and immutable historical cutoffs.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use fleet_sync_protocol::*;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_turso::{Turso, TursoConnectOptions};

fn request(id: u128, epoch: u64, expected: u64, holder: PeerId) -> TestResult<FenceRequest> {
    Ok(FenceRequest {
        database: database()?,
        transfer_id: TransferId::new(uuid::Uuid::from_u128(id))?,
        new_epoch: Epoch::new(epoch)?,
        expect: FenceExpectation {
            epoch: Epoch::new(expected)?,
            acked: position(0)?,
        },
        target_schema_max: 1,
        holder,
    })
}
#[tokio::test]
async fn fence_checks_forward_motion_exact_preview_and_schema_before_mutation() -> TestResult {
    let mut connection = connection().await?;
    let target = PeerId::new(uuid::Uuid::from_u128(501))?;
    let mut cases = Vec::new();
    cases.push((
        request(501, 1, 1, target)?,
        FenceResult::FenceNotAdvanced {
            current_epoch: Epoch::new(1)?,
        },
    ));
    let mut changed = request(502, 2, 1, target)?;
    changed.expect.acked = position(1)?;
    cases.push((
        changed,
        FenceResult::PreviewStale {
            acked: position(0)?,
        },
    ));
    let mut changed = request(503, 2, 1, target)?;
    changed.target_schema_max = 0;
    let status = ready_status(copy_status(&mut TursoScope(&mut connection)).await?)?;
    cases.push((
        changed,
        FenceResult::SchemaTooNew {
            store: status.schema.clone(),
            supported: 0,
        },
    ));
    for (request, expected) in cases {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        assert_eq!(
            fence(&mut TursoScope(&mut transaction), &request).await?,
            expected
        );
        assert_eq!(
            ready_status(copy_status(&mut TursoScope(&mut transaction)).await?)?,
            status
        );
        transaction.commit().await?;
    }
    Ok(())
}

#[tokio::test]
async fn reopened_copy_keeps_old_fence_receipts_and_cutoffs_after_later_fences() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("copy.db");
    let mut connection = connection_with_options(
        TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true),
    )
    .await?;
    let second = PeerId::new(uuid::Uuid::from_u128(502))?;
    let third = PeerId::new(uuid::Uuid::from_u128(503))?;
    let first = request(511, 2, 1, second)?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let original = fence(&mut TursoScope(&mut transaction), &first).await?;
    transaction.commit().await?;
    drop(connection);
    let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let next = request(512, 3, 2, third)?;
    assert!(
        matches!(fence(&mut TursoScope(&mut transaction),&next).await?,FenceResult::Fenced{new_epoch,..} if new_epoch==Epoch::new(3)?)
    );
    assert_eq!(
        fence(&mut TursoScope(&mut transaction), &first).await?,
        original
    );
    assert_eq!(
        ready_status(copy_status(&mut TursoScope(&mut transaction)).await?)?.fence,
        Epoch::new(3)?
    );
    let mut changed = first.clone();
    changed.holder = third;
    assert_eq!(
        fence(&mut TursoScope(&mut transaction), &changed)
            .await
            .err(),
        Some(ReceiverError::FenceConflict)
    );
    let incoming = piece(1, 0)?;
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::StaleEpoch {
            newer: Epoch::new(3)?,
            ended_at: position(0)?
        }))
    );
    assert_eq!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            &incoming,
            &second,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(1)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::NotWriter {
            current: Some(Epoch::new(3)?)
        }))
    );
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let damaged =
        sqlx_core::query::query::<Turso>("UPDATE _fleet_sync_fence_receipts SET result='{}'")
            .execute(&mut *transaction)
            .await?;
    assert_eq!(damaged.rows_affected(), 2);
    assert!(matches!(
        fence(&mut TursoScope(&mut transaction), &first).await,
        Err(ReceiverError::Storage(StoreError::Corrupt { .. }))
    ));
    transaction.rollback().await?;
    Ok(())
}
