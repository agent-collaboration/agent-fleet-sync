//! Original copy creation remains immutable after its live fence and holder change.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use fleet_sync_protocol::*;
use sqlx_core::connection::Connection;

#[tokio::test]
async fn changed_creation_identity_is_refused_and_original_retry_never_restores_old_authority()
-> TestResult {
    let mut connection = connection().await?;
    let original = CreateCopy {
        database: database()?,
        epoch: Epoch::new(1)?,
        holder: Some(writer()?),
        origin: CopyOrigin::Empty,
    };
    let next_writer = PeerId::new("019f0000-0000-7000-8000-000000000051".parse()?)?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        create_copy(&mut TursoScope(&mut transaction), &original).await?,
        CreateCopyAnswer::AlreadyCreated
    );
    let mut changed = original.clone();
    changed.holder = Some(next_writer);
    assert_eq!(
        create_copy(&mut TursoScope(&mut transaction), &changed).await?,
        CreateCopyAnswer::CreationConflict {
            existing: original.clone()
        }
    );
    changed = original.clone();
    changed.origin = CopyOrigin::AwaitingSeed {
        expected_position: Some(position(0)?),
    };
    assert_eq!(
        create_copy(&mut TursoScope(&mut transaction), &changed).await?,
        CreateCopyAnswer::CreationConflict {
            existing: original.clone()
        }
    );
    changed = original.clone();
    changed.epoch = Epoch::new(2)?;
    assert_eq!(
        create_copy(&mut TursoScope(&mut transaction), &changed).await?,
        CreateCopyAnswer::CreationConflict {
            existing: original.clone()
        }
    );
    let request = FenceRequest {
        database: database()?,
        transfer_id: TransferId::new("019f0000-0000-7000-8000-000000000052".parse()?)?,
        new_epoch: Epoch::new(2)?,
        expect: FenceExpectation {
            epoch: Epoch::new(1)?,
            acked: position(0)?,
        },
        target_schema_max: 1,
        holder: next_writer,
    };
    assert!(matches!(
        fence(&mut TursoScope(&mut transaction), &request).await?,
        FenceResult::Fenced { .. }
    ));
    assert_eq!(
        create_copy(&mut TursoScope(&mut transaction), &original).await?,
        CreateCopyAnswer::AlreadyCreated
    );
    let current = ready_status(copy_status(&mut TursoScope(&mut transaction)).await?)?;
    assert_eq!(current.fence, Epoch::new(2)?);
    assert_eq!(current.holder, Some(next_writer));
    transaction.commit().await?;
    Ok(())
}
