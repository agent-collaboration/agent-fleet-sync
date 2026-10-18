#![allow(clippy::panic_in_result_fn)]
use crate::{HolderError, LeaseHolder, LeasePolicy, WriterState};
use fleet_sync_protocol::*;
use std::time::Duration;
type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
fn holder() -> Result<LeaseHolder, Box<dyn std::error::Error + Send + Sync>> {
    Ok(LeaseHolder::provisional(
        DatabaseId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000001,
        ))?,
        PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_secs(1),
        },
    ))
}
fn refusal() -> Result<GateReason, TextValueError> {
    Ok(GateReason {
        code: GateReasonCode::new("denied")?,
        detail: "fixture gate refused".into(),
    })
}
#[test]
fn pending_registration_seed_blocks_writes_and_can_be_rejected_before_a_grant() -> TestResult {
    let mut holder = holder()?;
    holder.begin_seeding()?;
    holder.begin_seeding()?;
    assert_eq!(
        holder.writer_state(SyncTime::from_unix_millis(0))?,
        WriterState::Seeding {
            epoch: Epoch::new(1)?
        }
    );
    let reason = refusal()?;
    holder.reject_registration(reason.clone())?;
    assert_eq!(
        holder.writer_state(SyncTime::from_unix_millis(0)),
        Err(HolderError::RegistrationRejected { reason })
    );
    Ok(())
}
#[test]
fn granted_seed_refuses_late_rejection_and_live_writer_cannot_enter_seeding() -> TestResult {
    let now = SyncTime::from_unix_millis(100);
    let mut holder = holder()?;
    holder.begin_seeding()?;
    holder.observe_registration(
        LeaseStatus {
            database: holder.database(),
            epoch: Epoch::new(1)?,
            holder: holder.peer(),
            state: LeaseState::Held,
            expires_at: now.after(Duration::from_secs(10)),
            remaining_ms: 10_000,
        },
        now,
    )?;
    assert_eq!(
        holder.reject_registration(refusal()?),
        Err(HolderError::AlreadyGranted)
    );
    assert_eq!(
        holder.writer_state(now)?,
        WriterState::Seeding {
            epoch: Epoch::new(1)?
        }
    );
    holder.complete_seed(Epoch::new(1)?)?;
    assert_eq!(holder.begin_seeding(), Err(HolderError::InvalidTransition));
    assert_eq!(
        holder.writer_state(now)?,
        WriterState::Writing {
            epoch: Epoch::new(1)?
        }
    );
    Ok(())
}
