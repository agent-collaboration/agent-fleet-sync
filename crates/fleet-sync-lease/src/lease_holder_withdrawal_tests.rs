#![allow(clippy::panic_in_result_fn)]
use crate::{LeaseHolder, LeasePolicy, WriterState};
use fleet_sync_protocol::*;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
fn transfer(number: u128) -> Result<TransferId, IdentityError> {
    TransferId::new(durable_steps::Uuid::from_u128(number))
}
fn renew(holder: &mut LeaseHolder, started: i64, remaining_ms: u64) -> TestResult {
    let now = SyncTime::from_unix_millis(started);
    holder.observe_renewal(
        RenewAnswer::Renewed {
            recorded: RenewResult {
                epoch: Epoch::new(1)?,
                expires_at: now.after(Duration::from_millis(remaining_ms)),
            },
            current: LeaseStatus {
                database: holder.database(),
                epoch: Epoch::new(1)?,
                holder: holder.peer(),
                state: LeaseState::Held,
                expires_at: now.after(Duration::from_millis(remaining_ms)),
                remaining_ms,
            },
            transfer_in_progress: None,
            replayed: false,
        },
        now,
    )?;
    Ok(())
}
fn holder() -> Result<LeaseHolder, Box<dyn std::error::Error + Send + Sync>> {
    let mut holder = LeaseHolder::provisional(
        DatabaseId::new(durable_steps::Uuid::from_u128(1))?,
        PeerId::new(durable_steps::Uuid::from_u128(2))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_secs(1),
        },
    );
    renew(&mut holder, 100, 1000)?;
    Ok(holder)
}
fn position() -> Result<ReplicationPosition, Box<dyn std::error::Error + Send + Sync>> {
    Ok(ReplicationPosition {
        epoch: Epoch::new(1)?,
        sequence: CommitSequence::new(7),
    })
}

#[test]
fn released_holder_keeps_renewing_and_withdraws_at_its_same_epoch() -> TestResult {
    let mut holder = holder()?;
    let transfer = transfer(3)?;
    holder.begin_draining(transfer)?;
    holder.complete_release(transfer, position()?)?;
    renew(&mut holder, 500, 2000)?;
    assert!(matches!(
        holder.writer_state(SyncTime::from_unix_millis(600))?,
        WriterState::Released { .. }
    ));
    assert_eq!(holder.renewal_at(), Some(SyncTime::from_unix_millis(1500)));
    assert_eq!(
        holder.withdraw_release(Epoch::new(1)?, transfer),
        WithdrawAnswer::Withdrawn
    );
    assert_eq!(
        holder.writer_state(SyncTime::from_unix_millis(2499))?,
        WriterState::Writing {
            epoch: Epoch::new(1)?
        }
    );
    assert_eq!(
        holder.writer_state(SyncTime::from_unix_millis(2500))?,
        WriterState::Paused {
            epoch: Epoch::new(1)?,
            expired_at: SyncTime::from_unix_millis(2500)
        }
    );
    Ok(())
}

#[test]
fn restored_release_withdraws_without_extending_the_original_deadline() -> TestResult {
    let mut holder = holder()?;
    let transfer = transfer(3)?;
    holder.restore_release(Epoch::new(1)?, transfer, Some(position()?))?;
    assert_eq!(
        holder.withdraw_release(Epoch::new(1)?, transfer),
        WithdrawAnswer::Withdrawn
    );
    assert_eq!(
        holder.writer_state(SyncTime::from_unix_millis(1100))?,
        WriterState::Paused {
            epoch: Epoch::new(1)?,
            expired_at: SyncTime::from_unix_millis(1100)
        }
    );
    Ok(())
}

#[test]
fn withdrawal_cannot_erase_another_drain_or_supersession() -> TestResult {
    let mut holder = holder()?;
    let active = transfer(3)?;
    holder.begin_draining(active)?;
    assert_eq!(
        holder.withdraw_release(Epoch::new(1)?, transfer(4)?),
        WithdrawAnswer::Withdrawn
    );
    assert_eq!(
        holder.writer_state(SyncTime::from_unix_millis(100))?,
        WriterState::Draining {
            epoch: Epoch::new(1)?,
            transfer: active
        }
    );
    holder.observe_supersession(Epoch::new(2)?);
    holder.restore_release(Epoch::new(1)?, active, Some(position()?))?;
    assert_eq!(
        holder.withdraw_release(Epoch::new(1)?, active),
        WithdrawAnswer::NotHolder
    );
    assert_eq!(
        holder.writer_state(SyncTime::from_unix_millis(100))?,
        WriterState::Superseded {
            current: Epoch::new(2)?
        }
    );
    Ok(())
}
