#![allow(clippy::panic_in_result_fn)] // Assertions decide behavior; Result propagates setup failures.

use durable_steps::Uuid;
use fleet_sync_lease::{HolderError, LeaseHolder, LeasePolicy, WriterState};
use fleet_sync_protocol::{
    CommitSequence, DatabaseId, Epoch, GateReason, GateReasonCode, LeaseRefusal, LeaseState,
    LeaseStatus, PeerId, RenewAnswer, RenewResult, ReplicationPosition, SyncTime, TransferId,
};
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn sync_time(value: i64) -> SyncTime {
    SyncTime::from_unix_millis(value)
}
fn identities() -> TestResult<(DatabaseId, PeerId)> {
    Ok((
        DatabaseId::new(Uuid::from_u128(0x019eef00000070008000000000000001))?,
        PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000002))?,
    ))
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_millis(100),
        renew_interval: Duration::from_millis(20),
    }
}
fn lease(
    database: DatabaseId,
    peer: PeerId,
    epoch: u64,
    remaining_ms: u64,
) -> TestResult<LeaseStatus> {
    Ok(LeaseStatus {
        database,
        holder: peer,
        epoch: Epoch::new(epoch)?,
        state: LeaseState::Held,
        expires_at: sync_time(99_999),
        remaining_ms,
    })
}
fn answer(current: LeaseStatus, recorded_epoch: u64) -> TestResult<RenewAnswer> {
    Ok(RenewAnswer::Renewed {
        recorded: RenewResult {
            epoch: Epoch::new(recorded_epoch)?,
            expires_at: sync_time(i64::MAX),
        },
        current,
        transfer_in_progress: None,
        replayed: true,
    })
}

#[test]
fn local_deadline_is_conservative_and_replayed_grants_never_supply_authority() -> TestResult {
    let (database, peer) = identities()?;
    let mut holder = LeaseHolder::provisional(database, peer, policy());
    holder.observe_registration(lease(database, peer, 1, 100)?, sync_time(100))?;
    assert_eq!(
        holder.writer_state(sync_time(199))?,
        WriterState::Writing {
            epoch: Epoch::new(1)?
        }
    );
    assert_eq!(
        holder.writer_state(sync_time(200))?,
        WriterState::Paused {
            epoch: Epoch::new(1)?,
            expired_at: sync_time(200)
        }
    );
    holder.observe_renewal(answer(lease(database, peer, 1, 20)?, 9)?, sync_time(250))?;
    assert_eq!(
        holder.writer_state(sync_time(260))?,
        WriterState::Writing {
            epoch: Epoch::new(1)?
        }
    );
    assert!(matches!(
        holder.writer_state(sync_time(270))?,
        WriterState::Paused { .. }
    ));
    assert_eq!(holder.renewal_at(), Some(sync_time(270)));
    Ok(())
}

#[test]
fn replay_after_takeover_supersedes_and_late_old_answers_cannot_restore_writes() -> TestResult {
    let (database, peer) = identities()?;
    let mut holder = LeaseHolder::provisional(database, peer, policy());
    holder.observe_registration(lease(database, peer, 1, 100)?, sync_time(0))?;
    let new_peer = PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000003))?;
    holder.observe_renewal(
        answer(lease(database, new_peer, 2, 100)?, 1)?,
        sync_time(10),
    )?;
    assert_eq!(
        holder.writer_state(sync_time(20))?,
        WriterState::Superseded {
            current: Epoch::new(2)?
        }
    );
    holder.observe_renewal(answer(lease(database, peer, 1, 1_000)?, 1)?, sync_time(30))?;
    assert!(matches!(
        holder.writer_state(sync_time(31))?,
        WriterState::Superseded { .. }
    ));
    Ok(())
}

#[test]
fn seeding_draining_and_released_keep_the_gate_closed_across_same_epoch_renewals() -> TestResult {
    let (database, peer) = identities()?;
    let epoch = Epoch::new(2)?;
    let mut holder = LeaseHolder::seeding(database, peer, epoch, policy());
    holder.observe_renewal(answer(lease(database, peer, 2, 100)?, 2)?, sync_time(0))?;
    assert_eq!(
        holder.writer_state(sync_time(1))?,
        WriterState::Seeding { epoch }
    );
    holder.complete_seed(epoch)?;
    let transfer = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000004))?;
    holder.begin_draining(transfer)?;
    holder.observe_renewal(answer(lease(database, peer, 2, 100)?, 2)?, sync_time(10))?;
    assert_eq!(
        holder.writer_state(sync_time(20))?,
        WriterState::Draining { epoch, transfer }
    );
    holder.refuse_release(transfer)?;
    assert_eq!(
        holder.writer_state(sync_time(20))?,
        WriterState::Writing { epoch }
    );
    holder.begin_draining(transfer)?;
    let final_position = ReplicationPosition {
        epoch,
        sequence: CommitSequence::new(7),
    };
    holder.complete_release(transfer, final_position)?;
    holder.observe_renewal(answer(lease(database, peer, 2, 100)?, 2)?, sync_time(30))?;
    assert_eq!(
        holder.writer_state(sync_time(40))?,
        WriterState::Released {
            epoch,
            final_position
        }
    );
    Ok(())
}

#[test]
fn retirement_and_registration_refusal_never_look_like_writing() -> TestResult {
    let (database, peer) = identities()?;
    let mut holder = LeaseHolder::provisional(database, peer, policy());
    let reason = GateReason {
        code: GateReasonCode::new("consumer_refused")?,
        detail: "registration refused by consumer".into(),
    };
    holder.reject_registration(reason.clone())?;
    assert_eq!(
        holder.writer_state(sync_time(0)),
        Err(HolderError::RegistrationRejected { reason })
    );
    let mut retired = LeaseHolder::provisional(database, peer, policy());
    retired.observe_renewal(RenewAnswer::Refused(LeaseRefusal::Retired), sync_time(0))?;
    retired.observe_renewal(answer(lease(database, peer, 1, 100)?, 1)?, sync_time(10))?;
    assert_eq!(
        retired.writer_state(sync_time(20)),
        Err(HolderError::Refused(LeaseRefusal::Retired))
    );
    Ok(())
}

fn idle_successor() -> TestResult<(LeaseHolder, TransferId, ReplicationPosition)> {
    let (database, peer) = identities()?;
    let epoch = Epoch::new(2)?;
    let mut holder = LeaseHolder::seeding(database, peer, epoch, policy());
    holder.observe_renewal(answer(lease(database, peer, 2, 100)?, 2)?, sync_time(0))?;
    holder.complete_seed(epoch)?;
    let transfer = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000006))?;
    let inherited = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(7));
    Ok((holder, transfer, inherited))
}

#[test]
fn idle_holder_completes_release_at_its_inherited_position() -> TestResult {
    let (mut holder, transfer, inherited) = idle_successor()?;
    holder.begin_draining(transfer)?;
    holder.complete_release(transfer, inherited)?;
    assert_eq!(
        holder.writer_state(sync_time(1))?,
        WriterState::Released {
            epoch: Epoch::new(2)?,
            final_position: inherited,
        }
    );
    holder.complete_release(transfer, inherited)?;
    Ok(())
}

#[test]
fn idle_holder_restores_release_at_its_inherited_position() -> TestResult {
    let (mut holder, transfer, inherited) = idle_successor()?;
    holder.restore_release(Epoch::new(2)?, transfer, Some(inherited))?;
    assert_eq!(
        holder.writer_state(sync_time(1))?,
        WriterState::Released {
            epoch: Epoch::new(2)?,
            final_position: inherited,
        }
    );
    Ok(())
}

#[test]
fn holder_rejects_future_release_positions_without_changing_its_gate() -> TestResult {
    let (mut holder, transfer, _) = idle_successor()?;
    let future = ReplicationPosition::new(Epoch::new(3)?, CommitSequence::new(0));
    assert_eq!(
        holder.restore_release(Epoch::new(2)?, transfer, Some(future)),
        Err(HolderError::LeaseMismatch)
    );
    assert_eq!(
        holder.writer_state(sync_time(1))?,
        WriterState::Writing {
            epoch: Epoch::new(2)?
        }
    );
    holder.begin_draining(transfer)?;
    assert_eq!(
        holder.complete_release(transfer, future),
        Err(HolderError::InvalidTransition)
    );
    assert_eq!(
        holder.writer_state(sync_time(1))?,
        WriterState::Draining {
            epoch: Epoch::new(2)?,
            transfer
        }
    );
    Ok(())
}
