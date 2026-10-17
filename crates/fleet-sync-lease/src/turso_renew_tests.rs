//! Renewal answers are checked against real persisted registry records.
#![allow(clippy::panic_in_result_fn)]
use crate::{registry_records::*, registry_storage::RegistryStorage, *};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
struct TursoScope<'a>(&'a mut TursoConnection);
impl TransactionScope for TursoScope<'_> {
    type Database = Turso;
    type Executor<'q>
        = &'q mut TursoConnection
    where
        Self: 'q;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
fn database() -> TestResult<DatabaseId> {
    Ok(DatabaseId::new(
        "019f0000-0000-7000-8000-000000000041".parse()?,
    )?)
}
fn holder(value: u128) -> TestResult<PeerId> {
    Ok(PeerId::new(uuid::Uuid::from_u128(value))?)
}
fn request(attempt: u128, epoch: u64) -> TestResult<RenewRequest> {
    Ok(RenewRequest {
        attempt: RequestId::new(uuid::Uuid::from_u128(attempt))?,
        database: database()?,
        epoch: Epoch::new(epoch)?,
    })
}
fn now(value: i64) -> SyncTime {
    SyncTime::from_unix_millis(value)
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_millis(200),
        renew_interval: Duration::from_millis(50),
    }
}
async fn setup() -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_core::raw_sql::raw_sql(concat!(
        include_str!("../migrations/0001_registry_rules.sql"),
        "\n",
        include_str!("../migrations/0002_holder_workflow.sql"),
        "\n",
        include_str!("../migrations/0003_cleanup_failure.sql")
    ))
    .execute(&mut *transaction)
    .await?;
    let mut scope = TursoScope(&mut transaction);
    let database = database()?;
    RegistryStorage(&mut scope)
        .save_database(&DatabaseRecord {
            database,
            lifecycle: DatabaseLifecycle::Registered {
                origin: Origin::Empty,
            },
        })
        .await?;
    RegistryStorage(&mut scope)
        .save_lease(&RegistryLease {
            database,
            holder: holder(1)?,
            epoch: Epoch::new(1)?,
            last_renewed_at: now(0),
            expires_at: now(200),
        })
        .await?;
    RegistryStorage(&mut scope)
        .save_floor(database, Epoch::new(1)?)
        .await?;
    RegistryStorage(&mut scope)
        .save_history(&EpochHistory {
            database,
            holder: holder(1)?,
            epoch: Epoch::new(1)?,
            granted_at: now(0),
            ended_at: None,
        })
        .await?;
    transaction.commit().await?;
    Ok(connection)
}

#[tokio::test]
async fn renewal_replay_never_extends_expiry_and_an_expired_current_holder_can_renew_the_same_epoch()
-> TestResult {
    let mut connection = setup().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let first = renew(
        &mut TursoScope(&mut transaction),
        &request(100, 1)?,
        &holder(1)?,
        now(100),
        &policy(),
    )
    .await?;
    assert!(
        matches!(first,RenewAnswer::Renewed{recorded:RenewResult{expires_at,..},replayed:false,..} if expires_at==now(300))
    );
    transaction.commit().await?;
    for elapsed in [0, 1, 50, 199, 200, 201, 1000, 86_400_000, i64::MAX - 100] {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let replay = renew(
            &mut TursoScope(&mut transaction),
            &request(100, 1)?,
            &holder(1)?,
            now(100 + elapsed),
            &policy(),
        )
        .await?;
        assert!(
            matches!(replay,RenewAnswer::Renewed{recorded:RenewResult{epoch,expires_at},current:LeaseStatus{expires_at:current,..},replayed:true,..} if epoch==Epoch::new(1)? && expires_at==now(300) && current==now(300))
        );
        transaction.commit().await?;
    }
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let fresh = renew(
        &mut TursoScope(&mut transaction),
        &request(101, 1)?,
        &holder(1)?,
        now(1000),
        &policy(),
    )
    .await?;
    assert!(
        matches!(fresh,RenewAnswer::Renewed{recorded:RenewResult{epoch,expires_at},replayed:false,..} if epoch==Epoch::new(1)? && expires_at==now(1200))
    );
    transaction.commit().await?;
    Ok(())
}
#[tokio::test]
async fn replay_preserves_recorded_answer_but_returns_current_lease_after_later_renewal_and_takeover()
-> TestResult {
    let mut connection = setup().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let first = renew(
        &mut TursoScope(&mut transaction),
        &request(10, 1)?,
        &holder(1)?,
        now(100),
        &policy(),
    )
    .await?;
    assert!(
        matches!(first,RenewAnswer::Renewed{recorded:RenewResult{expires_at,..},replayed:false,..} if expires_at==now(300))
    );
    let later = renew(
        &mut TursoScope(&mut transaction),
        &request(11, 1)?,
        &holder(1)?,
        now(200),
        &policy(),
    )
    .await?;
    assert!(
        matches!(later,RenewAnswer::Renewed{recorded:RenewResult{expires_at,..},..} if expires_at==now(400))
    );
    let replay = renew(
        &mut TursoScope(&mut transaction),
        &request(10, 1)?,
        &holder(1)?,
        now(250),
        &policy(),
    )
    .await?;
    assert!(
        matches!(replay,RenewAnswer::Renewed{recorded:RenewResult{expires_at,..},current:LeaseStatus{expires_at:current,remaining_ms:150,..},replayed:true,..} if expires_at==now(300)&&current==now(400))
    );
    let mut scope = TursoScope(&mut transaction);
    let mut old = RegistryStorage(&mut scope)
        .history(database()?, Epoch::new(1)?)
        .await?
        .ok_or("history missing")?;
    let ended = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(9));
    old.ended_at = Some(ended);
    RegistryStorage(&mut scope).save_history(&old).await?;
    RegistryStorage(&mut scope)
        .save_floor(database()?, Epoch::new(2)?)
        .await?;
    RegistryStorage(&mut scope)
        .save_lease(&RegistryLease {
            database: database()?,
            holder: holder(2)?,
            epoch: Epoch::new(2)?,
            last_renewed_at: now(260),
            expires_at: now(460),
        })
        .await?;
    let replay = renew(
        &mut scope,
        &request(10, 1)?,
        &holder(1)?,
        now(270),
        &policy(),
    )
    .await?;
    assert!(
        matches!(replay,RenewAnswer::Renewed{recorded:RenewResult{epoch,expires_at},current:LeaseStatus{epoch:current,holder:current_holder,..},replayed:true,..} if epoch==Epoch::new(1)?&&expires_at==now(300)&&current==Epoch::new(2)?&&current_holder==holder(2)?)
    );
    assert_eq!(
        renew(
            &mut scope,
            &request(12, 1)?,
            &holder(1)?,
            now(270),
            &policy()
        )
        .await?,
        RenewAnswer::Refused(LeaseRefusal::StaleEpoch {
            current_epoch: Epoch::new(2)?,
            epoch_ended_at: ended
        })
    );
    transaction.commit().await?;
    Ok(())
}
#[tokio::test]
async fn open_transfer_does_not_stop_renewal_and_retired_marker_precedes_saved_answer() -> TestResult
{
    let mut connection = setup().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    let transfer = TransferId::new(uuid::Uuid::from_u128(20))?;
    let offered = HolderChangeRequest::Move {
        attempt: transfer,
        database: database()?,
        from_epoch: Epoch::new(1)?,
        to: holder(2)?,
    };
    RegistryStorage(&mut scope)
        .save_admission(&crate::registry_records::TransferAdmission {
            operation: durable_steps::OperationId(uuid::Uuid::from_u128(21)),
            request: offered.clone(),
            requested_by: ActorRef::new("renewal-fixture")?,
            admitted_at: now(200),
            follow_up: None,
        })
        .await?;
    RegistryStorage(&mut scope)
        .save_transfer(&TransferRecord::requested(
            TransferIntent {
                request: offered,
                new_epoch: Some(Epoch::new(2)?),
            },
            now(200),
        ))
        .await?;
    assert!(
        matches!(renew(&mut scope,&request(10,1)?,&holder(1)?,now(210),&policy()).await?,RenewAnswer::Renewed{transfer_in_progress:Some(current),current:LeaseStatus{state:LeaseState::Held,..},..} if current==transfer)
    );
    assert!(matches!(
        lease_status(&mut scope, database()?, now(410)).await?,
        Some(LeaseStatus {
            state: LeaseState::Expired,
            remaining_ms: 0,
            ..
        })
    ));
    RegistryStorage(&mut scope)
        .save_database(&DatabaseRecord {
            database: database()?,
            lifecycle: DatabaseLifecycle::Retired { at: now(500) },
        })
        .await?;
    assert_eq!(
        renew(
            &mut scope,
            &request(10, 1)?,
            &holder(1)?,
            now(510),
            &policy()
        )
        .await?,
        RenewAnswer::Refused(LeaseRefusal::Retired)
    );
    transaction.commit().await?;
    Ok(())
}
#[tokio::test]
async fn changed_renewal_identity_is_an_error_and_cannot_extend_the_lease() -> TestResult {
    let mut connection = setup().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    renew(
        &mut scope,
        &request(10, 1)?,
        &holder(1)?,
        now(100),
        &policy(),
    )
    .await?;
    assert!(matches!(
        renew(
            &mut scope,
            &request(10, 2)?,
            &holder(1)?,
            now(200),
            &policy()
        )
        .await,
        Err(LeaseError::InvalidInput { .. })
    ));
    assert_eq!(
        lease_status(&mut scope, database()?, now(200))
            .await?
            .map(|lease| lease.expires_at),
        Some(now(300))
    );
    transaction.commit().await?;
    Ok(())
}
