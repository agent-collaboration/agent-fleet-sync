//! Real writer rollback -> actual release answer -> real session -> registry refusal.
//! The offered-count fixture follows the product's writer_pending_release_tests.
use crate::{Observations, fixture_service::*, support::*};
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest, SampleProtocol, TestResult};
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::{
    Migration, OpenError, ReplicatedDatabase, ReplicatedSchema, WriterOptions,
};
use runtime_primitives::{Clock, SystemClock};
use serde_json::json;
use sqlx_core::{migrate::Migrator, query::query, row::Row};
use sqlx_turso::Turso;
use std::{
    path::Path,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

static REGULAR: Migrator =
    sqlx::migrate!("../../../crates/fleet-sync-writer/tests/fixtures/regular");
static PENDING: Migrator = sqlx::migrate!("./fixtures/pending-migrations");
static OFFERED: LazyLock<Vec<Migration>> =
    LazyLock::new(|| REGULAR.iter().chain(PENDING.iter()).cloned().collect());
static TABLES: LazyLock<Result<Vec<TableName>, TextValueError>> = LazyLock::new(|| {
    ["text_records", "composite_records", "real_records"]
        .into_iter()
        .map(TableName::new)
        .collect()
});

struct PendingSchema {
    offered_count: Arc<AtomicUsize>,
    tables: &'static [TableName],
}
impl ReplicatedSchema for PendingSchema {
    fn migrations(&self) -> &'static [Migration] {
        OFFERED
            .get(..self.offered_count.load(Ordering::SeqCst))
            .unwrap_or_default()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}

async fn register_writer(
    writer: &ReplicatedDatabase<PendingSchema>,
    socket: &mut WebSocketConnection,
    peer: PeerId,
) -> TestResult<Epoch> {
    let registered = request(
        socket,
        OperationRequest::Lease(LeaseRequest::Register(Registration {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database: writer.database(),
            writer: peer,
            origin: Origin::Empty,
        })),
    )
    .await?;
    let lease = match registered {
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered {
            lease,
            ..
        })) => lease,
        _ => return Err("real pending writer registration failed".into()),
    };
    let started = SystemClock.now();
    let renewed = request(
        socket,
        OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database: writer.database(),
            epoch: lease.epoch,
        })),
    )
    .await?;
    match renewed {
        OperationAnswer::Lease(LeaseAnswer::Renew(answer)) => {
            writer.observe_renewal(answer, started).await?
        }
        _ => return Err("real pending writer renewal failed".into()),
    }
    Ok(lease.epoch)
}

async fn answer_real_release(
    writer: &ReplicatedDatabase<PendingSchema>,
    socket: &mut WebSocketConnection,
    transfer: TransferId,
    epoch: Epoch,
) -> TestResult<ReleaseAnswer> {
    loop {
        match server_frame(socket).await? {
            ServerFrame::Notice { .. } => {}
            ServerFrame::Request {
                frame,
                body: ServerRequest::Crate(ServerCall::Release { call, request }),
            } => {
                if request.database != writer.database()
                    || request.transfer != transfer
                    || request.epoch != epoch
                {
                    return Err("real release did not correlate to the requested move".into());
                }
                let answer = writer.request_release(&request, call.clone()).await?;
                println!(
                    "{}",
                    json!({"case":"real-writer-release","call":call,"request":request,"answer":answer})
                );
                let response: ClientFrame<SampleProtocol> = ClientFrame::Answer {
                    frame,
                    body: ClientAnswer::Crate(ServerCallAnswer::Release(answer.clone())),
                };
                socket.send(serde_json::to_vec(&response)?.into()).await?;
                return Ok(answer);
            }
            _ => return Err("unexpected frame at real pending writer".into()),
        }
    }
}

pub(super) async fn observe(
    service: &ServiceProcess,
    control: &mut WebSocketConnection,
    root: &Path,
    observations: &mut Observations,
) -> TestResult {
    let peer = new_peer().await?;
    let target = new_peer().await?;
    let offered_count = Arc::new(AtomicUsize::new(2));
    let writer = ReplicatedDatabase::create(
        &root.join("real-pending-writer.db"),
        PendingSchema {
            offered_count: offered_count.clone(),
            tables: TABLES.as_ref().map(Vec::as_slice).map_err(Clone::clone)?,
        },
        peer,
        LeasePolicy {
            lease_duration: Duration::from_secs(600),
            renew_interval: Duration::from_secs(30),
        },
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let mut socket = attach_peer(service, peer, 7).await?;
    let target_socket = attach_peer(service, target, 7).await?;
    let result: TestResult = async {
        let epoch = register_writer(&writer, &mut socket, peer).await?;
        let snapshot = writer.snapshot().await?;
        let before = snapshot.position();
        snapshot.close().await?;
        offered_count.store(3, Ordering::SeqCst);
        let migration_result = writer.migrate().await;
        if !matches!(migration_result, Err(OpenError::Storage { .. })) {
            return Err(format!("pending fixture did not reach the expected real storage rollback: {migration_result:?}").into());
        }
        let rows = query::<Turso>("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(writer.reader())
            .await?;
        let versions = rows
            .into_iter()
            .map(|row| row.try_get::<i64, _>("version"))
            .collect::<Result<Vec<_>, _>>()?;
        let row = query::<Turso>("SELECT label FROM text_records")
            .fetch_one(writer.reader())
            .await?;
        let label: String = row.try_get("label")?;
        let snapshot = writer.snapshot().await?;
        let after = snapshot.position();
        snapshot.close().await?;
        observations.check(
            "real-pending-migration-rolled-back",
            versions == [1, 2] && label == "backfilled" && after == before,
            json!({"appliedVersions":[1,2],"label":"backfilled","position":before}),
            json!({"appliedVersions":versions,"label":label,"position":after,"migrationError":format!("{migration_result:?}")}),
        );
        let transfer = TransferId::new(durable_steps::Uuid::now_v7())?;
        let change = HolderChangeRequest::Move {
            attempt: transfer,
            database: writer.database(),
            from_epoch: epoch,
            to: target,
        };
        match control_request(control, ControlRequest::HolderChange(change.clone())).await? {
            ControlAnswer::HolderChange(HolderChangeAnswer::Pending { transfer: actual }) if actual == transfer => {},
            _ => return Err("real pending move did not reach the holder release".into()),
        }
        let release = answer_real_release(&writer, &mut socket, transfer, epoch).await?;
        observations.check(
            "real-writer-pending-answer",
            release == ReleaseAnswer::ReleaseRefused(ReleaseRefusal::MigrationPending {
                migration: 7,
                state: MigrationState::Pending,
            }),
            json!({"migration":7,"state":"pending"}),
            serde_json::to_value(release)?,
        );
        let answer = match control_request(control, ControlRequest::WaitHolderChange(change.clone())).await? {
            ControlAnswer::HolderChange(answer) => answer,
            _ => return Err("real pending move completion missing".into()),
        };
        let expected = HolderChangeAnswer::Refused(HolderChangeRefusal::MigrationPending {
            migration: 7,
            state: MigrationState::Pending,
        });
        observations.check(
            "real-pending-move-detail",
            answer == expected,
            serde_json::to_value(&expected)?,
            serde_json::to_value(&answer)?,
        );
        offered_count.store(2, Ordering::SeqCst);
        let repeated = match control_request(control, ControlRequest::HolderChange(change)).await? {
            ControlAnswer::HolderChange(answer) => answer,
            _ => return Err("real pending move replay missing".into()),
        };
        observations.check(
            "real-pending-move-frozen-replay",
            repeated == expected && repeated == answer,
            serde_json::to_value(expected)?,
            serde_json::to_value(repeated)?,
        );
        Ok(())
    }
    .await;
    socket.close(CloseReason::ClientClosed).await;
    target_socket.close(CloseReason::ClientClosed).await;
    writer.close().await;
    result
}
