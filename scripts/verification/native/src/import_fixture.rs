//! Reuses native_lifecycle's import/seed route with an isolated schema-5 source.
use crate::{fixture_service::*, support::*};
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest, TestResult};
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::{Migration, ReplicatedDatabase, ReplicatedSchema, WriterOptions};
use std::{path::Path, sync::LazyLock, time::Duration};

pub struct CapabilitySchema {
    tables: &'static [TableName],
}
impl CapabilitySchema {
    pub fn new() -> TestResult<Self> {
        static TABLES: LazyLock<Result<Vec<TableName>, TextValueError>> =
            LazyLock::new(|| Ok(vec![TableName::new("capability_rows")?]));
        Ok(Self {
            tables: TABLES.as_ref().map(Vec::as_slice).map_err(Clone::clone)?,
        })
    }
}
impl ReplicatedSchema for CapabilitySchema {
    fn migrations(&self) -> &'static [Migration] {
        static MIGRATIONS: sqlx_core::migrate::Migrator = sqlx::migrate!("./fixtures/migrations");
        MIGRATIONS.iter().as_slice()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}

pub async fn publish_unheld(
    service: &ServiceProcess,
    control: &mut WebSocketConnection,
    path: &Path,
) -> TestResult<(DatabaseId, ReplicationPosition)> {
    let writer = ReplicatedDatabase::create(
        path,
        CapabilitySchema::new()?,
        new_peer()?,
        LeasePolicy {
            lease_duration: Duration::from_secs(600),
            renew_interval: Duration::from_secs(30),
        },
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let database = writer.database();
    let mut snapshot = writer.snapshot().await?;
    let manifest = snapshot.manifest().await?;
    if manifest.schema.version != 5 {
        return Err("capability fixture schema is not 5".into());
    }
    let import = Import {
        attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
        database,
        digest: ImportDigest::of_canonical(&manifest)?,
    };
    match control_request(control, ControlRequest::Import(import)).await? {
        ControlAnswer::Import(ImportAnswer::Imported) => {}
        _ => return Err("unheld import was not admitted".into()),
    }
    let seed = match control_request(
        control,
        ControlRequest::SeedBegin {
            database,
            attempt: durable_steps::Uuid::now_v7(),
        },
    )
    .await?
    {
        ControlAnswer::Replication(answer) => match *answer {
            ReplicationAnswer::SeedStarted { seed } => seed,
            _ => return Err("seed start missing".into()),
        },
        _ => return Err("seed start answer missing".into()),
    };
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let contents = snapshot.page(cursor).await?;
        let next = contents.next.clone();
        let page = SeedPage {
            seed,
            page: pages,
            digest: PageDigest::of(&contents)?,
            contents,
        };
        match control_request(control, ControlRequest::SeedPage(Box::new(page))).await? {
            ControlAnswer::Replication(answer)
                if matches!(*answer, ReplicationAnswer::SeedPage(SeedPageAnswer::Staged)) => {}
            _ => return Err("seed page not staged".into()),
        }
        pages += 1;
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    match control_request(
        control,
        ControlRequest::SeedComplete(SeedComplete {
            database,
            seed,
            position: manifest.position,
            pages,
            schema: manifest.schema,
            table_digests: manifest
                .tables
                .into_iter()
                .map(|table| (table.table, table.digest))
                .collect(),
        }),
    )
    .await?
    {
        ControlAnswer::Replication(answer) if matches!(*answer,ReplicationAnswer::SeedComplete(SeedAnswer::Published {position}) if position==manifest.position) =>
            {}
        _ => return Err("seed completion not published".into()),
    }
    match control_request(control, ControlRequest::CopyStatus { database }).await? {
        ControlAnswer::CopyStatus(CopyStatusAnswer::Status(status))
            if status.schema.version == 5 && status.holder.is_none() => {}
        _ => return Err("schema-5 copy is not ready and unheld".into()),
    }
    let mut reader = attach_peer(service, new_peer()?, 5).await?;
    let rows = copy_rows(&mut reader, database).await?;
    if !rows.contains_key("capability_rows") {
        return Err("published schema-5 table missing".into());
    }
    fleet_sync_session::TransportConnection::close(reader, CloseReason::ClientClosed).await;
    snapshot.close().await?;
    writer.close().await;
    Ok((database, manifest.position))
}

pub async fn claim(
    socket: &mut WebSocketConnection,
    control: &mut WebSocketConnection,
    claimant: PeerId,
    database: DatabaseId,
    attempt: RequestId,
) -> TestResult<HolderChangeAnswer> {
    let answer = match request(
        socket,
        OperationRequest::Lease(LeaseRequest::Claim { attempt, database }),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Claim(answer)) => answer,
        _ => return Err("claim answer missing".into()),
    };
    if matches!(answer, HolderChangeAnswer::Pending { .. }) {
        match control_request(
            control,
            ControlRequest::WaitHolderChange(HolderChangeRequest::Claim {
                attempt: TransferId::new(attempt.as_uuid())?,
                database,
                claimant,
            }),
        )
        .await?
        {
            ControlAnswer::HolderChange(answer) => Ok(answer),
            _ => Err("claim completion missing".into()),
        }
    } else {
        Ok(answer)
    }
}

/// A consumer fixture owns the holder's state; the real registry issues Release.
/// This observes registry propagation and fencing, not native writer migration.
pub async fn answer_move_from_holder(
    control: &mut WebSocketConnection,
    holder_socket: &mut WebSocketConnection,
    change: HolderChangeRequest,
    release_answer: ReleaseAnswer,
) -> TestResult<HolderChangeAnswer> {
    let (attempt, database, epoch) = match &change {
        HolderChangeRequest::Move {
            attempt,
            database,
            from_epoch,
            ..
        } => (*attempt, *database, *from_epoch),
        _ => return Err("holder fixture only answers a move".into()),
    };
    let admitted =
        match control_request(control, ControlRequest::HolderChange(change.clone())).await? {
            ControlAnswer::HolderChange(answer) => answer,
            _ => return Err("move admission answer missing".into()),
        };
    if !matches!(admitted, HolderChangeAnswer::Pending { .. }) {
        return Ok(admitted);
    }
    loop {
        match server_frame(holder_socket).await? {
            ServerFrame::Notice { .. } => {}
            ServerFrame::Request {
                frame,
                body: ServerRequest::Crate(ServerCall::Release { call, request }),
            } => {
                if request.database != database
                    || request.epoch != epoch
                    || request.transfer != attempt
                {
                    return Err("release request did not match the admitted move".into());
                }
                println!(
                    "{}",
                    serde_json::json!({"case":"fixture-holder-release","scope":"holder-owned fixture state; real registry request","call":call,"request":request,"answer":release_answer})
                );
                let response: ClientFrame<fleet_sync_test_consumer::SampleProtocol> =
                    ClientFrame::Answer {
                        frame,
                        body: ClientAnswer::Crate(ServerCallAnswer::Release(release_answer)),
                    };
                fleet_sync_session::TransportConnection::send(
                    holder_socket,
                    serde_json::to_vec(&response)?.into(),
                )
                .await?;
                break;
            }
            _ => return Err("unexpected frame at fixture holder".into()),
        }
    }
    match control_request(control, ControlRequest::WaitHolderChange(change)).await? {
        ControlAnswer::HolderChange(answer) => Ok(answer),
        _ => Err("move completion answer missing".into()),
    }
}
