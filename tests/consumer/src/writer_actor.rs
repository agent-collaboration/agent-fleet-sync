use crate::{
    ConsumerStore, SampleProtocol, SampleSchema, TestResult, checked_queries,
    consumer_store::ConsumerScope,
    writer_control::{WriterCommand, WriterControl, WriterReply},
};
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use fleet_sync_tokio_runner::{NativeWriterRuntime, SessionPump};
use fleet_sync_transport_websocket::WebSocketTransport;
use fleet_sync_writer::*;
use runtime_primitives::{Backoff, SystemClock, UuidV7Ids};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    migrate::Migrator,
};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::{mpsc, watch},
    task::JoinSet,
};

pub(crate) struct Annotation;
pub(crate) type WriterSender = ReplicationSender<
    SampleSchema,
    Annotation,
    (),
    fleet_sync_tokio_runner::WriterSessionCalls<SampleSchema, SampleProtocol>,
>;
impl PieceAnnotation for Annotation {
    type Protocol = SampleProtocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<String> {
        None
    }
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(600),
        renew_interval: Duration::from_secs(1),
    }
}
async fn preexisting(path: &Path) -> TestResult {
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    Migrator::with_migrations(SampleSchema::new()?.migrations().to_vec())
        .run_direct(None, &mut *transaction, false)
        .await?;
    let mut scope = ConsumerScope(&mut transaction);
    for index in 0..300 {
        checked_queries::insert_sample(
            &mut scope,
            format!("019eef00-0000-7000-8000-{index:012x}"),
            format!("adopted-{index}"),
            9_007_199_254_740_993,
            vec![0, 255, 128],
        )
        .await?;
    }
    checked_queries::insert_reading(
        &mut scope,
        "019eef00-0000-7000-8001-000000000001".into(),
        3.25,
    )
    .await?;
    transaction.commit().await?;
    connection.close().await?;
    Ok(())
}
pub async fn run_writer(
    directory: &Path,
    endpoint: &str,
    mode: &str,
    database_id: Option<DatabaseId>,
) -> TestResult {
    tokio::fs::create_dir_all(directory).await?;
    let path = directory.join("writer.db");
    let bootstrap = matches!(mode, "bootstrap" | "resume-bootstrap" | "rename-bootstrap");
    let peer = PeerId::new(durable_steps::Uuid::from_u128(if bootstrap {
        4
    } else {
        2
    }))?;
    let store = ConsumerStore::open(&directory.join("client.db")).await?;
    let client = SessionClient::<_, _, SampleProtocol>::load(
        WebSocketTransport::default(),
        store.clone(),
        &UuidV7Ids,
        SchemaSupport { max_version: 100 },
        Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_secs(1),
            factor: 2,
        },
    )
    .await?;
    let session = client.state().session_id;
    let (requests, mut incoming, pump) = SessionPump::new(
        client,
        Endpoint::new(endpoint)?,
        PeerCredential::new(if bootstrap {
            "Bearer peer-4"
        } else {
            "Bearer peer-2"
        })?,
        Duration::from_secs(10),
    );
    let (stop, stopped) = watch::channel(false);
    let mut tasks = JoinSet::<TestResult>::new();
    let pumping = stopped.clone();
    tasks.spawn(async move {
        pump.run(pumping).await?;
        Ok(())
    });
    let options = WriterOptions {
        allow_provisional_writes: true,
    };
    let database = Arc::new(match mode {
        "create" => {
            ReplicatedDatabase::create(&path, SampleSchema::new()?, peer, policy(), options).await?
        }
        "adopt" => {
            preexisting(&path).await?;
            ReplicatedDatabase::adopt(&path, SampleSchema::new()?, peer, policy(), options).await?
        }
        "resume" => {
            let id = database_id.ok_or("resume needs database identity")?;
            ReplicatedDatabase::open_with(
                &path,
                id,
                SampleSchema::new()?,
                LeaseHolder::provisional(id, peer, policy()),
                Arc::new(SystemClock),
                options,
            )
            .await?
        }
        "rename-source" => {
            ReplicatedDatabase::create(
                &path,
                SampleSchema::rename_prefix()?,
                peer,
                policy(),
                options,
            )
            .await?
        }
        "bootstrap" | "resume-bootstrap" | "rename-bootstrap" => {
            crate::writer_bootstrap::open(
                &path,
                database_id.ok_or("bootstrap needs database identity")?,
                &requests,
                policy(),
                if mode == "rename-bootstrap" {
                    SampleSchema::rename_bundle()?
                } else {
                    SampleSchema::extended()?
                },
            )
            .await?
        }
        _ => return Err("unknown writer mode".into()),
    });
    if matches!(mode, "create" | "adopt") {
        let mut write = database.begin_write().await?;
        crate::insert_sample(
            &mut write,
            durable_steps::Uuid::now_v7().to_string(),
            "provisional".into(),
            9_007_199_254_740_993,
            vec![0, 255, 128],
        )
        .await?;
        write.commit().await?;
    }
    let receiver = ReceiverId::new(durable_steps::Uuid::from_u128(3))?;
    let mut runtime =
        NativeWriterRuntime::new(database.clone(), receiver, requests.clone()).await?;
    if bootstrap {
        runtime
            .enable_snapshot_installs(&directory.join("installs"))
            .await?;
    }
    let runtime = Arc::new(runtime);
    let sender = Arc::new(ReplicationSender::new(
        database.clone(),
        Arc::new(Annotation),
        Arc::new(()),
        runtime.transport(),
        SenderOptions::default(),
    ));
    let (events, mut event_stream) = mpsc::channel(128);
    let (acked, acked_state) = watch::channel(None);
    let control = Arc::new(WriterControl {
        database: database.clone(),
        runtime: runtime.clone(),
        acked: acked_state,
        sender: sender.clone(),
        receiver,
    });
    let running = stopped.clone();
    let work = runtime.clone();
    tasks.spawn(async move {
        work.run(running).await?;
        Ok(())
    });
    let sending = stopped.clone();
    tasks.spawn(async move {
        sender.run(sending, events).await?;
        Ok(())
    });
    let (answers, mut answered) = mpsc::channel(16);
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    println!(
        "WRITER_READY {} {} {}",
        std::process::id(),
        database.database().as_uuid(),
        session.as_uuid()
    );
    let result:TestResult=async {loop {
        tokio::select! {
            line=lines.next_line()=>match line? {
                Some(line)=>{
                    let command:WriterCommand=serde_json::from_str(&line)?;
                    if matches!(command,WriterCommand::Shutdown){println!("WRITER_REPLY {}",serde_json::to_string(&WriterReply::Ok)?);break;}
                    let control=control.clone();let answers=answers.clone();let mut shutdown=stopped.clone();
                    tasks.spawn(async move {
                        let answer=tokio::select! {answer=control.dispatch(command)=>answer,_=shutdown.changed()=>WriterReply::Error {message:"writer shutting down".into()}};
                        let _=answers.send(answer).await;Ok(())
                    });
                }
                None=>break,
            },
            Some(answer)=answered.recv()=>println!("WRITER_REPLY {}",serde_json::to_string(&answer)?),
            event=event_stream.recv()=>if let Some(Ok(ReplicationEvents::Acknowledged {through,..}))=event {
                let current=*acked.borrow();acked.send_replace(Some(current.map_or(through,|old:ReplicationPosition|old.max(through))));
            },
            event=incoming.recv()=>match event {
                Some(ClientEvent::Notice {position,body})=>{
                    if matches!(body,Notice::LeaseChanged {database:changed} if changed==database.database()){runtime.prompt_renewal().await?;}
                    requests.acknowledge_notice(position).await?;
                }
                Some(ClientEvent::Request {frame,body:ServerRequest::Crate(ServerCall::Release {call,request})})=>{
                    let work=runtime.clone();
                    tasks.spawn(async move {work.answer_release(frame,call,&request).await?;Ok(())});
                }
                Some(ClientEvent::Request {frame,body:ServerRequest::Crate(ServerCall::WithdrawRelease {call,request})})=>{
                    let work=runtime.clone();
                    tasks.spawn(async move {work.answer_withdrawal(frame,call,&request).await?;Ok(())});
                }
                Some(ClientEvent::Request {..})=>return Err("writer fixture received an unconfigured server request".into()),
                Some(_)=>{},None=>break,
            },
            Some(task)=tasks.join_next(),if !tasks.is_empty()=>{task??;},
        }
    }Ok(())}.await;
    stop.send_replace(true);
    runtime.transport().release_seed_page_answer();
    runtime.transport().release_transport_call();
    database.release_bootstrap_rename();
    while let Some(task) = tasks.join_next().await {
        task??;
    }
    database.close().await;
    store.close().await;
    result
}
