//! A second delivery consumer with real durable custody and an external append-only effect sink.
//! Standard input is test control; all delivery hops use the relay's localhost WebSocket service.
mod consumer;
mod journal_observation;
mod observer;
mod router;
mod socket;

use crate::{SampleDelivery, SampleProtocol, TestResult, delivery_limits};
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use fleet_sync_tokio_runner::{
    ObjectKey, ObjectOptions, ObjectStore, TokioHost, TokioObject, TokioRunner,
};
use runtime_primitives::{Clock, SystemClock, WakeScheduler};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx_core::migrate::Migrator;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, OnceLock, Weak, atomic::AtomicBool},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{Mutex, Notify, watch},
};

type NodeHost = TokioHost<router::NodeRouter>;
type NodeStore = TokioObject<NodeHost>;
type SubmitTask = SubmitItem<NodeStore, sqlx_turso::Turso, SampleDelivery, consumer::NodeConsumer>;
type ReceiveTask =
    ReceiveItems<NodeStore, sqlx_turso::Turso, SampleDelivery, consumer::NodeConsumer>;
type DeliverTask =
    DeliverLocally<NodeStore, sqlx_turso::Turso, SampleDelivery, consumer::NodeConsumer>;
type ReportTask = ReportOutcome<NodeStore, sqlx_turso::Turso, SampleDelivery>;

#[derive(Serialize, Deserialize)]
pub enum NodeCommand {
    Snapshot,
    Connect {
        port: u16,
    },
    Disconnect,
    Enqueue {
        item: ItemId,
        recipient: PeerId,
        body: String,
        valid_until: Option<SyncTime>,
    },
    Cancel {
        item: ItemId,
        target: ItemId,
        recipient: PeerId,
    },
    Receive {
        stream: DeliveryStream,
    },
    StatusFetch,
    Query {
        mailbox: PeerId,
        items: Vec<ItemId>,
    },
    Retire {
        mailbox: PeerId,
        items: Vec<ItemId>,
    },
    Inspect {
        item: ItemId,
    },
    WaitStatus {
        item: ItemId,
        side: CustodySide,
        kind: String,
    },
    WaitMailboxUnavailable {
        hop: String,
    },
    WaitPendingCall {
        hop: String,
        attempts: u32,
    },
    SetStatusPaused {
        enabled: bool,
    },
    SetHeld {
        enabled: bool,
    },
    SetOversizedAcceptance {
        enabled: bool,
    },
    Release {
        item: ItemId,
    },
    Arm {
        hop: String,
        mode: FaultMode,
    },
    ArmItem {
        hop: String,
        item: ItemId,
        mode: FaultMode,
    },
    WaitReportRetired {
        item: ItemId,
    },
    WaitBarrier,
    Resume,
    Shutdown,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum CustodySide {
    Outbox,
    Inbox,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum FaultMode {
    Lose,
    Pause,
}

pub(crate) struct FaultGate {
    armed: Mutex<Option<ArmedFault>>,
    reached: Mutex<Option<String>>,
    changed: Notify,
    resumed: Notify,
}
struct ArmedFault {
    hop: String,
    item: Option<ItemId>,
    mode: FaultMode,
}
impl FaultGate {
    fn new() -> Self {
        Self {
            armed: Mutex::new(None),
            reached: Mutex::new(None),
            changed: Notify::new(),
            resumed: Notify::new(),
        }
    }
    async fn arm(&self, hop: String, mode: FaultMode) {
        self.arm_item(hop, None, mode).await;
    }
    async fn arm_item(&self, hop: String, item: Option<ItemId>, mode: FaultMode) {
        *self.reached.lock().await = None;
        *self.armed.lock().await = Some(ArmedFault { hop, item, mode });
    }
    pub async fn after(&self, hop: &str) -> bool {
        self.after_item(hop, None).await
    }
    pub async fn after_item(&self, hop: &str, item: Option<ItemId>) -> bool {
        let mode = {
            let mut armed = self.armed.lock().await;
            if armed.as_ref().is_none_or(|expected| {
                expected.hop != hop || expected.item.is_some() && expected.item != item
            }) {
                return false;
            }
            armed.take().map(|fault| fault.mode)
        };
        *self.reached.lock().await = Some(hop.into());
        self.changed.notify_waiters();
        match mode {
            Some(FaultMode::Lose) => true,
            Some(FaultMode::Pause) => {
                self.resumed.notified().await;
                false
            }
            None => false,
        }
    }
    async fn wait(&self) -> String {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(hop) = self.reached.lock().await.clone() {
                return hop;
            }
            notified.await;
        }
    }
}

pub(crate) struct NodeState {
    object: Arc<NodeStore>,
    owner: PeerId,
    link: socket::PeerLink,
    changed: Arc<Notify>,
    gate: Arc<FaultGate>,
    held: AtomicBool,
    oversized: AtomicBool,
    effect_file: PathBuf,
    effect_lock: Mutex<()>,
    status_paused: AtomicBool,
    unavailable: Mutex<Vec<(String, CallId)>>,
}
impl NodeState {
    fn config(&self) -> Result<DeliveryTaskConfig, DeliveryError> {
        Ok(DeliveryTaskConfig {
            owner: self.owner,
            relay: peer(3)?,
            limits: delivery_limits()?,
            fetch_items: 100,
            fetch_bytes: 1_000_000,
        })
    }
    async fn kick(&self) -> TestResult {
        let host = self.object.host();
        host.wake_at(host.now())
            .await
            .map_err(|error| error.to_string())?;
        self.changed.notify_waiters();
        Ok(())
    }
    async fn inspect(&self, item: ItemId) -> TestResult<Value> {
        let config = self.config()?;
        let (outbox, inbox) = self
            .object
            .rule_transaction::<_, DeliveryError, _>(move |scope| {
                Box::pin(async move {
                    let outbox = Outbox::<_, SampleDelivery>::new(scope, config.limits)
                        .status(item)
                        .await?;
                    let inbox = Inbox::<_, SampleDelivery>::new(scope, config.owner, config.limits)
                        .local_outcome(item)
                        .await?;
                    Ok((outbox, inbox))
                })
            })
            .await?;
        let effects = {
            let _guard = self.effect_lock.lock().await;
            consumer::read_effects(&self.effect_file).await?
        };
        Ok(json!({"outbox":outbox,"inbox":inbox,"effects":effects}))
    }
    async fn wait_status(&self, item: ItemId, side: CustodySide, kind: &str) -> TestResult<Value> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let snapshot = self.inspect(item).await?;
            let status = match side {
                CustodySide::Inbox => &snapshot["inbox"]["kind"],
                CustodySide::Outbox => &snapshot["outbox"]["state"]["value"]["status"]["kind"],
            };
            if status.as_str() == Some(kind)
                || matches!(side, CustodySide::Inbox)
                    && kind == "absent"
                    && snapshot["inbox"].is_null()
            {
                return Ok(snapshot);
            }
            notified.await;
        }
    }
    async fn dispatch(&self, command: NodeCommand) -> TestResult<Value> {
        let config = self.config()?;
        match command {
            NodeCommand::Snapshot => {
                return Ok(serde_json::to_value(
                    self.object
                        .rule_transaction::<_, DeliveryError, _>(|scope| {
                            Box::pin(fleet_sync_delivery::test_support::snapshot(scope))
                        })
                        .await?,
                )?);
            }
            NodeCommand::Connect { port } => self.link.connect(port, self.owner).await?,
            NodeCommand::Disconnect => self.link.disconnect().await?,
            NodeCommand::Enqueue {
                item,
                recipient,
                body,
                valid_until,
            } => {
                let item = DeliveryItem::<SampleDelivery>::new(
                    item,
                    "authorized sender".into(),
                    Recipient {
                        peer: recipient,
                        address: "effects".into(),
                    },
                    valid_until,
                    body,
                )?;
                let admitted = self
                    .object
                    .rule_transaction::<_, DeliveryError, _>(move |scope| {
                        Box::pin(async move {
                            Outbox::<_, SampleDelivery>::new(scope, config.limits)
                                .enqueue_and_admit::<SubmitTask>(item, config.relay)
                                .await
                        })
                    })
                    .await?;
                self.kick().await?;
                return Ok(serde_json::to_value(admitted.0)?);
            }
            NodeCommand::Cancel {
                item,
                target,
                recipient,
            } => {
                let cancel = DeliveryCancel::<SampleDelivery>::new(
                    item,
                    "authorized sender".into(),
                    Recipient {
                        peer: recipient,
                        address: "effects".into(),
                    },
                    None,
                    CancelBody { target },
                )?;
                let admitted = self
                    .object
                    .rule_transaction::<_, DeliveryError, _>(move |scope| {
                        Box::pin(async move {
                            Outbox::<_, SampleDelivery>::new(scope, config.limits)
                                .enqueue_cancel_and_admit::<SubmitTask, _>(
                                    cancel,
                                    config.relay,
                                    &consumer::NodeConsumer(Weak::new()),
                                )
                                .await
                        })
                    })
                    .await?;
                self.kick().await?;
                return Ok(serde_json::to_value(admitted.0)?);
            }
            NodeCommand::Receive { stream } => {
                self.object
                    .rule_transaction::<_, DeliveryError, _>(move |scope| {
                        Box::pin(async move {
                            Inbox::<_, SampleDelivery>::new(scope, config.owner, config.limits)
                                .begin_receive_and_admit::<ReceiveTask>(stream)
                                .await
                        })
                    })
                    .await?;
                self.kick().await?;
            }
            NodeCommand::Release { item } => {
                let now = self.object.host().now();
                self.object
                    .rule_transaction::<_, DeliveryError, _>(move |scope| {
                        Box::pin(async move {
                            Inbox::<_, SampleDelivery>::new(scope, config.owner, config.limits)
                                .release_held::<DeliverTask>(item, now)
                                .await
                        })
                    })
                    .await?;
                self.kick().await?;
            }
            NodeCommand::Inspect { item } => return self.inspect(item).await,
            NodeCommand::WaitStatus { item, side, kind } => {
                return self.wait_status(item, side, &kind).await;
            }
            NodeCommand::WaitMailboxUnavailable { hop } => {
                return self.wait_mailbox_unavailable(hop).await;
            }
            NodeCommand::WaitPendingCall { hop, attempts } => {
                return self.wait_pending_call(hop, attempts).await;
            }
            NodeCommand::SetStatusPaused { enabled } => self
                .status_paused
                .store(enabled, std::sync::atomic::Ordering::SeqCst),
            NodeCommand::SetHeld { enabled } => self
                .held
                .store(enabled, std::sync::atomic::Ordering::SeqCst),
            NodeCommand::SetOversizedAcceptance { enabled } => self
                .oversized
                .store(enabled, std::sync::atomic::Ordering::SeqCst),
            NodeCommand::Arm { hop, mode } => self.gate.arm(hop, mode).await,
            NodeCommand::ArmItem { hop, item, mode } => {
                self.gate.arm_item(hop, Some(item), mode).await
            }
            NodeCommand::WaitReportRetired { item } => return self.wait_report_retired(item).await,
            NodeCommand::WaitBarrier => return Ok(json!({"hop":self.gate.wait().await})),
            NodeCommand::Resume => self.gate.resumed.notify_one(),
            NodeCommand::StatusFetch => {
                let after = self
                    .object
                    .rule_transaction::<_, DeliveryError, _>(move |scope| {
                        Box::pin(async move {
                            Outbox::<_, SampleDelivery>::new(scope, config.limits)
                                .status_cursor()
                                .await
                        })
                    })
                    .await?;
                let answer = self
                    .link
                    .request(DeliveryRequest::StatusFetch {
                        call: new_call()?,
                        after,
                        limit: 100,
                    })
                    .await
                    .map_err(|error| format!("status fetch: {error:?}"))?;
                let DeliveryAnswer::StatusFetch(batch) = answer else {
                    return Err("wrong status fetch answer".into());
                };
                let receipt = serde_json::to_value(&batch)?;
                self.object
                    .rule_transaction::<_, DeliveryError, _>(move |scope| {
                        Box::pin(async move {
                            Outbox::<_, SampleDelivery>::new(scope, config.limits)
                                .apply_status_batch(after, batch)
                                .await
                        })
                    })
                    .await?;
                self.changed.notify_waiters();
                return Ok(receipt);
            }
            NodeCommand::Query { mailbox, items } => {
                return Ok(serde_json::to_value(
                    self.link
                        .request(DeliveryRequest::Query {
                            call: new_call()?,
                            mailbox,
                            items,
                        })
                        .await
                        .map_err(|error| format!("query: {error:?}"))?,
                )?);
            }
            NodeCommand::Retire { mailbox, items } => {
                return Ok(serde_json::to_value(
                    self.link
                        .request(DeliveryRequest::Retire {
                            call: new_call()?,
                            mailbox,
                            items,
                        })
                        .await
                        .map_err(|error| format!("retire: {error:?}"))?,
                )?);
            }
            NodeCommand::Shutdown => {}
        }
        Ok(json!({"ok":true}))
    }
}

pub fn peer(number: u128) -> Result<PeerId, DeliveryError> {
    PeerId::new(durable_steps::Uuid::from_u128(
        (number) | 0x019eef00000070008000000000000000,
    ))
    .map_err(|error| DeliveryError::Encoding(error.to_string()))
}
fn new_call() -> TestResult<CallId> {
    Ok(CallId {
        operation: OperationId::new(durable_steps::Uuid::now_v7())?,
        step: StepName::new("delivery-node-control")?,
    })
}
static MIGRATIONS: Migrator = sqlx::migrate!("../../crates/fleet-sync-delivery/migrations");
pub async fn run(directory: &Path, owner: u128) -> TestResult {
    let router = Arc::new(router::NodeRouter(OnceLock::<Weak<NodeState>>::new()));
    let host = Arc::new(NodeHost::new(router.clone(), SystemClock));
    let objects = ObjectStore::open(directory).await?;
    let object = objects
        .object(
            ObjectKey::new("delivery-node")?,
            ObjectOptions::default(),
            host.clone(),
        )
        .await?;
    object
        .transaction(|scope| {
            Box::pin(async move {
                let mut migrations =
                    Migrator::with_migrations(MIGRATIONS.iter().cloned().collect::<Vec<_>>());
                migrations.dangerous_set_table_name("_fleet_sync_delivery_migrations");
                migrations
                    .run_direct(None, scope.executor(), false)
                    .await
                    .map_err(StoreError::backend)
            })
        })
        .await?;
    let (link, actor) = socket::PeerLink::new();
    let state = Arc::new(NodeState {
        object: object.clone(),
        owner: peer(owner)?,
        link,
        changed: Arc::new(Notify::new()),
        gate: Arc::new(FaultGate::new()),
        held: AtomicBool::new(false),
        oversized: AtomicBool::new(false),
        effect_file: directory.join("effects.jsonl"),
        effect_lock: Mutex::new(()),
        status_paused: AtomicBool::new(false),
        unavailable: Mutex::new(Vec::new()),
    });
    router
        .0
        .set(Arc::downgrade(&state))
        .map_err(|_| "node router initialized twice")?;
    let consumer = Arc::new(consumer::NodeConsumer(Arc::downgrade(&state)));
    let config = state.config()?;
    let mut engine = Engine::new(object.clone(), host.clone());
    engine.register(observer::ObservedTask::new(
        SubmitTask::new(config.limits, consumer.clone()),
        &state,
    ))?;
    engine.register(observer::ObservedTask::new(
        ReceiveTask::new(config, consumer.clone()),
        &state,
    ))?;
    engine.register(observer::ObservedTask::new(
        DeliverTask::new(config, consumer, host.clone()),
        &state,
    ))?;
    engine.register(observer::ObservedTask::new(ReportTask::new(config), &state))?;
    let (shutdown, stopped) = watch::channel(false);
    let runner = TokioRunner::new(Arc::new(engine), host, object.engine_lock());
    let runner_task = tokio::spawn(async move { runner.run(stopped).await });
    let socket_task = tokio::spawn(actor.run(Arc::downgrade(&state), shutdown.subscribe()));
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    let mut output = tokio::io::stdout();
    output
        .write_all(format!("READY {}\n", std::process::id()).as_bytes())
        .await?;
    output.flush().await?;
    while let Some(line) = input.next_line().await? {
        let command: NodeCommand = serde_json::from_str(&line)?;
        let stop = matches!(command, NodeCommand::Shutdown);
        let answer =
            match tokio::time::timeout(std::time::Duration::from_secs(45), state.dispatch(command))
                .await
            {
                Ok(Ok(value)) => json!({"result":value}),
                Ok(Err(error)) => json!({"error":error.to_string()}),
                Err(_) => {
                    json!({"error":"node command timed out awaiting committed state or barrier"})
                }
            };
        output
            .write_all(format!("{}\n", serde_json::to_string(&answer)?).as_bytes())
            .await?;
        output.flush().await?;
        if stop {
            break;
        }
    }
    shutdown.send_replace(true);
    // Every spawned owner is joined. Tests use child.kill only at an explicit committed hop barrier.
    runner_task.await??;
    socket_task.await??;
    objects.close().await?;
    Ok(())
}
