use super::{session_fixture_contract::*, session_fixture_store::*};
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use sqlx_turso::{TursoConnection, sqlx::Connection};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

pub enum SocketCommand {
    Send(Bytes),
    Close(CloseReason),
    Drop,
}
pub struct SocketPeer {
    pub sender: mpsc::Sender<SocketCommand>,
}
pub struct HeldClose {
    pub release: oneshot::Sender<()>,
    pub done: oneshot::Receiver<()>,
}
pub struct HostDatabase {
    pub connection: TursoConnection,
    pub registrations: BTreeMap<ConnectionId, ConnectionRegistration>,
    pub sockets: BTreeMap<ConnectionId, SocketPeer>,
    pub drop_next_answer: bool,
    pub unreadable_copy: bool,
    pub fail_next_welcome: bool,
    pub hold_next_close: bool,
    pub held_closes: Vec<HeldClose>,
}
#[derive(Clone)]
pub struct FixtureHost {
    pub database: Arc<Mutex<HostDatabase>>,
    pub clock: watch::Sender<SyncTime>,
    pub stop: watch::Sender<bool>,
    pub close_held: watch::Sender<bool>,
}
pub enum RuleInput {
    Opened,
    Frame(Bytes),
    Closed(CloseCause),
    Revoke,
}

impl FixtureHost {
    pub async fn open_and_migrate(path: &Path) -> TestResult<Self> {
        let mut connection = open_and_migrate(path).await?;
        let state = read_state(&mut connection).await?;
        let (clock, _) = watch::channel(state.now);
        let (stop, _) = watch::channel(false);
        let (close_held, _) = watch::channel(false);
        Ok(Self {
            database: Arc::new(Mutex::new(HostDatabase {
                connection,
                registrations: BTreeMap::new(),
                sockets: BTreeMap::new(),
                drop_next_answer: false,
                unreadable_copy: false,
                fail_next_welcome: false,
                hold_next_close: false,
                held_closes: vec![],
            })),
            clock,
            stop,
            close_held,
        })
    }

    pub async fn apply(
        &self,
        binding: ConnectionBinding,
        input: RuleInput,
    ) -> TestResult<Vec<SessionEffect<FixtureProtocol>>> {
        let mut database = self.database.lock().await;
        let registrations = database.registrations.clone();
        let mut transaction = database.connection.begin_with("BEGIN IMMEDIATE").await?;
        let state = read_state(&mut transaction).await?;
        let now = state.now;
        let mut scope = FixtureScope {
            transaction: &mut transaction,
            state,
            registrations,
        };
        let effects: Vec<SessionEffect<FixtureProtocol>> = match input {
            RuleInput::Opened if scope.state.revoked => vec![
                SessionEffect::Send {
                    connection: binding.connection,
                    frame: ServerFrame::Closed {
                        reason: CloseReason::CredentialRejected(AuthRefusal::Revoked),
                    },
                },
                SessionEffect::Close {
                    connection: binding.connection,
                    reason: CloseReason::CredentialRejected(AuthRefusal::Revoked),
                },
            ],
            RuleInput::Opened => connection_opened(&mut scope, &binding, now).await?,
            RuleInput::Frame(frame) => frame_received(&mut scope, &binding, &frame, now).await?,
            RuleInput::Closed(cause) => connection_closed(&mut scope, &binding, cause, now).await?,
            RuleInput::Revoke => {
                scope.state.revoked = true;
                end_sessions(&mut scope, &binding.peer, EndReason::PeerRevoked, now).await?
            }
        };
        let mut expanded = Vec::new();
        for effect in effects {
            match effect {
                SessionEffect::Route {
                    frame,
                    operation: OperationRequest::Consumer(request),
                    ..
                } => {
                    let answer = if let Some(receipt) = scope
                        .state
                        .receipts
                        .iter()
                        .find(|receipt| receipt.operation == request.operation)
                    {
                        if receipt.amount == request.amount {
                            IncrementAnswer::Applied {
                                value: receipt.value,
                            }
                        } else {
                            IncrementAnswer::Conflict
                        }
                    } else {
                        scope.state.value = scope
                            .state
                            .value
                            .checked_add(request.amount)
                            .ok_or("fixture counter overflow")?;
                        scope.state.receipts.push(IncrementReceipt {
                            operation: request.operation,
                            amount: request.amount,
                            value: scope.state.value,
                        });
                        IncrementAnswer::Applied {
                            value: scope.state.value,
                        }
                    };
                    expanded.push(SessionEffect::Send {
                        connection: binding.connection,
                        frame: ServerFrame::Answer {
                            frame,
                            body: OperationAnswer::Consumer(answer),
                        },
                    });
                }
                SessionEffect::Route { .. } | SessionEffect::RouteAnswer { .. } => {
                    return Err("unsupported fixture operation".into());
                }
                effect => expanded.push(effect),
            }
        }
        write_state(scope.transaction, &scope.state).await?;
        let registrations = scope.registrations;
        transaction.commit().await?;
        database.registrations = registrations;
        // Deliberately lose one answer only after the operation and receipt commit.
        if database.drop_next_answer
            && expanded.iter().any(|effect| {
                matches!(
                    effect,
                    SessionEffect::Send {
                        frame: ServerFrame::Answer { .. },
                        ..
                    }
                )
            })
        {
            database.drop_next_answer = false;
            expanded.retain(|effect| {
                !matches!(
                    effect,
                    SessionEffect::Send {
                        frame: ServerFrame::Answer { .. },
                        ..
                    }
                )
            });
            if let Some(socket) = database.sockets.get(&binding.connection) {
                socket
                    .sender
                    .try_send(SocketCommand::Drop)
                    .map_err(|_| "fixture drop queue full")?;
            }
        }
        Ok(expanded)
    }

    pub async fn dispatch(&self, effects: Vec<SessionEffect<FixtureProtocol>>) -> TestResult {
        for effect in effects {
            let (connection, command) = match effect {
                SessionEffect::Welcome {
                    connection,
                    peer,
                    session_id,
                } => {
                    let command = match self.gather_databases(peer).await {
                        Ok(databases) => SocketCommand::Send(
                            serde_json::to_vec(&ServerFrame::<FixtureProtocol>::Welcome {
                                session_id,
                                databases,
                            })?
                            .into(),
                        ),
                        Err(_) => SocketCommand::Drop,
                    };
                    (connection, command)
                }
                SessionEffect::Send { connection, frame } => (
                    connection,
                    SocketCommand::Send(serde_json::to_vec(&frame)?.into()),
                ),
                SessionEffect::Close { connection, reason } => {
                    (connection, SocketCommand::Close(reason))
                }
                SessionEffect::Route { .. } | SessionEffect::RouteAnswer { .. } => {
                    return Err("unexpanded fixture route".into());
                }
            };
            let sender = self
                .database
                .lock()
                .await
                .sockets
                .get(&connection)
                .map(|socket| socket.sender.clone());
            if let Some(sender) = sender {
                let _ = tokio::time::timeout(Duration::from_secs(2), sender.send(command)).await;
            }
        }
        Ok(())
    }

    // Registry/copy stand-ins are host-owned reads; session SQL owns no copy state.
    async fn gather_databases(&self, peer: PeerId) -> TestResult<Vec<DatabaseStatus>> {
        let mut host = self.database.lock().await;
        if std::mem::take(&mut host.fail_next_welcome) {
            return Err("injected registry gather failure".into());
        }
        let state = read_state(&mut host.connection).await?;
        let database = DatabaseId::new(uuid::Uuid::from_u128(10))?;
        let epoch = Epoch::new(1)?;
        let remaining_ms = u64::try_from(
            state
                .lease_expires_at
                .unix_millis()
                .saturating_sub(state.now.unix_millis())
                .max(0),
        )?;
        let mut databases = vec![DatabaseStatus::Ready {
            database,
            acked: ReplicationPosition::new(epoch, CommitSequence::new(state.value)),
            lease: LeaseStatus {
                database,
                epoch,
                holder: peer,
                state: if remaining_ms == 0 {
                    LeaseState::Expired
                } else {
                    LeaseState::Held
                },
                expires_at: state.lease_expires_at,
                remaining_ms,
            },
        }];
        if host.unreadable_copy {
            databases.push(DatabaseStatus::Unavailable {
                database: DatabaseId::new(uuid::Uuid::from_u128(11))?,
                lease: None,
                reason: CopyUnavailableReason::Damaged {
                    detail: "injected unreadable copy".into(),
                },
            });
        }
        Ok(databases)
    }

    pub async fn control(&self, request: ControlRequest) -> TestResult<FixtureSnapshot> {
        match request {
            ControlRequest::SetUnreadableCopy { unreadable } => {
                self.database.lock().await.unreadable_copy = unreadable
            }
            ControlRequest::FailNextWelcome => self.database.lock().await.fail_next_welcome = true,
            ControlRequest::DropNextAnswer => self.database.lock().await.drop_next_answer = true,
            ControlRequest::HoldNextClose => {
                self.close_held.send_replace(false);
                self.database.lock().await.hold_next_close = true;
            }
            ControlRequest::ReleaseHeldClose => {
                let mut ready = self.close_held.subscribe();
                if !*ready.borrow() {
                    tokio::time::timeout(Duration::from_secs(3), ready.changed()).await??;
                }
                let held = std::mem::take(&mut self.database.lock().await.held_closes);
                for close in held {
                    let _ = close.release.send(());
                    tokio::time::timeout(Duration::from_secs(3), close.done).await??;
                }
            }
            ControlRequest::Revoke => {
                let binding = ConnectionBinding::new(
                    ConnectionId::new(uuid::Uuid::now_v7())?,
                    VerifiedPeer {
                        peer: PeerId::new(uuid::Uuid::from_u128(1))?,
                        expires_at: SyncTime::from_unix_millis(10_000),
                    },
                );
                self.dispatch(self.apply(binding, RuleInput::Revoke).await?)
                    .await?;
            }
            ControlRequest::AdvanceClock { now } => {
                self.update_state(|state| state.now = now).await?;
                self.clock.send_replace(now);
            }
            ControlRequest::SetLease { expires_at } => {
                self.update_state(|state| state.lease_expires_at = expires_at)
                    .await?
            }
            ControlRequest::Allow => self.update_state(|state| state.revoked = false).await?,
            ControlRequest::Stop => {
                self.stop.send_replace(true);
            }
            ControlRequest::Snapshot => {}
        }
        self.snapshot().await
    }

    async fn update_state(&self, update: impl FnOnce(&mut FixtureState)) -> TestResult {
        let mut database = self.database.lock().await;
        let mut transaction = database.connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut state = read_state(&mut transaction).await?;
        update(&mut state);
        write_state(&mut transaction, &state).await?;
        transaction.commit().await?;
        Ok(())
    }
    pub async fn snapshot(&self) -> TestResult<FixtureSnapshot> {
        let mut database = self.database.lock().await;
        let state = read_state(&mut database.connection).await?;
        Ok(FixtureSnapshot {
            value: state.value,
            receipt_count: state.receipts.len(),
            sessions: read_sessions(&mut database.connection).await?,
            registrations: database
                .registrations
                .iter()
                .map(|(id, registration)| {
                    (
                        *id,
                        match registration {
                            ConnectionRegistration::Attached { session_id, .. } => {
                                Some(*session_id)
                            }
                            _ => None,
                        },
                    )
                })
                .collect(),
            held_closes: database.held_closes.len(),
        })
    }
}

impl PeerAuthenticator for FixtureHost {
    type Material = String;
    async fn verify(&self, material: String) -> Result<VerifiedPeer, AuthRefusal> {
        let expires_at = match material.as_str() {
            "Bearer fixture" => SyncTime::from_unix_millis(10_000),
            "Bearer short" => SyncTime::from_unix_millis(50),
            _ => return Err(AuthRefusal::Unknown),
        };
        let mut database = self.database.lock().await;
        let state = read_state(&mut database.connection)
            .await
            .map_err(|_| AuthRefusal::Unknown)?;
        if state.revoked {
            return Err(AuthRefusal::Revoked);
        }
        if expires_at <= state.now {
            return Err(AuthRefusal::Expired);
        }
        Ok(VerifiedPeer {
            peer: PeerId::new(uuid::Uuid::from_u128(1)).map_err(|_| AuthRefusal::WrongKind)?,
            expires_at,
        })
    }
}
