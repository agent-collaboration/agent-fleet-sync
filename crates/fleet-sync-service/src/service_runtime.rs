use crate::{
    ServiceConsumer, ServiceError, ServiceHost, ServiceOptions,
    call_router::{RoutedCall, ServiceCalls},
    registry_host::registry_engine,
    service_state::{ServiceSessions, ServiceState},
    session_handler::ServiceSessionHandler,
};
use fleet_sync_protocol::{ConsumerProtocol, DatabaseId};
use fleet_sync_session::PeerAuthenticator;
use fleet_sync_tokio_runner::{
    InstallCoordinator, InstallSnapshot, ObjectKey, ObjectOptions, ObjectStore, SessionHost,
    StagingStore, TokioRunner,
};
use runtime_primitives::SystemClock;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::IntoFuture,
    sync::{Arc, OnceLock},
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify, mpsc, watch},
    task::JoinSet,
};

/// The service owns every spawned task. The consumer supplies its authenticated
/// admin surface separately from the peer session router.
pub struct FleetService<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> {
    pub(crate) state: Arc<ServiceState<P, C, A>>,
    pub(crate) sessions: Arc<ServiceSessions<P, C, A>>,
    calls: mpsc::Receiver<RoutedCall>,
}
pub struct ServiceHandle<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> {
    pub(crate) state: Arc<ServiceState<P, C, A>>,
    pub(crate) sessions: Arc<ServiceSessions<P, C, A>>,
}
impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> Clone for ServiceHandle<P, C, A>
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            sessions: self.sessions.clone(),
        }
    }
}
impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> FleetService<P, C, A>
{
    pub async fn open(
        options: ServiceOptions,
        consumer: Arc<C>,
        authenticator: Arc<A>,
    ) -> Result<Self, ServiceError> {
        let (sender, calls) = mpsc::channel(128);
        let call_router = Arc::new(ServiceCalls {
            sender,
            answer_timeout: options.call_timeout,
        });
        let objects = Arc::new(ObjectStore::open(&options.directory).await?);
        let registry = objects
            .object(
                ObjectKey::new("registry").map_err(ServiceError::rule)?,
                ObjectOptions::default(),
                Arc::new(ServiceHost::new(call_router.clone(), SystemClock)),
            )
            .await?;
        let registry_engine = Arc::new(registry_engine(registry.clone(), options.registry).await?);
        let control = objects
            .object(
                ObjectKey::new("control").map_err(ServiceError::rule)?,
                ObjectOptions::default(),
                Arc::new(ServiceHost::new(call_router.clone(), SystemClock)),
            )
            .await?;
        let installs = Arc::new(
            InstallCoordinator::open(
                control.clone(),
                Arc::new(
                    StagingStore::open(options.directory.join("staging"))
                        .await
                        .map_err(ServiceError::rule)?,
                ),
            )
            .await?,
        );
        let mut control_engine = durable_steps::Engine::new(control.clone(), control.host());
        control_engine.register(InstallSnapshot::new(control.host()))?;
        let state = Arc::new(ServiceState {
            options,
            consumer,
            calls: call_router.clone(),
            objects: objects.clone(),
            registry,
            registry_engine,
            control,
            control_engine: Arc::new(control_engine),
            installs,
            sources: Mutex::new(BTreeMap::new()),
            source_runners: Mutex::new(Vec::new()),
            source_added: Notify::new(),
            shutdown: watch::channel(false).0,
            #[cfg(feature = "test-support")]
            install_page_probe: Default::default(),
            #[cfg(feature = "test-support")]
            schema_support_probe: Default::default(),
            #[cfg(feature = "test-support")]
            retirement_reply_probe: Default::default(),
            copies: Mutex::new(BTreeMap::new()),
            copy_added: Notify::new(),
            delivery: Mutex::new(BTreeMap::new()),
            delivery_added: Notify::new(),
            sessions: OnceLock::new(),
        });
        let session_object = objects
            .object(
                ObjectKey::new("sessions").map_err(ServiceError::rule)?,
                ObjectOptions::default(),
                Arc::new(ServiceHost::new(call_router, SystemClock)),
            )
            .await?;
        let sessions = Arc::new(
            SessionHost::open(
                session_object,
                authenticator,
                Arc::new(ServiceSessionHandler {
                    state: Arc::downgrade(&state),
                }),
                state.options.socket_timeout,
            )
            .await
            .map_err(ServiceError::rule)?,
        );
        state
            .sessions
            .set(Arc::downgrade(&sessions))
            .map_err(|_| ServiceError::rule("session owner already installed"))?;
        // Physical inventory is only a discovery hint. Each file's rules read
        // its own identity/state, and Welcome uses the registry's database list.
        let prefix = format!("copy-{}-", state.options.receiver.as_uuid());
        let mut files = tokio::fs::read_dir(&state.options.directory)
            .await
            .map_err(ServiceError::io)?;
        while let Some(entry) = files.next_entry().await.map_err(ServiceError::io)? {
            let name = entry.file_name();
            if state.options.delivery.is_some()
                && let Some(key) = name
                    .to_str()
                    .map(crate::delivery_owner::DeliveryObject::from_file)
                    .transpose()?
                    .flatten()
            {
                match key {
                    crate::delivery_owner::DeliveryObject::Mailbox(_) => {
                        state.delivery_slot(key).await;
                    }
                    crate::delivery_owner::DeliveryObject::Status(_) => {
                        state.delivery_owner(key).await?;
                    }
                }
                continue;
            }
            let Some(id) = name
                .to_str()
                .and_then(|name| name.strip_prefix(&prefix))
                .and_then(|name| name.strip_suffix(".db"))
            else {
                continue;
            };
            let database = DatabaseId::new(id.parse().map_err(ServiceError::rule)?)
                .map_err(ServiceError::rule)?;
            if let Err(error) = state.copy(database).await {
                tracing::warn!(%error,?database,"copy could not be opened at startup");
            }
        }
        Ok(Self {
            state,
            sessions,
            calls,
        })
    }
    pub fn handle(&self) -> ServiceHandle<P, C, A> {
        ServiceHandle {
            state: self.state.clone(),
            sessions: self.sessions.clone(),
        }
    }

    pub async fn serve(
        mut self,
        listener: TcpListener,
        additional_routes: axum::Router,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ServiceError> {
        let (app, mut incoming) = self.session_router();
        let app = app.merge(additional_routes);
        let stop = self.state.shutdown.clone();
        let stopped = stop.subscribe();
        let mut server_shutdown = stopped.clone();
        let server = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                loop {
                    if *server_shutdown.borrow() {
                        break;
                    }
                    if server_shutdown.changed().await.is_err() {
                        break;
                    }
                }
            })
            .into_future();
        tokio::pin!(server);
        let mut tasks = JoinSet::new();
        let session_runner = self.sessions.clone();
        let session_stop = stopped.clone();
        tasks.spawn(async move {
            session_runner
                .run(session_stop)
                .await
                .map_err(ServiceError::rule)
        });
        let registry_runner = TokioRunner::new(
            self.state.registry_engine.clone(),
            self.state.registry.host(),
            self.state.registry.engine_lock(),
        );
        let registry_stop = stopped.clone();
        tasks.spawn(async move {
            registry_runner
                .run(registry_stop)
                .await
                .map_err(ServiceError::from)
        });
        let control_runner = TokioRunner::new(
            self.state.control_engine.clone(),
            self.state.control.host(),
            self.state.control.engine_lock(),
        );
        let control_stop = stopped.clone();
        tasks.spawn(async move {
            control_runner
                .run(control_stop)
                .await
                .map_err(ServiceError::from)
        });
        let mut started = BTreeSet::new();
        let mut delivery_started = BTreeSet::new();
        let mut server_finished = false;
        let result = loop {
            if *shutdown.borrow() {
                break Ok(());
            }
            self.start_copy_runners(&mut started, &mut tasks, &stopped)
                .await;
            self.start_source_runners(&mut tasks, &stopped).await;
            self.start_delivery_runners(&mut delivery_started, &mut tasks, &stopped)
                .await;
            tokio::select! {
                _=self.state.delivery_added.notified()=>{},
                result=&mut server=>{server_finished=true;break result.map_err(ServiceError::io);}
                changed=shutdown.changed()=>if changed.is_err() || *shutdown.borrow() {break Ok(());},
                _=self.state.copy_added.notified()=>{},
                _=self.state.source_added.notified()=>{},
                accepted=incoming.recv()=>if let Some(accepted)=accepted {
                    let sessions=self.sessions.clone();let socket_timeout=self.state.options.socket_timeout;let shutdown=stopped.clone();
                    tasks.spawn(async move {
                        let result=sessions.serve_connection(fleet_sync_transport_websocket::AxumConnection::new(accepted.socket,socket_timeout),accepted.admission,shutdown).await.map_err(ServiceError::rule);
                        let _=accepted.done.send(());
                        result
                    });
                },
                call=self.calls.recv()=>match call {
                    Some(call)=>{
                        let state=self.state.clone();
                        let mut shutdown=stopped.clone();
                        tasks.spawn(async move {
                            let mut answer=call.answer;
                            tokio::select! {
                                result=state.route_call(call.envelope)=>{let _=answer.send(result);}
                                _=answer.closed()=>{},
                                _=shutdown.changed()=>{},
                            }
                            Ok(())
                        });
                    }
                    None=>break Err(ServiceError::rule("service call carrier closed")),
                },
                task=tasks.join_next(),if !tasks.is_empty()=>match task {
                    Some(Ok(Ok(())))=>{},
                    Some(Ok(Err(error)))=>break Err(error),
                    Some(Err(error))=>break Err(ServiceError::rule(error)),
                    None=>{},
                }
            }
        };
        stop.send_replace(true);
        // An attachment admitted just before shutdown still has an owner. Run
        // its shutdown path and join it before closing the native stores.
        self.start_source_runners(&mut tasks, &stopped).await;
        incoming.close();
        while let Ok(accepted) = incoming.try_recv() {
            drop(accepted.socket);
            let _ = accepted.done.send(());
        }
        self.calls.close();
        // Calls have deadlines and shutdown is delivered to every object runner.
        // The owner joins them before it closes native connections.
        while let Some(task) = tasks.join_next().await {
            if let Err(error) = task {
                tracing::warn!(%error,"service task stopped during shutdown");
            }
        }
        self.state.objects.close().await?;
        if !server_finished {
            tokio::time::timeout(self.state.options.socket_timeout, &mut server)
                .await
                .map_err(ServiceError::rule)?
                .map_err(ServiceError::io)?;
        }
        result
    }
    async fn start_delivery_runners(
        &self,
        started: &mut BTreeSet<crate::delivery_owner::DeliveryObject>,
        tasks: &mut JoinSet<Result<(), ServiceError>>,
        shutdown: &watch::Receiver<bool>,
    ) {
        let owners = self
            .state
            .delivery
            .lock()
            .await
            .iter()
            .map(|(key, slot)| (*key, slot.owner.get().cloned()))
            .collect::<Vec<_>>();
        for (key, owner) in owners {
            if started.contains(&key) {
                continue;
            }
            if let crate::delivery_owner::DeliveryObject::Mailbox(mailbox) = key {
                started.insert(key);
                let state = self.state.clone();
                let shutdown = shutdown.clone();
                let Some(limits) = self.state.options.delivery else {
                    continue;
                };
                tasks.spawn(async move {
                    crate::delivery_deadlines::run_mailbox(state, mailbox, limits, shutdown).await;
                    Ok(())
                });
                continue;
            }
            let Some(owner) = owner else {
                continue;
            };
            started.insert(key);
            let runner = TokioRunner::new(
                owner.engine.clone(),
                owner.object.host(),
                owner.object.engine_lock(),
            );
            let shutdown = shutdown.clone();
            tasks.spawn(async move { runner.run(shutdown).await.map_err(ServiceError::from) });
        }
    }
    async fn start_copy_runners(
        &self,
        started: &mut BTreeSet<DatabaseId>,
        tasks: &mut JoinSet<Result<(), ServiceError>>,
        shutdown: &watch::Receiver<bool>,
    ) {
        let owners = self
            .state
            .copies
            .lock()
            .await
            .iter()
            .filter_map(|(database, slot)| slot.get().map(|owner| (*database, owner.clone())))
            .collect::<Vec<_>>();
        for (database, owner) in owners {
            if !started.insert(database) {
                continue;
            }
            let runner = TokioRunner::new(
                owner.engine.clone(),
                owner.object.host(),
                owner.object.engine_lock(),
            );
            let shutdown = shutdown.clone();
            tasks.spawn(async move {
                if let Err(error) = runner.run(shutdown).await {
                    *owner.health.lock().await =
                        Some(fleet_sync_protocol::CopyUnavailableReason::Damaged {
                            detail: error.to_string(),
                        });
                    tracing::warn!(?database,%error,"copy runner unavailable");
                }
                Ok(())
            });
        }
    }
    async fn start_source_runners(
        &self,
        tasks: &mut JoinSet<Result<(), ServiceError>>,
        shutdown: &watch::Receiver<bool>,
    ) {
        let runners = std::mem::take(&mut *self.state.source_runners.lock().await);
        for mut source in runners {
            let mut shutdown = shutdown.clone();
            tasks.spawn(async move {
                let result = tokio::select! {
                    result=&mut source.run=>result,
                    _=shutdown.changed()=>{source.stop.send_replace(true);source.run.await},
                };
                if let Err(error) = result {
                    tracing::warn!(%error,"source session pump stopped");
                }
                Ok(())
            });
        }
    }
}
