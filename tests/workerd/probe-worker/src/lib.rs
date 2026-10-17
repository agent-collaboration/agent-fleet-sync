#![forbid(unsafe_code)]
use probe_core::{ProbeError, TransactionCallback, TransactionScope, TransactionStore};
use sqlx_core::executor::Executor;
use sqlx_durable_object::{Do, DoConnection, DoTransaction};
use std::{future::Future, sync::Arc};
use tower_service::Service;
use worker::{Context, DurableObject, Env, Request, Response, State, durable_object, event};

#[derive(Clone)]
struct WorkerStorage {
    connection: Arc<DoConnection>,
}
struct WorkerScope<'scope> {
    transaction: &'scope DoTransaction,
}
impl TransactionScope for WorkerScope<'_> {
    type Database = Do;
    type Executor<'query>
        = &'query DoTransaction
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.transaction
    }
}
impl TransactionStore for WorkerStorage {
    type Database = Do;
    type Scope<'scope> = WorkerScope<'scope>;
    fn transaction<TOutput, TCallback>(
        &self,
        callback: TCallback,
    ) -> impl Future<Output = Result<TOutput, ProbeError>> + Send
    where
        TOutput: Send + 'static,
        TCallback: TransactionCallback<Self, TOutput>,
    {
        self.connection.transaction(move |transaction| async move {
            callback
                .call(WorkerScope {
                    transaction: &transaction,
                })
                .await
        })
    }
}
impl probe_core::shared_http::PlatformCapabilities for WorkerStorage {
    fn run_effect(
        &self,
        effect: probe_core::shared_http::CommittedEffect,
    ) -> impl Future<Output = Result<String, ProbeError>> + Send {
        worker::send::SendFuture::new(async move {
            // SDK storage.sync is the real durable-write completion primitive.
            self.connection.storage().sync().await.map_err(|error| {
                ProbeError::Storage(sqlx_core::error::Error::Protocol(error.to_string()))
            })?;
            let rows = self
                .transaction(probe_core::transaction_cases::ReadNames)
                .await?;
            let receipt = probe_core::shared_http::validate_effect_visibility(&rows, &effect)?;
            worker::console_log!("EFFECT {}", effect.request_name);
            Ok(receipt)
        })
    }
}
#[durable_object]
pub struct ProbeObject {
    state: State,
}
impl DurableObject for ProbeObject {
    fn new(state: State, _env: Env) -> Self {
        Self { state }
    }
    async fn fetch(&self, request: Request) -> worker::Result<Response> {
        let connection = DoConnection::new(self.state.storage());
        (&connection)
            .execute(probe_core::SCHEMA)
            .await
            .map_err(worker_error)?;
        let storage = WorkerStorage {
            connection: Arc::new(connection),
        };
        let request: worker::HttpRequest = request.try_into()?;
        let response = probe_core::shared_http::shared_router(storage)
            .call(request)
            .await
            .map_err(worker_error)?;
        response.try_into()
    }
}
fn worker_error(error: impl std::fmt::Display) -> worker::Error {
    worker::Error::RustError(error.to_string())
}
#[event(fetch)]
async fn fetch(
    request: worker::HttpRequest,
    env: Env,
    _context: Context,
) -> worker::Result<worker::HttpResponse> {
    let namespace = env.durable_object("PROBE")?;
    let name = env.var("PROBE_OBJECT_NAME")?.to_string();
    let response = namespace
        .id_from_name(&name)?
        .get_stub()?
        .fetch_with_request(request.try_into()?)
        .await?;
    response.try_into()
}
