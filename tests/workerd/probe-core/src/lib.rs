#![forbid(unsafe_code)]
//! One application callback; adapters supply only SQLx transaction scope.
use futures_core::future::BoxFuture;
use sqlx_core::{
    column::ColumnIndex, database::Database, decode::Decode, encode::Encode, executor::Executor,
    query::query, row::Row, types::Type,
};
use std::future::Future;

#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    #[error("storage: {0}")]
    Storage(#[from] sqlx_core::error::Error),
    #[error("domain decision rejected")]
    Rejected,
}

pub trait TransactionScope: Send {
    type Database: Database;
    type Executor<'query>: Executor<'query, Database = Self::Database>
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_>;
}

pub trait TransactionCallback<TStore: TransactionStore, TOutput>: Send + 'static {
    fn call<'scope>(
        self,
        scope: TStore::Scope<'scope>,
    ) -> BoxFuture<'scope, Result<TOutput, ProbeError>>;
}

pub trait TransactionStore: Send + 'static {
    type Database: Database;
    type Scope<'scope>: TransactionScope<Database = Self::Database>
    where
        Self: 'scope;
    fn transaction<TOutput, TCallback>(
        &self,
        callback: TCallback,
    ) -> impl Future<Output = Result<TOutput, ProbeError>> + Send
    where
        TOutput: Send + 'static,
        TCallback: TransactionCallback<Self, TOutput>,
        Self: Sized;
}

pub enum CallbackEnding {
    Commit,
    CallbackError,
    SqlError,
    PauseThenCommit {
        entered: futures_channel::oneshot::Sender<()>,
        release: futures_channel::oneshot::Receiver<()>,
    },
    PauseThenError {
        entered: futures_channel::oneshot::Sender<()>,
        release: futures_channel::oneshot::Receiver<()>,
    },
}
pub struct ReadDecideInsert {
    pub request_name: String,
    pub ending: CallbackEnding,
}
impl<TStore> TransactionCallback<TStore, String> for ReadDecideInsert
where
    TStore: TransactionStore,
    <TStore::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TStore::Database>,
    for<'value> String: Type<TStore::Database>
        + Decode<'value, TStore::Database>
        + Encode<'value, TStore::Database>,
    for<'row> &'row str: ColumnIndex<<TStore::Database as Database>::Row>,
{
    fn call<'scope>(
        self,
        scope: TStore::Scope<'scope>,
    ) -> BoxFuture<'scope, Result<String, ProbeError>> {
        Box::pin(read_decide_insert(scope, self.request_name, self.ending))
    }
}

mod checked_source;
struct StoredSource {
    source_name: Option<String>,
}

pub async fn read_decide_insert<TScope>(
    mut scope: TScope,
    request_name: String,
    ending: CallbackEnding,
) -> Result<String, ProbeError>
where
    TScope: TransactionScope,
    <TScope::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TScope::Database>,
    for<'value> String: Type<TScope::Database>
        + Decode<'value, TScope::Database>
        + Encode<'value, TScope::Database>,
    for<'row> &'row str: ColumnIndex<<TScope::Database as Database>::Row>,
{
    let source_name = checked_source::fetch_source(&mut scope)
        .await?
        .source_name
        .ok_or(ProbeError::Rejected)?;
    if source_name != "ready" {
        return Err(ProbeError::Rejected);
    }
    query::<TScope::Database>(
        "INSERT INTO dependent_records (request_name, source_name) VALUES (?, ?)",
    )
    .bind(request_name.clone())
    .bind(source_name)
    .execute(scope.executor())
    .await?;
    match ending {
        CallbackEnding::Commit => {}
        CallbackEnding::CallbackError => return Err(ProbeError::Rejected),
        CallbackEnding::SqlError => {
            query::<TScope::Database>("INSERT INTO nonexistent_table VALUES (1)")
                .execute(scope.executor())
                .await?;
            return Err(ProbeError::Rejected);
        }
        CallbackEnding::PauseThenCommit { entered, release } => {
            query::<TScope::Database>("SELECT request_name FROM dependent_records")
                .fetch_all(scope.executor())
                .await?;
            entered.send(()).map_err(|_| ProbeError::Rejected)?;
            release.await.map_err(|_| ProbeError::Rejected)?;
        }
        CallbackEnding::PauseThenError { entered, release } => {
            // Controlled completion seam around a real storage operation, with no network/timer.
            query::<TScope::Database>("SELECT request_name FROM dependent_records")
                .fetch_all(scope.executor())
                .await?;
            entered.send(()).map_err(|_| ProbeError::Rejected)?;
            release.await.map_err(|_| ProbeError::Rejected)?;
            return Err(ProbeError::Rejected);
        }
    }
    let dependent_row = query::<TScope::Database>(
        "SELECT request_name FROM dependent_records WHERE request_name = ?",
    )
    .bind(request_name)
    .fetch_one(scope.executor())
    .await?;
    Ok(dependent_row.try_get("request_name")?)
}

pub const SCHEMA: &str = include_str!("../../schema.sql");

pub mod numeric_cases;
pub mod transaction_cases;

pub mod shared_http;
