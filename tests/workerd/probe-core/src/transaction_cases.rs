//! Real storage scenarios, shared by both probe callers.
use crate::{
    CallbackEnding, ProbeError, ReadDecideInsert, TransactionCallback, TransactionScope,
    TransactionStore,
};
use futures_core::future::BoxFuture;
use futures_util::{
    future::{Either, join, poll_fn, select},
    pin_mut,
};
use sqlx_core::{
    column::ColumnIndex, database::Database, decode::Decode, encode::Encode, query::query,
    row::Row, types::Type,
};
use std::{future::Future, task::Poll};

pub struct ReadNames;
impl<TStore> TransactionCallback<TStore, Vec<String>> for ReadNames
where
    TStore: TransactionStore,
    <TStore::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TStore::Database>,
    for<'value> String: Type<TStore::Database> + Decode<'value, TStore::Database>,
    for<'row> &'row str: ColumnIndex<<TStore::Database as Database>::Row>,
{
    fn call<'scope>(
        self,
        mut scope: TStore::Scope<'scope>,
    ) -> BoxFuture<'scope, Result<Vec<String>, ProbeError>> {
        Box::pin(async move {
            let rows = query::<TStore::Database>(
                "SELECT request_name FROM dependent_records ORDER BY request_name",
            )
            .fetch_all(scope.executor())
            .await?;
            rows.iter()
                .map(|row| row.try_get("request_name").map_err(ProbeError::from))
                .collect()
        })
    }
}

struct ReadThenInsert;
impl<TStore> TransactionCallback<TStore, String> for ReadThenInsert
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
        mut scope: TStore::Scope<'scope>,
    ) -> BoxFuture<'scope, Result<String, ProbeError>> {
        Box::pin(async move {
            let rows = query::<TStore::Database>(
                "SELECT request_name FROM dependent_records WHERE request_name = 'held-prefix'",
            )
            .fetch_all(scope.executor())
            .await?;
            if !rows.is_empty() {
                return Err(ProbeError::Rejected);
            }
            crate::read_decide_insert(scope, "second-callback".into(), CallbackEnding::Commit).await
        })
    }
}

pub async fn run_cases<TStore>(storage: &TStore) -> Result<String, ProbeError>
where
    TStore: TransactionStore,
    <TStore::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TStore::Database>,
    for<'value> String: Type<TStore::Database>
        + Decode<'value, TStore::Database>
        + Encode<'value, TStore::Database>,
    for<'row> &'row str: ColumnIndex<<TStore::Database as Database>::Row>,
{
    let committed = storage
        .transaction(ReadDecideInsert {
            request_name: "committed".into(),
            ending: CallbackEnding::Commit,
        })
        .await?;
    if committed != "committed" {
        return Err(ProbeError::Rejected);
    }
    let callback_error = storage
        .transaction(ReadDecideInsert {
            request_name: "callback-prefix".into(),
            ending: CallbackEnding::CallbackError,
        })
        .await;
    if !matches!(callback_error, Err(ProbeError::Rejected)) {
        return Err(ProbeError::Rejected);
    }
    let sql_error = storage
        .transaction(ReadDecideInsert {
            request_name: "sql-prefix".into(),
            ending: CallbackEnding::SqlError,
        })
        .await;
    if !matches!(sql_error, Err(ProbeError::Storage(_))) {
        return Err(ProbeError::Rejected);
    }
    let after_errors = storage.transaction(ReadNames).await?;
    if after_errors != ["committed"] {
        return Err(ProbeError::Rejected);
    }

    let (entered_sender, entered) = futures_channel::oneshot::channel();
    let (release, released) = futures_channel::oneshot::channel();
    let first = storage.transaction(ReadDecideInsert {
        request_name: "held-prefix".into(),
        ending: CallbackEnding::PauseThenError {
            entered: entered_sender,
            release: released,
        },
    });
    pin_mut!(first);
    match select(first.as_mut(), entered).await {
        Either::Left(_) => return Err(ProbeError::Rejected),
        Either::Right((result, _)) => result.map_err(|_| ProbeError::Rejected)?,
    }
    let second = storage.transaction(ReadThenInsert);
    pin_mut!(second);
    let second_pending =
        poll_fn(|cx| Poll::Ready(matches!(second.as_mut().poll(cx), Poll::Pending))).await;
    if !second_pending {
        return Err(ProbeError::Rejected);
    }
    release.send(()).map_err(|_| ProbeError::Rejected)?;
    let (first_result, second_result) = join(first, second).await;
    if !matches!(first_result, Err(ProbeError::Rejected)) || second_result? != "second-callback" {
        return Err(ProbeError::Rejected);
    }
    let committed_rows = storage.transaction(ReadNames).await?;
    if committed_rows != ["committed", "second-callback"] {
        return Err(ProbeError::Rejected);
    }
    Ok(format!(
        "commit, callback rollback, SQL rollback, read-own-writes, exclusion passed; committed={committed_rows:?}"
    ))
}

pub async fn observe_interruption<TStore>(storage: &TStore) -> Result<String, ProbeError>
where
    TStore: TransactionStore,
    <TStore::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TStore::Database>,
    for<'value> String: Type<TStore::Database>
        + Decode<'value, TStore::Database>
        + Encode<'value, TStore::Database>,
    for<'row> &'row str: ColumnIndex<<TStore::Database as Database>::Row>,
{
    let (entered_sender, entered) = futures_channel::oneshot::channel();
    let (release, released) = futures_channel::oneshot::channel();
    let mut interrupted = Box::pin(storage.transaction(ReadDecideInsert {
        request_name: "cancel-prefix".into(),
        ending: CallbackEnding::PauseThenCommit {
            entered: entered_sender,
            release: released,
        },
    }));
    match select(interrupted.as_mut(), entered).await {
        Either::Left(_) => return Err(ProbeError::Rejected),
        Either::Right((result, _)) => result.map_err(|_| ProbeError::Rejected)?,
    }
    drop(interrupted);
    let callback_still_alive = release.send(()).is_ok();
    let rows = storage.transaction(ReadNames).await?;
    let committed = rows.iter().any(|name| name == "cancel-prefix");
    Ok(format!(
        "dropped transaction future: callback_still_alive={callback_still_alive}, cancel_prefix_committed={committed}, rows={rows:?}"
    ))
}
