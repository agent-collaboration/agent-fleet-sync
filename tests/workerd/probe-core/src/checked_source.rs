// Generated from SQLx describe, schema.sql and queries/source.sql.
use super::{ProbeError, StoredSource, TransactionScope};
use sqlx_core::{
    column::ColumnIndex, database::Database, decode::Decode, query::query, row::Row, types::Type,
};
pub(super) async fn fetch_source<TScope>(scope: &mut TScope) -> Result<StoredSource, ProbeError>
where
    TScope: TransactionScope,
    <TScope::Database as Database>::Arguments:
        sqlx_core::arguments::IntoArguments<TScope::Database>,
    for<'value> Option<String>: Type<TScope::Database> + Decode<'value, TScope::Database>,
    for<'row> &'row str: ColumnIndex<<TScope::Database as Database>::Row>,
{
    let row = query::<TScope::Database>(include_str!("../../queries/source.sql"))
        .fetch_one(scope.executor())
        .await?;
    Ok(StoredSource {
        source_name: row.try_get::<Option<String>, _>("source_name")?,
    })
}
