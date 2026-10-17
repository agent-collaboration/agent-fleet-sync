use sqlx_core::database::Database;

use crate::{
    DoArgumentValue, DoArguments, DoColumn, DoConnection, DoQueryResult, DoRow, DoStatement,
    DoTransactionManager, DoTypeInfo, DoValue, DoValueRef,
};

/// The Cloudflare Durable Object storage driver: the type sqlx code names in
/// `Executor<'_, Database = Do>`, `sqlx::query_as::<Do, _>` and so on.
#[derive(Debug)]
pub struct Do;

impl Database for Do {
    type Connection = DoConnection;

    type TransactionManager = DoTransactionManager;

    type Row = DoRow;

    type QueryResult = DoQueryResult;

    type Column = DoColumn;

    type TypeInfo = DoTypeInfo;

    type Value = DoValue;
    type ValueRef<'r> = DoValueRef<'r>;

    type Arguments = DoArguments;
    type ArgumentBuffer = Vec<DoArgumentValue>;

    type Statement = DoStatement;

    const NAME: &'static str = "Do";

    const URL_SCHEMES: &'static [&'static str] = &["do"];
}
