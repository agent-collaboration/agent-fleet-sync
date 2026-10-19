//! Compare receiver bookkeeping with its own migration corpus, without migrating.
use crate::{StoreError, checked_queries, schema_reads::required_text};
use durable_steps::storage::TransactionScope;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::collections::{BTreeMap, BTreeSet};
use turso_parser::{
    ast::{Cmd, Stmt},
    parser::Parser,
};

fn corrupt(field: impl Into<String>) -> StoreError {
    StoreError::Corrupt {
        field: field.into(),
    }
}
fn parse_table_definition(sql: &str) -> Result<Cmd, StoreError> {
    let mut parser = Parser::new(sql.as_bytes());
    let command = parser
        .next_cmd()
        .map_err(|_| corrupt("receiver table definition"))?
        .ok_or_else(|| corrupt("missing receiver table definition"))?;
    if parser
        .next_cmd()
        .map_err(|_| corrupt("receiver table definition"))?
        .is_some()
    {
        return Err(corrupt("multiple receiver table definitions"));
    }
    Ok(command)
}
fn declared_tables() -> Result<BTreeMap<String, Cmd>, StoreError> {
    let mut tables = BTreeMap::new();
    for migration in crate::RECEIVER_MIGRATOR.iter() {
        let sql = migration.sql.as_str();
        let mut parser = Parser::new(sql.as_bytes());
        while let Some(command) = parser
            .next_cmd()
            .map_err(|_| corrupt("receiver migration corpus"))?
        {
            let Cmd::Stmt(Stmt::CreateTable { tbl_name, .. }) = &command else {
                return Err(corrupt(
                    "receiver corpus must declare its current table definitions",
                ));
            };
            let name = tbl_name.name.as_str().to_ascii_lowercase();
            if tables.insert(name, command).is_some() {
                return Err(corrupt("duplicate receiver corpus table"));
            }
        }
    }
    if tables.is_empty() {
        return Err(corrupt("empty receiver corpus"));
    }
    Ok(tables)
}

pub(crate) async fn current_corpus_present<S>(scope: &mut S) -> Result<bool, StoreError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let expected = declared_tables()?;
    let mut present = BTreeSet::new();
    for row in checked_queries::read_inspection_corpus(scope)
        .await
        .map_err(crate::sql_transport::sql_error)?
    {
        let name = required_text(row.object_name, row.name_class, "receiver catalog name")?
            .to_ascii_lowercase();
        let Some(expected_definition) = expected.get(&name) else {
            continue;
        };
        let kind = required_text(row.object_kind, row.kind_class, "receiver catalog kind")?;
        if kind != "table" {
            return Err(corrupt(format!("receiver object {name} is not a table")));
        }
        let sql = required_text(row.statement, row.statement_class, "receiver table SQL")?;
        if &parse_table_definition(&sql)? != expected_definition {
            return Err(corrupt(format!(
                "receiver table {name} differs from the current corpus"
            )));
        }
        if !present.insert(name) {
            return Err(corrupt("duplicate receiver catalog table"));
        }
    }
    if present.is_empty() {
        return Ok(false);
    }
    if present.len() != expected.len() {
        return Err(corrupt("partial receiver corpus"));
    }
    Ok(true)
}
