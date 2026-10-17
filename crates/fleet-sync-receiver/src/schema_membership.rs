//! Keep schema ownership in the same transaction as each replicated migration.
use crate::{ReceiverError, StoreError, checked_queries};
use durable_steps::storage::TransactionScope;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::collections::BTreeSet;
use turso_parser::ast::{AlterTableBody, Cmd, QualifiedName, Stmt};

pub(crate) fn invalid_schema(detail: impl ToString) -> ReceiverError {
    ReceiverError::InvalidInput {
        field: detail.to_string(),
    }
}

pub(crate) fn statements(sql: &str) -> Result<Vec<(String, Cmd)>, ReceiverError> {
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let mut start = 0;
    let mut result = Vec::new();
    while let Some(command) = parser.next_cmd().map_err(invalid_schema)? {
        fleet_sync_protocol::admit_schema_command(&command).map_err(invalid_schema)?;
        let end = parser.offset();
        let statement = sql
            .get(start..end)
            .ok_or_else(|| invalid_schema("schema statement bounds"))?;
        result.push((statement.to_owned(), command));
        start = end;
    }
    Ok(result)
}

fn name(name: &QualifiedName) -> Result<String, ReceiverError> {
    if name
        .db_name
        .as_ref()
        .is_some_and(|schema| !schema.as_str().eq_ignore_ascii_case("main"))
    {
        return Err(invalid_schema("schema outside the copy file"));
    }
    Ok(name.name.as_str().to_ascii_lowercase())
}

pub(crate) struct SchemaMembership<'scope, S>(pub &'scope mut S);
impl<S> SchemaMembership<'_, S>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub(crate) async fn names(&mut self) -> Result<BTreeSet<String>, ReceiverError> {
        checked_queries::read_copy_tables(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?
            .into_iter()
            .map(|row| {
                let name = crate::schema_reads::required_text(
                    row.name,
                    row.name_class,
                    "replicated schema member",
                )?;
                if fleet_sync_protocol::TableName::new(&name).is_err()
                    || fleet_sync_protocol::reserved_namespace(&name)
                {
                    return Err(StoreError::Corrupt {
                        field: "replicated schema membership".into(),
                    }
                    .into());
                }
                Ok(name.to_ascii_lowercase())
            })
            .collect()
    }

    pub(crate) async fn object_owner(
        &mut self,
        name: &str,
    ) -> Result<Option<String>, ReceiverError> {
        checked_queries::read_schema_owner(self.0, name.to_owned())
            .await
            .map_err(crate::sql_transport::sql_error)?
            .map(|row| {
                crate::schema_reads::required_text(
                    row.owner,
                    row.owner_class,
                    "schema object owner",
                )
                .map(|owner| owner.to_ascii_lowercase())
                .map_err(ReceiverError::from)
            })
            .transpose()
    }

    pub(crate) async fn apply(&mut self, sql: &[String]) -> Result<(), ReceiverError> {
        let mut members = self.names().await?;
        for sql in sql {
            let mut parsed = statements(sql)?;
            if parsed.len() != 1 {
                return Err(invalid_schema("expected one schema statement"));
            }
            let (_, command) = parsed
                .pop()
                .ok_or_else(|| invalid_schema("missing schema statement"))?;
            let Cmd::Stmt(statement) = command else {
                return Err(invalid_schema("expected schema statement"));
            };
            let mut add = None;
            let mut remove = None;
            match &statement {
                Stmt::CreateTable {
                    tbl_name,
                    temporary,
                    ..
                }
                | Stmt::CreateView {
                    view_name: tbl_name,
                    temporary,
                    ..
                } => {
                    if *temporary {
                        return Err(invalid_schema("temporary schema cannot replicate"));
                    }
                    let target = name(tbl_name)?;
                    self.check_create(&members, &target).await?;
                    add = Some(target);
                }
                Stmt::DropTable { tbl_name, .. }
                | Stmt::DropView {
                    view_name: tbl_name,
                    ..
                } => {
                    let target = name(tbl_name)?;
                    require_member(&members, &target)?;
                    remove = Some(target);
                }
                Stmt::AlterTable(alter) => {
                    let target = name(&alter.name)?;
                    require_member(&members, &target)?;
                    if let AlterTableBody::RenameTo(next) = &alter.body {
                        let next = next.as_str().to_ascii_lowercase();
                        self.check_create(&members, &next).await?;
                        remove = Some(target);
                        add = Some(next);
                    }
                }
                Stmt::Insert { tbl_name, .. } | Stmt::Delete { tbl_name, .. } => {
                    require_member(&members, &name(tbl_name)?)?
                }
                Stmt::Update(update) => require_member(&members, &name(&update.tbl_name)?)?,
                Stmt::CreateIndex { tbl_name, .. } => {
                    require_member(&members, &tbl_name.as_str().to_ascii_lowercase())?
                }
                Stmt::DropIndex { idx_name, .. } => {
                    if let Some(owner) = self.object_owner(&name(idx_name)?).await? {
                        require_member(&members, &owner)?;
                    }
                }
                _ => return Err(invalid_schema("unsupported replicated schema statement")),
            }
            sqlx_core::raw_sql::raw_sql(sqlx_core::sql_str::AssertSqlSafe(sql.as_str()))
                .execute(self.0.executor())
                .await
                .map_err(crate::sql_transport::sql_error)?;
            if let Some(target) = remove {
                checked_queries::drop_copy_table(self.0, target.clone())
                    .await
                    .map_err(crate::sql_transport::sql_error)?;
                members.remove(&target);
            }
            if let Some(target) = add {
                checked_queries::write_copy_table(self.0, target.clone())
                    .await
                    .map_err(crate::sql_transport::sql_error)?;
                members.insert(target);
            }
        }
        Ok(())
    }

    async fn check_create(
        &mut self,
        members: &BTreeSet<String>,
        target: &str,
    ) -> Result<(), ReceiverError> {
        if !members.contains(target)
            && checked_queries::schema_object_exists(self.0, target.to_owned())
                .await
                .map_err(crate::sql_transport::sql_error)?
        {
            return Err(invalid_schema(format!(
                "replicated schema conflicts with local object {target}"
            )));
        }
        Ok(())
    }
}

fn require_member(members: &BTreeSet<String>, target: &str) -> Result<(), ReceiverError> {
    if !members.contains(target) {
        return Err(invalid_schema(format!(
            "schema table is not replicated: {target}"
        )));
    }
    Ok(())
}
