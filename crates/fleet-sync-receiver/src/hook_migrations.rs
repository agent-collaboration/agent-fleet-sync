//! Initialize consumer-local tables without admitting them to replication.
use crate::{
    ReceiverError, ReceiverHook, StoreError, checked_queries,
    schema_membership::{SchemaMembership, invalid_schema, statements},
};
use durable_steps::storage::TransactionScope;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use turso_parser::ast::{Cmd, Stmt};

/// Run the hook's ordered migrations once in the host's copy-initialization transaction.
/// The host must roll back any error and call this before applying pieces.
#[allow(clippy::manual_async_fn)]
pub fn initialize_hook<'scope, S, H>(
    scope: &'scope mut S,
    hook: &'scope H,
) -> impl std::future::Future<Output = Result<(), ReceiverError>> + Send + 'scope
where
    S: TransactionScope + 'scope,
    H: ReceiverHook<S>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        let migrations = hook.local_migrations();
        let applied = checked_queries::read_hook_migrations(scope)
            .await
            .map_err(crate::sql_transport::sql_error)?
            .into_iter()
            .map(|row| {
                Ok((
                    crate::schema_reads::required_text(
                        row.version,
                        row.version_class,
                        "hook migration version",
                    )?,
                    crate::schema_reads::required_text(
                        row.checksum,
                        row.checksum_class,
                        "hook migration checksum",
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let replicated = SchemaMembership(scope).names().await?;
        for row in &applied {
            let version = row.0.parse::<i64>().map_err(|_| StoreError::Corrupt {
                field: "hook migration version".into(),
            })?;
            let migration = migrations
                .iter()
                .find(|migration| {
                    migration.version == version && migration.migration_type.is_up_migration()
                })
                .ok_or_else(|| invalid_schema("applied hook migration missing from consumer"))?;
            if checksum(migration)? != row.1 {
                return Err(invalid_schema("hook migration checksum changed"));
            }
        }
        let mut previous = None;
        let highest = applied
            .iter()
            .filter_map(|row| row.0.parse::<i64>().ok())
            .max();
        for migration in migrations
            .iter()
            .filter(|migration| migration.migration_type.is_up_migration())
        {
            if migration.no_tx || previous.is_some_and(|version| migration.version <= version) {
                return Err(invalid_schema(
                    "hook migrations must be transactional and ordered",
                ));
            }
            previous = Some(migration.version);
            if applied
                .iter()
                .any(|row| row.0 == migration.version.to_string())
            {
                continue;
            }
            if highest.is_some_and(|version| migration.version <= version) {
                return Err(invalid_schema("hook migration added below applied version"));
            }
            for (sql, command) in statements(migration.sql.as_str())? {
                let Cmd::Stmt(statement) = command else {
                    return Err(invalid_schema("expected local schema statement"));
                };
                let target = match &statement {
                    Stmt::CreateTable {
                        tbl_name,
                        temporary: false,
                        ..
                    }
                    | Stmt::DropTable { tbl_name, .. }
                    | Stmt::Insert { tbl_name, .. }
                    | Stmt::Delete { tbl_name, .. } => Some(tbl_name),
                    Stmt::AlterTable(alter) => {
                        if let turso_parser::ast::AlterTableBody::RenameTo(next) = &alter.body
                            && replicated.contains(&next.as_str().to_ascii_lowercase())
                        {
                            return Err(invalid_schema(
                                "hook migration renames a local table to a replicated name",
                            ));
                        }
                        Some(&alter.name)
                    }
                    Stmt::Update(update) => Some(&update.tbl_name),
                    Stmt::CreateIndex {
                        idx_name, tbl_name, ..
                    } => {
                        if idx_name
                            .db_name
                            .as_ref()
                            .is_some_and(|schema| !schema.as_str().eq_ignore_ascii_case("main"))
                        {
                            return Err(invalid_schema("hook index is outside the copy file"));
                        }
                        if replicated.contains(&tbl_name.as_str().to_ascii_lowercase()) {
                            return Err(invalid_schema(
                                "hook migration changes a replicated table",
                            ));
                        }
                        None
                    }
                    Stmt::DropIndex { idx_name, .. } => {
                        if idx_name
                            .db_name
                            .as_ref()
                            .is_some_and(|schema| !schema.as_str().eq_ignore_ascii_case("main"))
                        {
                            return Err(invalid_schema("hook index is outside the copy file"));
                        }
                        if let Some(owner) = SchemaMembership(scope)
                            .object_owner(idx_name.name.as_str())
                            .await?
                            && replicated.contains(&owner)
                        {
                            return Err(invalid_schema(
                                "hook migration changes a replicated table",
                            ));
                        }
                        None
                    }
                    _ => {
                        return Err(invalid_schema(
                            "hook migration must change local tables only",
                        ));
                    }
                };
                if let Some(target) = target
                    && (replicated.contains(&target.name.as_str().to_ascii_lowercase())
                        || target
                            .db_name
                            .as_ref()
                            .is_some_and(|schema| !schema.as_str().eq_ignore_ascii_case("main")))
                {
                    return Err(invalid_schema(
                        "hook migration changes a replicated table or another file",
                    ));
                }
                sqlx_core::raw_sql::raw_sql(sqlx_core::sql_str::AssertSqlSafe(sql.as_str()))
                    .execute(scope.executor())
                    .await
                    .map_err(crate::sql_transport::sql_error)?;
            }
            checked_queries::write_hook_migration(
                scope,
                migration.version.to_string(),
                checksum(migration)?,
            )
            .await
            .map_err(crate::sql_transport::sql_error)?;
        }
        Ok(())
    }
}

fn checksum(migration: &sqlx_core::migrate::Migration) -> Result<String, ReceiverError> {
    serde_json::to_string(migration.checksum.as_ref())
        .map_err(|_| invalid_schema("hook migration checksum encoding"))
}
