//! Initialize consumer-local tables without admitting them to replication.
use crate::{
    ReceiverError, ReceiverRules, StoreError, checked_queries,
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
    H: ReceiverRules<S>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Vec<u8>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        let migrations = hook.local_migrations();
        adopt_legacy_hook_migrations(scope, migrations).await?;
        let applied = checked_queries::read_hook_migrations(scope)
            .await
            .map_err(crate::sql_transport::sql_error)?
            .into_iter()
            .map(|row| {
                Ok((
                    stored_version(row.version, row.version_class, "integer")?,
                    {
                        if row.checksum_class.as_deref() != Some("blob") {
                            return Err(corrupt("hook migration checksum"));
                        }
                        if row.success_class.as_deref() != Some("integer")
                            || row.success.as_deref() != Some("1")
                        {
                            return Err(corrupt("hook migration success"));
                        }
                        row.checksum
                            .ok_or_else(|| corrupt("hook migration checksum"))?
                    },
                ))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let replicated = SchemaMembership(scope).names().await?;
        for row in &applied {
            let version = row.0;
            let migration = migrations
                .iter()
                .find(|migration| {
                    migration.version == version && migration.migration_type.is_up_migration()
                })
                .ok_or_else(|| invalid_schema("applied hook migration missing from consumer"))?;
            if migration.checksum.as_ref() != row.1 {
                return Err(invalid_schema("hook migration checksum changed"));
            }
        }
        let mut previous = None;
        let highest = applied.iter().map(|row| row.0).max();
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
            if applied.iter().any(|row| row.0 == migration.version) {
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
                migration.description.to_string(),
                migration.checksum.to_vec(),
                "-1".into(),
            )
            .await
            .map_err(crate::sql_transport::sql_error)?;
        }
        Ok(())
    }
}

fn corrupt(field: &str) -> StoreError {
    StoreError::Corrupt {
        field: field.into(),
    }
}

fn stored_version(
    value: Option<String>,
    class: Option<String>,
    expected: &str,
) -> Result<i64, StoreError> {
    if class.as_deref() != Some(expected) {
        return Err(corrupt("hook migration version"));
    }
    value
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| corrupt("hook migration version"))
}

async fn adopt_legacy_hook_migrations<S>(
    scope: &mut S,
    migrations: &[sqlx_core::migrate::Migration],
) -> Result<(), ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Vec<u8>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    if !checked_queries::schema_object_exists(scope, "_fleet_sync_legacy_hook_migrations".into())
        .await
        .map_err(crate::sql_transport::sql_error)?
    {
        return Ok(());
    }
    let legacy = checked_queries::read_legacy_hook_migrations(scope)
        .await
        .map_err(crate::sql_transport::sql_error)?;
    if !checked_queries::read_hook_migrations(scope)
        .await
        .map_err(crate::sql_transport::sql_error)?
        .is_empty()
    {
        return Err(invalid_schema(
            "both legacy and standard hook migration histories exist",
        ));
    }
    let mut adopted = Vec::new();
    for row in legacy {
        let version = stored_version(row.version, row.version_class, "text")?;
        let encoded = crate::schema_reads::required_text(
            row.checksum,
            row.checksum_class,
            "hook migration checksum",
        )?;
        let checksum: Vec<u8> =
            serde_json::from_str(&encoded).map_err(|_| corrupt("hook migration checksum"))?;
        let migration = migrations
            .iter()
            .find(|migration| {
                migration.version == version && migration.migration_type.is_up_migration()
            })
            .ok_or_else(|| invalid_schema("applied hook migration missing from consumer"))?;
        if migration.checksum.as_ref() != checksum {
            return Err(invalid_schema("hook migration checksum changed"));
        }
        if adopted
            .iter()
            .any(|migration: &&sqlx_core::migrate::Migration| migration.version == version)
        {
            return Err(corrupt("hook migration version").into());
        }
        adopted.push(migration);
    }
    for migration in adopted {
        checked_queries::write_hook_migration(
            scope,
            migration.version.to_string(),
            migration.description.to_string(),
            migration.checksum.to_vec(),
            "-1".into(),
        )
        .await
        .map_err(crate::sql_transport::sql_error)?;
    }
    checked_queries::drop_legacy_hook_migrations(scope)
        .await
        .map_err(crate::sql_transport::sql_error)?;
    Ok(())
}
