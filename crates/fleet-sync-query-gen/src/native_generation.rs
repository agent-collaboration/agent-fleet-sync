use crate::{
    ColumnMetadata, ColumnTypeSource, CorpusMetadata, GenerateError, QueryMetadata, QueryResult,
    expression_classes::expression_classes,
    query_render::render_queries,
    source_freshness::digest_query_descriptions,
    source_inputs::{SourceInputs, digest_bytes},
    source_manifest::{read_manifest, validate_identifier},
};
use sqlx_core::{
    column::Column,
    connection::{ConnectOptions, Connection},
    executor::Executor,
    sql_str::{AssertSqlSafe, SqlSafeStr},
    type_info::TypeInfo,
};
use sqlx_turso::TursoConnectOptions;
use std::{collections::BTreeSet, fs, path::Path};
use turso_parser::{
    ast::fmt::{BlankContext, ToTokens, TokenStream},
    parser::Parser,
    token::TokenType,
};

struct BindSlots {
    highest: usize,
}
impl TokenStream for BindSlots {
    type Error = GenerateError;
    fn append(&mut self, kind: TokenType, value: Option<&str>) -> Result<(), Self::Error> {
        if kind == TokenType::TK_VARIABLE {
            let slot = value
                .and_then(|value| value.strip_prefix('?'))
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|slot| *slot > 0)
                .ok_or_else(|| {
                    GenerateError::Manifest("only positional SQL bind slots are supported".into())
                })?;
            self.highest = self.highest.max(slot);
        }
        Ok(())
    }
}
pub(crate) fn bind_count(sql: &str) -> Result<usize, GenerateError> {
    let mut parser = Parser::new(sql.as_bytes());
    let command = parser
        .next_cmd()
        .map_err(|error| GenerateError::Sql(error.to_string()))?
        .ok_or_else(|| GenerateError::Sql("empty query".into()))?;
    if parser
        .next_cmd()
        .map_err(|error| GenerateError::Sql(error.to_string()))?
        .is_some()
    {
        return Err(GenerateError::Sql(
            "expected exactly one SQL statement".into(),
        ));
    }
    let mut slots = BindSlots { highest: 0 };
    command.to_tokens(&mut slots, &BlankContext)?;
    Ok(slots.highest)
}

/// Regenerate, or compare without writing when `check` is true. Exact statements
/// are described by the owned driver against the complete migrated schema.
pub async fn generate_manifest(path: impl AsRef<Path>, check: bool) -> Result<(), GenerateError> {
    let path = path.as_ref();
    let root = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let manifest = read_manifest(path)?;
    let mut connection = TursoConnectOptions::new()
        .connect()
        .await
        .map_err(|error| GenerateError::Sql(error.to_string()))?;
    let mut migrations = Vec::new();
    let mut versions = BTreeSet::new();
    for directory in &manifest.migrations {
        let migrator = sqlx_core::migrate::Migrator::new(root.join(directory).as_path())
            .await
            .map_err(|error| GenerateError::Sql(error.to_string()))?;
        for migration in migrator.iter() {
            if !versions.insert(migration.version) {
                return Err(GenerateError::Manifest(format!(
                    "duplicate migration version {}",
                    migration.version
                )));
            }
            migrations.push(migration.clone());
        }
    }
    migrations.sort_by_key(|migration| migration.version);
    let migrator = sqlx_core::migrate::Migrator::with_migrations(migrations);
    let mut transaction = connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|error| GenerateError::Sql(error.to_string()))?;
    migrator
        .run_direct(None, &mut *transaction, false)
        .await
        .map_err(|error| GenerateError::Sql(error.to_string()))?;
    transaction
        .commit()
        .await
        .map_err(|error| GenerateError::Sql(error.to_string()))?;
    let mut queries = Vec::new();
    for query in &manifest.queries {
        let sql = fs::read_to_string(root.join(&query.sql))?;
        let expression_classes = expression_classes(&sql)?;
        let actual = bind_count(&sql)?;
        if actual != query.parameters.len() {
            return Err(GenerateError::BindCount {
                query: query.name.clone(),
                expected: query.parameters.len(),
                actual,
            });
        }
        connection
            .prepare(AssertSqlSafe(sql.clone()).into_sql_str())
            .await
            .map_err(|error| GenerateError::Sql(error.to_string()))?;
        let description = connection
            .describe(AssertSqlSafe(sql).into_sql_str())
            .await
            .map_err(|error| GenerateError::Sql(error.to_string()))?;
        if query.result != QueryResult::Exists
            && description.columns().is_empty() != (query.result == QueryResult::Execute)
        {
            return Err(GenerateError::Manifest(format!(
                "{} result mode disagrees with its described columns",
                query.name
            )));
        }
        let mut columns = Vec::new();
        let mut column_names = BTreeSet::new();
        for (index, column) in description.columns().iter().enumerate() {
            // Existence calls intentionally decode no result metadata or values.
            if query.result == QueryResult::Exists {
                break;
            }
            validate_identifier(column.name())?;
            if !column_names.insert(column.name()) {
                return Err(GenerateError::Manifest(format!(
                    "{} has duplicate output column {}",
                    query.name,
                    column.name()
                )));
            }
            let described_type = column.type_info().name();
            let (sql_type, type_source) =
                if matches!(described_type, "TEXT" | "INTEGER" | "REAL" | "BLOB") {
                    (described_type, ColumnTypeSource::Driver)
                } else if let Some((class, source)) = expression_classes.get(column.name()) {
                    (*class, *source)
                } else {
                    return Err(GenerateError::UnsupportedType {
                        query: query.name.clone(),
                        column: column.name().into(),
                        sql_type: described_type.into(),
                    });
                };
            let base_type = match sql_type {
                "TEXT" => "String",
                "INTEGER" => "i64",
                "REAL" => "f64",
                "BLOB" => "Vec<u8>",
                _ => {
                    return Err(GenerateError::UnsupportedType {
                        query: query.name.clone(),
                        column: column.name().into(),
                        sql_type: sql_type.into(),
                    });
                }
            };
            let nullable = description.nullable(index);
            let rust_type = if nullable == Some(false) {
                base_type.into()
            } else {
                format!("Option<{base_type}>")
            };
            columns.push(ColumnMetadata {
                name: column.name().into(),
                sql_type: sql_type.into(),
                rust_type,
                nullable,
                type_source,
            });
        }
        queries.push(QueryMetadata {
            name: query.name.clone(),
            bind_slots: actual,
            columns,
        });
    }
    let generated = render_queries(&manifest, &queries)?;
    let mut sources = SourceInputs::collect(path, &manifest)?;
    sources.add_output(&manifest.output, digest_bytes(generated.as_bytes()))?;
    let metadata = serde_json::to_string_pretty(&CorpusMetadata {
        producer: "owned sqlx-turso / Turso 0.8.1 describe".into(),
        sources: sources.into_hashes(),
        queries_digest: digest_query_descriptions(&queries)?,
        queries,
    })? + "\n";
    for (relative, content) in [
        (&manifest.output, generated),
        (&manifest.metadata, metadata),
    ] {
        let target = root.join(relative);
        if check {
            if fs::read_to_string(&target).ok().as_deref() != Some(content.as_str()) {
                return Err(GenerateError::Stale(relative.clone()));
            }
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(target, content)?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    #[test]
    fn bind_slots_follow_the_parser_not_question_marks_in_literals_or_comments()
    -> Result<(), GenerateError> {
        assert_eq!(
            bind_count("SELECT '?' AS literal, ? AS first, ?2 AS second -- ?99")?,
            2
        );
        assert_eq!(bind_count("SELECT ?1, ?1")?, 1);
        assert!(bind_count("SELECT :named").is_err());
        assert!(bind_count("SELECT 1; SELECT 2").is_err());
        Ok(())
    }
}
