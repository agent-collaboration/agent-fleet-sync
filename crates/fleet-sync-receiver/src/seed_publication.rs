use crate::{
    BoundStatement, ReceiverError, StoreError, bound_sql::quoted_identifier, checked_queries,
    copy_storage::encode_record, row_apply::primary_columns, row_transactions::SchemaStepRecord,
    schema_reads::SchemaReads,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{SeedAnswer, SeedComplete, SeedPage, TableDigestBuilder, TableName};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) async fn publish_seed<S>(
    scope: &mut S,
    done: &SeedComplete,
    pages: &[SeedPage],
) -> Result<(), ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> f64: Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Vec<u8>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let first = pages.first().ok_or_else(|| ReceiverError::InvalidInput {
        field: "empty seed".into(),
    })?;
    for page in pages {
        if page.contents.database != done.database
            || page.contents.position != done.position
            || page.contents.schema != done.schema
            || page.contents.schema_steps != first.contents.schema_steps
        {
            return Err(ReceiverError::InvalidInput {
                field: "inconsistent seed page header".into(),
            });
        }
    }
    for sql in &first.contents.schema_steps {
        fleet_sync_protocol::admit_replicated_schema_statement(sql)
            .map_err(|error| ReceiverError::SchemaUnsupported { form: error.form })?;
    }
    crate::schema_membership::SchemaMembership(scope)
        .apply(&first.contents.schema_steps)
        .await?;
    if SchemaReads(scope).fingerprint().await? != done.schema.fingerprint {
        return Err(ReceiverError::SeedRefused {
            answer: SeedAnswer::SchemaMismatch,
        });
    }
    let tables = SchemaReads(scope).table_names().await?;
    let mut offered = done
        .table_digests
        .iter()
        .map(|(table, _)| table.as_str())
        .collect::<Vec<_>>();
    offered.sort();
    if offered != tables.iter().map(String::as_str).collect::<Vec<_>>() {
        return Err(ReceiverError::SeedRefused {
            answer: SeedAnswer::SchemaMismatch,
        });
    }
    // Data migrations can create rows; the snapshot replaces their complete contents.
    for table in &tables {
        BoundStatement::new(
            format!("DELETE FROM {}", quoted_identifier(table)),
            Vec::new(),
        )
        .execute(scope)
        .await?;
    }
    for page in pages {
        crate::snapshot_rows::insert_snapshot_rows(scope, &page.contents).await?;
    }
    for table in &tables {
        let columns = SchemaReads(scope).table_columns(table).await?;
        let column_names = columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let projection = columns
            .iter()
            .map(|column| quoted_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let keys = primary_columns(&columns)
            .iter()
            .map(|column| quoted_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let rows = BoundStatement::query(
            format!(
                "SELECT {projection} FROM {} ORDER BY {keys}",
                quoted_identifier(table)
            ),
            Vec::new(),
            column_names,
        )
        .fetch_rows(scope)
        .await?;
        let mut digest = TableDigestBuilder::new();
        for row in rows {
            let values = columns
                .iter()
                .map(|column| row.value(&column.name).cloned())
                .collect::<Result<Vec<_>, StoreError>>()?;
            digest
                .push_row(&values)
                .map_err(|_| ReceiverError::InvalidInput {
                    field: "seed table digest".into(),
                })?;
        }
        let computed = digest.finish();
        if !done
            .table_digests
            .iter()
            .any(|(name, offered)| name.as_str() == table && *offered == computed)
        {
            return Err(ReceiverError::SeedRefused {
                answer: SeedAnswer::DigestMismatch {
                    table: TableName::new(table).map_err(|_| ReceiverError::InvalidInput {
                        field: "seed table name".into(),
                    })?,
                },
            });
        }
    }
    let record = SchemaStepRecord {
        version: done.schema.version,
        statements: first.contents.schema_steps.clone(),
    };
    checked_queries::write_schema_step(
        scope,
        done.schema.version.to_string(),
        encode_record(&record)?,
    )
    .await
    .map_err(crate::sql_transport::sql_error)?;
    Ok(())
}
