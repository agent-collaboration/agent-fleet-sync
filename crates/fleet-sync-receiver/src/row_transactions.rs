use crate::{
    ReceiverError, StoreError, checked_queries, copy_storage::encode_record,
    row_apply::plan_row_change, schema_reads::SchemaReads,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{ReplicatedTransaction, RowChange, SchemaIdentity};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchemaStepRecord {
    pub version: u64,
    pub statements: Vec<String>,
}

pub(crate) struct RowTransactions<'scope, S>(pub &'scope mut S);

impl<S> RowTransactions<'_, S>
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
    pub(crate) async fn apply(
        &mut self,
        schema: &mut SchemaIdentity,
        transactions: &[ReplicatedTransaction],
    ) -> Result<(), ReceiverError> {
        for transaction in transactions {
            match transaction {
                ReplicatedTransaction::Rows { rows, .. } => {
                    for row in rows {
                        let table = match row {
                            RowChange::Upsert { table, .. } | RowChange::Delete { table, .. } => {
                                table
                            }
                        };
                        let columns = SchemaReads(self.0).table_columns(table).await?;
                        plan_row_change(&columns, row)?.execute(self.0).await?;
                    }
                }
                ReplicatedTransaction::SchemaStep {
                    migration_version,
                    statements,
                    ..
                } => {
                    if *migration_version <= schema.version {
                        return Err(ReceiverError::InvalidInput {
                            field: "schema version must advance".into(),
                        });
                    }
                    crate::schema_membership::SchemaMembership(self.0)
                        .apply(statements)
                        .await?;
                    validate_live_keys(self.0).await?;
                    let record = SchemaStepRecord {
                        version: *migration_version,
                        statements: statements.clone(),
                    };
                    checked_queries::write_schema_step(
                        self.0,
                        migration_version.to_string(),
                        encode_record(&record)?,
                    )
                    .await
                    .map_err(crate::sql_transport::sql_error)?;
                    *schema = SchemaIdentity::new(
                        *migration_version,
                        SchemaReads(self.0).fingerprint().await?,
                    )
                    .map_err(|_| StoreError::Corrupt {
                        field: "schema fingerprint".into(),
                    })?;
                }
            }
        }
        Ok(())
    }
}

pub(crate) async fn validate_live_keys<S>(scope: &mut S) -> Result<(), ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'a> String: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Option<String>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> f64: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Vec<u8>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    for table in SchemaReads(scope).table_names().await? {
        let columns = SchemaReads(scope).table_columns(&table).await?;
        let keys = crate::row_apply::primary_columns(&columns)
            .into_iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let projection = keys
            .iter()
            .map(|key| crate::bound_sql::quoted_identifier(key))
            .collect::<Vec<_>>()
            .join(",");
        let mut after = Vec::new();
        loop {
            let predicate = if after.is_empty() {
                String::new()
            } else {
                format!(
                    " WHERE ({projection}) > ({})",
                    keys.iter().map(|_| "?").collect::<Vec<_>>().join(",")
                )
            };
            let rows = crate::BoundStatement::query(
                format!(
                    "SELECT {projection} FROM {}{predicate} ORDER BY {projection} LIMIT 256",
                    crate::bound_sql::quoted_identifier(&table)
                ),
                after.clone(),
                keys.clone(),
            )
            .fetch_rows(scope)
            .await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                after = keys
                    .iter()
                    .map(|key| row.value(key).cloned())
                    .collect::<Result<Vec<_>, _>>()?;
                if after.iter().any(|value|!matches!(value,fleet_sync_protocol::SqlValue::Text(key) if fleet_sync_protocol::canonical_uuid_v7(key))) {
                    return Err(StoreError::Corrupt {field:"schema step UUIDv7 row key".into()}.into());
                }
            }
        }
    }
    Ok(())
}
