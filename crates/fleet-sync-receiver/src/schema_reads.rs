use crate::{ReceiverError, StoreError, checked_queries, row_apply::TableColumn};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::schema_catalog::{current_replicated_catalog, snapshot_schema_statements};
use fleet_sync_protocol::{SchemaEntry, SchemaObjectKind, TableName, reserved_namespace};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) struct SchemaReads<'scope, S>(pub &'scope mut S);

pub(crate) fn required_text(
    value: Option<String>,
    class: Option<String>,
    field: &str,
) -> Result<String, StoreError> {
    if class.as_deref() != Some("text") {
        return Err(StoreError::Corrupt {
            field: field.into(),
        });
    }
    value.ok_or_else(|| StoreError::Corrupt {
        field: field.into(),
    })
}

impl<S> SchemaReads<'_, S>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub(crate) async fn snapshot_statements(&mut self) -> Result<Vec<String>, ReceiverError> {
        snapshot_schema_statements(self.entries().await?).map_err(|error| {
            ReceiverError::SnapshotSchema {
                encoded_size: error.encoded_size,
            }
        })
    }

    pub(crate) async fn table_columns(
        &mut self,
        table: &str,
    ) -> Result<Vec<TableColumn>, ReceiverError> {
        if reserved_namespace(table) {
            return Err(ReceiverError::InvalidInput {
                field: "replicated table is reserved".into(),
            });
        }
        if !crate::schema_membership::SchemaMembership(self.0)
            .names()
            .await?
            .contains(&table.to_ascii_lowercase())
        {
            return Err(ReceiverError::InvalidInput {
                field: "table is outside the replicated schema".into(),
            });
        }
        let rows = checked_queries::read_table_columns(self.0, table.to_owned())
            .await
            .map_err(crate::sql_transport::sql_error)?;
        let columns = rows
            .into_iter()
            .map(|row| {
                let name = required_text(row.column_name, row.column_class, "column name")?;
                if row.key_class.as_deref() != Some("integer") {
                    return Err(StoreError::Corrupt {
                        field: "primary key order".into(),
                    });
                }
                let key_order = row
                    .key_order
                    .and_then(|value| value.parse::<i64>().ok())
                    .filter(|order| *order >= 0)
                    .ok_or_else(|| StoreError::Corrupt {
                        field: "primary key order".into(),
                    })?;
                if key_order > 0
                    && (!required_text(row.declared_type, row.type_class, "replicated key type")?
                        .eq_ignore_ascii_case("TEXT")
                        || row.not_null_class.as_deref() != Some("integer")
                        || row.not_null.as_deref() != Some("1"))
                {
                    return Err(StoreError::Corrupt {
                        field: "replicated UUIDv7 key schema".into(),
                    });
                }
                Ok(TableColumn { name, key_order })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        if columns.is_empty() || !columns.iter().any(|column| column.key_order > 0) {
            return Err(StoreError::Corrupt {
                field: "replicated UUIDv7 key schema".into(),
            }
            .into());
        }
        Ok(columns)
    }

    pub(crate) async fn entries(&mut self) -> Result<Vec<SchemaEntry>, ReceiverError> {
        let rows = checked_queries::read_schema_entries(self.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        let catalog = rows
            .into_iter()
            .map(|row| {
                let kind = required_text(row.object_kind, row.kind_class, "schema object kind")?;
                let kind =
                    SchemaObjectKind::try_from(kind.as_str()).map_err(|_| StoreError::Corrupt {
                        field: "schema object kind".into(),
                    })?;
                Ok((
                    SchemaEntry {
                        kind,
                        name: required_text(row.object_name, row.name_class, "schema object name")?,
                        sql: required_text(row.statement, row.statement_class, "schema statement")?,
                    },
                    required_text(row.owner, row.owner_class, "schema object owner")?,
                ))
            })
            .collect::<Result<Vec<_>, ReceiverError>>()?;
        let members = crate::schema_membership::SchemaMembership(self.0)
            .names()
            .await?;
        // Membership includes views. Only physical member tables admit their
        // owned indexes; view dependencies go through the shared classifier.
        let tables = catalog
            .iter()
            .filter(|(entry, _)| {
                entry.kind == SchemaObjectKind::Table
                    && members.contains(&entry.name.to_ascii_lowercase())
            })
            .map(|(entry, _)| {
                TableName::new(&entry.name).map_err(|_| StoreError::Corrupt {
                    field: "replicated schema membership".into(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(current_replicated_catalog(&catalog, &tables))
    }

    pub(crate) async fn fingerprint(&mut self) -> Result<String, ReceiverError> {
        fleet_sync_protocol::schema_fingerprint(&self.entries().await?).map_err(|_| {
            ReceiverError::InvalidInput {
                field: "schema fingerprint".into(),
            }
        })
    }

    pub(crate) async fn table_names(&mut self) -> Result<Vec<String>, ReceiverError> {
        let mut names = self
            .entries()
            .await?
            .into_iter()
            .filter(|entry| {
                entry.kind == SchemaObjectKind::Table && !reserved_namespace(&entry.name)
            })
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        names.sort();
        Ok(names)
    }
}
