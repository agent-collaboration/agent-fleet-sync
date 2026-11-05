//! Bootstrap metadata is validated before either adapter admits journal work.
use crate::{StoreError, sql_rows::corrupt};

#[derive(sqlx::FromRow)]
pub(crate) struct JournalMetadataRow {
    pub version: i64,
    pub id: Option<String>,
}

pub(crate) fn validate_metadata_ids(
    table: &str,
    rows: &[JournalMetadataRow],
) -> Result<(), StoreError> {
    for row in rows {
        if !row
            .id
            .as_deref()
            .is_some_and(runtime_primitives::canonical_uuid_v7)
        {
            return Err(corrupt(
                &format!("{table}.id"),
                format!("version {} has an invalid UUIDv7 row id", row.version),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_schema(rows: &[JournalMetadataRow]) -> Result<(), StoreError> {
    if rows.len() != 1 || rows.first().is_none_or(|row| row.version != 2) {
        return Err(corrupt("schema_version", "unsupported journal schema"));
    }
    validate_metadata_ids("_fleet_sync_schema", rows)
}
