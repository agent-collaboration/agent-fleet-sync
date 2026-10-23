//! Adapt pure SQL ownership refusals to the native writer's existing errors.
use crate::{OpenError, SchemaForm, WriteRefusal};
use fleet_sync_protocol::schema_catalog::MigrationError;
pub(crate) use fleet_sync_protocol::schema_catalog::MigrationKind;

pub(crate) fn writer_error(error: MigrationError) -> OpenError {
    let form = match error {
        MigrationError::InvalidCatalog { field } => return OpenError::corrupt(field),
        MigrationError::KeyChange { table } => {
            return OpenError::WriteRefused {
                refusal: WriteRefusal::KeyChange { table },
            };
        }
        MigrationError::InvalidSql { statement, message } => SchemaForm::InvalidMigration {
            statement,
            detail: crate::StatementRefusal {
                form: crate::StatementForm::InvalidSql { message },
            }
            .to_string(),
        },
        MigrationError::KeyNotUuidV7 { table, column } => {
            SchemaForm::KeyNotUuidV7 { table, column }
        }
        MigrationError::UndeclaredTable { table } => SchemaForm::UndeclaredTable { table },
        MigrationError::ReplicatedTableInLocalMigration { table } => {
            SchemaForm::ReplicatedTableInLocalMigration { table }
        }
        MigrationError::InvalidMigration { statement, detail } => {
            SchemaForm::InvalidMigration { statement, detail }
        }
        MigrationError::NonDeterministicMigration {
            statement,
            function,
        } => SchemaForm::NonDeterministicMigration {
            statement,
            function,
        },
    };
    OpenError::SchemaUnsupported { form }
}

#[cfg(test)]
use fleet_sync_protocol::{SchemaEntry, SchemaObjectKind, TableName};
#[cfg(test)]
#[path = "migration_ownership_tests.rs"]
mod tests;
