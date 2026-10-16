use std::{borrow::Cow, error::Error, fmt};

use sqlx_core::error::{DatabaseError, Error as SqlxError, ErrorKind};

use crate::value::TursoStorageClass;

/// Failures the driver itself detects, before or around the Turso engine
///
/// Configuration and usage variants surface as [`sqlx_core::error::Error::Configuration`];
/// decode variants surface as [`sqlx_core::error::Error::ColumnDecode`] through SQLx's decode
/// path. Callers match on the variant by downcasting the boxed source.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TursoAdapterError {
    /// The connection URL does not start with `turso:`
    #[error("Turso connection URLs must use the `turso:` scheme")]
    MissingTursoScheme,
    /// The connection URL path is not valid percent-encoded UTF-8
    #[error("the Turso connection URL path is not valid percent-encoded UTF-8")]
    InvalidUrlPath,
    /// The connection URL carries a parameter this driver does not accept
    #[error("unknown Turso connection URL parameter `{name}`")]
    UnknownUrlParameter {
        /// The rejected parameter name
        name: String,
    },
    /// A connection URL parameter has a value this driver does not accept
    #[error("unknown value {value:?} for Turso connection URL parameter `{name}`")]
    InvalidUrlParameterValue {
        /// The parameter name
        name: &'static str,
        /// The rejected value
        value: String,
    },
    /// Read-only opens need the lower Turso SDK, which this driver does not use
    #[error("read-only Turso connections are not supported")]
    ReadOnlyUnsupported,
    /// Arguments were bound to SQL that contains more than one statement
    #[error("arguments cannot be bound to multi-statement Turso SQL")]
    BatchArgumentsUnsupported,
    /// A named placeholder was used; only `?`, `?N` and `$N` are supported
    #[error("named placeholder `{placeholder}` is not supported; use `?`, `?N` or `$N`")]
    NamedPlaceholderUnsupported {
        /// The placeholder as written in the SQL
        placeholder: String,
    },
    /// `PRAGMA schema_version` did not return an integer, so cached statements cannot be
    /// checked against the current schema
    #[error("Turso did not report an integer schema version")]
    SchemaVersionUnreadable,
    /// A migrations table name is not a valid SQLite identifier path
    #[error("invalid SQLite identifier `{name}` for the migrations table")]
    InvalidMigrationTableName {
        /// The rejected table name
        name: String,
    },
    /// A value's storage class cannot decode into the requested Rust type
    #[error("cannot decode a Turso {actual} value as {expected}")]
    StorageClassMismatch {
        /// What the requested Rust type accepts
        expected: &'static str,
        /// The storage class the engine returned
        actual: TursoStorageClass,
    },
    /// Stored text does not parse as the requested date or time type
    #[error("invalid Turso {kind} text {value:?}")]
    InvalidTemporalText {
        /// The requested temporal kind
        kind: &'static str,
        /// The stored text
        value: String,
    },
    /// A stored number is outside the requested date or time type's range
    #[error("Turso {kind} value is out of range")]
    TemporalOutOfRange {
        /// The requested temporal kind
        kind: &'static str,
    },
}

impl From<TursoAdapterError> for SqlxError {
    fn from(error: TursoAdapterError) -> Self {
        SqlxError::Configuration(Box::new(error))
    }
}

/// Database error returned by the Turso engine
#[derive(Debug)]
pub struct TursoDatabaseError {
    code: &'static str,
    message: String,
}

impl TursoDatabaseError {
    pub(crate) fn from_turso(error: turso::Error) -> SqlxError {
        SqlxError::database(Self {
            code: turso_error_code(&error),
            message: error.to_string(),
        })
    }
}

impl fmt::Display for TursoDatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for TursoDatabaseError {}

impl DatabaseError for TursoDatabaseError {
    fn message(&self) -> &str {
        &self.message
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.code))
    }

    fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        sqlx_error_kind(self.code, &self.message)
    }
}

pub(crate) fn map_turso_error(error: turso::Error) -> SqlxError {
    TursoDatabaseError::from_turso(error)
}

fn turso_error_code(error: &turso::Error) -> &'static str {
    match error {
        turso::Error::ToSqlConversionFailure(_) => "TURSO_TO_SQL_CONVERSION_FAILURE",
        turso::Error::QueryReturnedNoRows => "TURSO_QUERY_RETURNED_NO_ROWS",
        turso::Error::ConversionFailure(_) => "TURSO_CONVERSION_FAILURE",
        turso::Error::Busy(_) => "SQLITE_BUSY",
        turso::Error::BusySnapshot(_) => "SQLITE_BUSY_SNAPSHOT",
        turso::Error::Interrupt(_) => "SQLITE_INTERRUPT",
        turso::Error::Error(_) => "SQLITE_ERROR",
        turso::Error::Misuse(_) => "SQLITE_MISUSE",
        turso::Error::Constraint(_) => "SQLITE_CONSTRAINT",
        turso::Error::Readonly(_) => "SQLITE_READONLY",
        turso::Error::DatabaseFull(_) => "SQLITE_FULL",
        turso::Error::NotAdb(_) => "SQLITE_NOTADB",
        turso::Error::Corrupt(_) => "SQLITE_CORRUPT",
        turso::Error::IoError(_, _) => "SQLITE_IOERR",
        // A failed batch reports its statement's code; a failed rollback wins because the
        // transaction state is then unknown.
        turso::Error::BatchStatementFailed { error, .. } => turso_error_code(error),
        turso::Error::BatchRollbackFailed { rollback_error, .. } => {
            turso_error_code(rollback_error)
        }
    }
}

fn sqlx_error_kind(code: &str, message: &str) -> ErrorKind {
    match code {
        "SQLITE_CONSTRAINT" => constraint_kind(message),
        _ => ErrorKind::Other,
    }
}

/// Classifies a constraint failure by the clause the engine writes first
///
/// The engine reports `<KIND> constraint failed[: <table>.<column>]`; a batch error prefixes
/// `batch statement N failed: `. Turso's SDK collapses SQLite's extended constraint codes into
/// one `Constraint(String)`, so this clause is the only structured signal. Words in the table
/// or column name after it (`tasks.unique_key`) never decide the kind.
fn constraint_kind(message: &str) -> ErrorKind {
    let Some((before_failure, _)) = message.split_once(" constraint failed") else {
        return ErrorKind::Other;
    };
    let clause = before_failure
        .rsplit(": ")
        .next()
        .unwrap_or(before_failure)
        .trim();
    match clause {
        "UNIQUE" | "PRIMARY KEY" => ErrorKind::UniqueViolation,
        "FOREIGN KEY" => ErrorKind::ForeignKeyViolation,
        "NOT NULL" => ErrorKind::NotNullViolation,
        "CHECK" => ErrorKind::CheckViolation,
        _ => ErrorKind::Other,
    }
}

#[cfg(test)]
mod tests {
    use sqlx_core::error::ErrorKind;

    use super::{constraint_kind, turso_error_code};

    #[test]
    fn constraint_kind_comes_from_the_leading_clause_not_the_column_name() {
        let cases = [
            (
                "UNIQUE constraint failed: tasks.id",
                ErrorKind::UniqueViolation,
            ),
            (
                "NOT NULL constraint failed: tasks.unique_key",
                ErrorKind::NotNullViolation,
            ),
            (
                "NOT NULL constraint failed: accounts.primary_account_id",
                ErrorKind::NotNullViolation,
            ),
            (
                "UNIQUE constraint failed: checks.foreign_key_name",
                ErrorKind::UniqueViolation,
            ),
            (
                "FOREIGN KEY constraint failed",
                ErrorKind::ForeignKeyViolation,
            ),
            (
                "CHECK constraint failed: is_default",
                ErrorKind::CheckViolation,
            ),
            (
                "batch statement 2 failed: NOT NULL constraint failed: t.unique_key",
                ErrorKind::NotNullViolation,
            ),
            ("database is locked", ErrorKind::Other),
        ];

        for (message, expected) in cases {
            assert_eq!(constraint_kind(message), expected, "{message}");
        }
    }

    #[test]
    fn batch_statement_failure_reports_the_statement_code() {
        // Arrange
        let error = turso::Error::BatchStatementFailed {
            index: 1,
            error: Box::new(turso::Error::Constraint(
                "UNIQUE constraint failed".to_owned(),
            )),
            results: Vec::new(),
        };

        // Act
        let code = turso_error_code(&error);

        // Assert
        assert_eq!(code, "SQLITE_CONSTRAINT");
    }

    #[test]
    fn batch_rollback_failure_reports_the_rollback_code() {
        // Arrange
        let error = turso::Error::BatchRollbackFailed {
            error: Box::new(turso::Error::Constraint(
                "UNIQUE constraint failed".to_owned(),
            )),
            rollback_error: Box::new(turso::Error::Busy("database is locked".to_owned())),
        };

        // Act
        let code = turso_error_code(&error);

        // Assert
        assert_eq!(code, "SQLITE_BUSY");
    }
}
