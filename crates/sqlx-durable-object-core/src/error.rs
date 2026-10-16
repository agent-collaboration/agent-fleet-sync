use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

use sqlx_core::error::ErrorKind;

/// An error the database returned for a statement.
///
/// Neither D1 nor Durable Object storage reports a structured result code
/// across the binding, only SQLite's message as text, which ends with the code
/// by name: `NOT NULL constraint failed: users.email: SQLITE_CONSTRAINT
/// (extended: SQLITE_CONSTRAINT_NOTNULL)`.
/// [`code`](sqlx_core::error::DatabaseError::code) is that name, and
/// [`kind`](sqlx_core::error::DatabaseError::kind) is read from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseError {
    message: String,
}

impl DatabaseError {
    /// The error for `message`, as the backend threw it.
    #[must_use]
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for DatabaseError {}

impl sqlx_core::error::DatabaseError for DatabaseError {
    fn message(&self) -> &str {
        &self.message
    }

    /// SQLite's result code by name -- the extended one when the message has it, as
    /// in `SQLITE_CONSTRAINT_UNIQUE`, else the primary one.
    fn code(&self) -> Option<Cow<'_, str>> {
        code(&self.message).map(Cow::Borrowed)
    }

    fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        kind(&self.message)
    }
}

/// The result code at the end of a message: the `extended:` one if there
/// is one, else the last `SQLITE_*` word.
fn code(message: &str) -> Option<&str> {
    let is_code_char = |c: char| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_';

    if let Some((_, rest)) = message.rsplit_once("(extended: ") {
        let end = rest.find(|c| !is_code_char(c)).unwrap_or(rest.len());
        if end > 0 {
            return rest.get(..end);
        }
    }

    let start = message.rfind("SQLITE_")?;
    let rest = message.get(start..)?;
    rest.get(..rest.find(|c| !is_code_char(c)).unwrap_or(rest.len()))
}

/// The kind of error a message means.
///
/// From the extended result code when the message carries one, as
/// sqlx-sqlite maps them. Failing that, from SQLite's constraint text, which
/// is brittle -- which is why both live here and nowhere else: when a backend
/// changes its format, this is the only thing to change, and the tests below
/// are what notice.
fn kind(message: &str) -> ErrorKind {
    match code(message) {
        Some("SQLITE_CONSTRAINT_UNIQUE" | "SQLITE_CONSTRAINT_PRIMARYKEY") => {
            return ErrorKind::UniqueViolation;
        }
        Some("SQLITE_CONSTRAINT_FOREIGNKEY") => return ErrorKind::ForeignKeyViolation,
        Some("SQLITE_CONSTRAINT_NOTNULL") => return ErrorKind::NotNullViolation,
        Some("SQLITE_CONSTRAINT_CHECK") => return ErrorKind::CheckViolation,
        _ => {}
    }

    if message.contains("UNIQUE constraint failed") {
        ErrorKind::UniqueViolation
    } else if message.contains("FOREIGN KEY constraint failed") {
        ErrorKind::ForeignKeyViolation
    } else if message.contains("NOT NULL constraint failed") {
        ErrorKind::NotNullViolation
    } else if message.contains("CHECK constraint failed") {
        ErrorKind::CheckViolation
    } else {
        ErrorKind::Other
    }
}

#[cfg(test)]
mod tests {
    use sqlx_core::error::{DatabaseError as _, ErrorKind};

    use super::DatabaseError;

    fn kind(message: &str) -> ErrorKind {
        DatabaseError::new(message.to_owned()).kind()
    }

    // As local D1 (workerd) reported it, 2026-09-23.
    const NOT_NULL: &str = "NOT NULL constraint failed: users.email: SQLITE_CONSTRAINT \
                            (extended: SQLITE_CONSTRAINT_NOTNULL)";

    #[test]
    fn reads_the_extended_code() {
        let error = DatabaseError::new(NOT_NULL.to_owned());

        assert_eq!(error.code().as_deref(), Some("SQLITE_CONSTRAINT_NOTNULL"));
        assert!(matches!(error.kind(), ErrorKind::NotNullViolation));
    }

    #[test]
    fn falls_back_to_the_primary_code() {
        let error = DatabaseError::new(
            "D1_ERROR: near \"SELEC\": syntax error at offset 0: SQLITE_ERROR".to_owned(),
        );

        assert_eq!(error.code().as_deref(), Some("SQLITE_ERROR"));
        assert!(matches!(error.kind(), ErrorKind::Other));
    }

    #[test]
    fn maps_each_extended_constraint_code() {
        for (code, expected) in [
            ("SQLITE_CONSTRAINT_UNIQUE", "unique"),
            ("SQLITE_CONSTRAINT_PRIMARYKEY", "unique"),
            ("SQLITE_CONSTRAINT_FOREIGNKEY", "foreign key"),
            ("SQLITE_CONSTRAINT_NOTNULL", "not null"),
            ("SQLITE_CONSTRAINT_CHECK", "check"),
        ] {
            let kind = kind(&format!("x: SQLITE_CONSTRAINT (extended: {code})"));
            let actual = match kind {
                ErrorKind::UniqueViolation => "unique",
                ErrorKind::ForeignKeyViolation => "foreign key",
                ErrorKind::NotNullViolation => "not null",
                ErrorKind::CheckViolation => "check",
                _ => "other",
            };
            assert_eq!(actual, expected, "{code}");
        }
    }

    // Without the extended code, the constraint text still decides.
    #[test]
    fn maps_constraint_messages_to_kinds() {
        assert!(matches!(
            kind("D1_ERROR: UNIQUE constraint failed: users.email: SQLITE_CONSTRAINT"),
            ErrorKind::UniqueViolation
        ));
        assert!(matches!(
            kind("D1_ERROR: FOREIGN KEY constraint failed: SQLITE_CONSTRAINT"),
            ErrorKind::ForeignKeyViolation
        ));
        assert!(matches!(
            kind("D1_ERROR: NOT NULL constraint failed: users.name: SQLITE_CONSTRAINT"),
            ErrorKind::NotNullViolation
        ));
        assert!(matches!(
            kind("D1_ERROR: CHECK constraint failed: age >= 0: SQLITE_CONSTRAINT"),
            ErrorKind::CheckViolation
        ));
    }

    #[test]
    fn anything_else_is_other() {
        assert!(matches!(
            kind("D1_ERROR: no such table: users: SQLITE_ERROR"),
            ErrorKind::Other
        ));
    }
}
