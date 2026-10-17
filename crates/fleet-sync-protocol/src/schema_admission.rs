use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SchemaStatementForm {
    CreateTable,
    CreateIndex,
    CreateView,
    AlterTable,
    DropTable,
    DropIndex,
    DropView,
    Insert,
    Update,
    Delete,
    CreateTrigger,
    DropTrigger,
    VirtualTable,
    Attach,
    Detach,
    Pragma,
    Vacuum,
    Reindex,
    Analyze,
    TransactionControl,
    Select,
    Explain,
    Other,
    InvalidSql,
    MultipleStatements,
    Empty,
}
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("schema statement {form:?} is unsupported: {reason}")]
pub struct SchemaAdmissionError {
    pub form: SchemaStatementForm,
    pub reason: SchemaAdmissionReason,
}
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SchemaAdmissionReason {
    #[error("replicated key column {column:?} of {table} must be TEXT NOT NULL")]
    KeyNotUuidV7 {
        table: String,
        column: Option<String>,
    },
    #[error("statement form is not admitted")]
    DisallowedForm,
    #[error("protected schema object {name}")]
    ProtectedObject { name: String },
    #[error("invalid SQL: {message}")]
    InvalidSql { message: String },
    #[error("expected exactly one statement")]
    StatementCount,
}
pub fn admit_schema_statement(sql: &str) -> Result<(), SchemaAdmissionError> {
    admit_schema_command(&parse_schema_statement(sql)?)
}

/// Replicated keys follow the UUIDv7 contract; local schema stays unrestricted.
pub fn admit_replicated_schema_statement(sql: &str) -> Result<(), SchemaAdmissionError> {
    admit_replicated_schema_command(&parse_schema_statement(sql)?)
}

pub fn admit_replicated_schema_command(
    command: &turso_parser::ast::Cmd,
) -> Result<(), SchemaAdmissionError> {
    admit_schema_command(command)?;
    if let Err(error) = crate::replicated_key_columns(command) {
        return Err(SchemaAdmissionError {
            form: statement_form(command),
            reason: SchemaAdmissionReason::KeyNotUuidV7 {
                table: error.table,
                column: error.column,
            },
        });
    }
    Ok(())
}

fn parse_schema_statement(sql: &str) -> Result<turso_parser::ast::Cmd, SchemaAdmissionError> {
    let invalid = |error: turso_parser::error::Error| SchemaAdmissionError {
        form: SchemaStatementForm::InvalidSql,
        reason: SchemaAdmissionReason::InvalidSql {
            message: error.to_string(),
        },
    };
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let command = parser
        .next_cmd()
        .map_err(invalid)?
        .ok_or(SchemaAdmissionError {
            form: SchemaStatementForm::Empty,
            reason: SchemaAdmissionReason::StatementCount,
        })?;
    if parser.next_cmd().map_err(invalid)?.is_some() {
        return Err(SchemaAdmissionError {
            form: SchemaStatementForm::MultipleStatements,
            reason: SchemaAdmissionReason::StatementCount,
        });
    }
    Ok(command)
}

/// Apply schema policy to one parsed command. The caller owns SQL framing;
/// `admit_schema_statement` also rejects empty, invalid and multiple statements.
pub fn admit_schema_command(command: &turso_parser::ast::Cmd) -> Result<(), SchemaAdmissionError> {
    let form = statement_form(command);
    if !matches!(
        form,
        SchemaStatementForm::CreateTable
            | SchemaStatementForm::CreateIndex
            | SchemaStatementForm::CreateView
            | SchemaStatementForm::AlterTable
            | SchemaStatementForm::DropTable
            | SchemaStatementForm::DropIndex
            | SchemaStatementForm::DropView
            | SchemaStatementForm::Insert
            | SchemaStatementForm::Update
            | SchemaStatementForm::Delete
    ) {
        return Err(SchemaAdmissionError {
            form,
            reason: SchemaAdmissionReason::DisallowedForm,
        });
    }
    use turso_parser::ast::fmt::ToTokens;
    command.to_tokens(
        &mut ProtectedIdentifiers { form },
        &turso_parser::ast::fmt::BlankContext,
    )
}
fn statement_form(command: &turso_parser::ast::Cmd) -> SchemaStatementForm {
    use SchemaStatementForm as Form;
    use turso_parser::ast::{Cmd, Stmt};
    let Cmd::Stmt(statement) = command else {
        return Form::Explain;
    };
    match statement {
        Stmt::CreateTable { .. } => Form::CreateTable,
        Stmt::CreateIndex { .. } => Form::CreateIndex,
        Stmt::CreateView { .. } => Form::CreateView,
        Stmt::AlterTable(..) => Form::AlterTable,
        Stmt::DropTable { .. } => Form::DropTable,
        Stmt::DropIndex { .. } => Form::DropIndex,
        Stmt::DropView { .. } => Form::DropView,
        Stmt::Insert { .. } => Form::Insert,
        Stmt::Update(..) => Form::Update,
        Stmt::Delete { .. } => Form::Delete,
        Stmt::CreateTrigger { .. } => Form::CreateTrigger,
        Stmt::DropTrigger { .. } => Form::DropTrigger,
        Stmt::CreateVirtualTable(..) => Form::VirtualTable,
        Stmt::Attach { .. } => Form::Attach,
        Stmt::Detach { .. } => Form::Detach,
        Stmt::Pragma { .. } => Form::Pragma,
        Stmt::Vacuum { .. } => Form::Vacuum,
        Stmt::Reindex { .. } => Form::Reindex,
        Stmt::Analyze { .. } => Form::Analyze,
        Stmt::Begin { .. }
        | Stmt::Commit { .. }
        | Stmt::Rollback { .. }
        | Stmt::Savepoint { .. }
        | Stmt::Release { .. } => Form::TransactionControl,
        Stmt::Select(..) => Form::Select,
        _ => Form::Other,
    }
}
// The pinned parser's AST writer visits nested expressions and CTEs. Name
// emits TK_ID with identifier quoting; literal strings emit TK_STRING.
struct ProtectedIdentifiers {
    form: SchemaStatementForm,
}
impl turso_parser::ast::fmt::TokenStream for ProtectedIdentifiers {
    type Error = SchemaAdmissionError;
    fn append(
        &mut self,
        kind: turso_parser::token::TokenType,
        value: Option<&str>,
    ) -> Result<(), Self::Error> {
        if kind == turso_parser::token::TokenType::TK_ID
            && let Some(value) = value
        {
            let name = turso_parser::ast::Name::from_string(value);
            if crate::reserved_namespace(name.as_str()) {
                return Err(SchemaAdmissionError {
                    form: self.form,
                    reason: SchemaAdmissionReason::ProtectedObject {
                        name: name.as_str().into(),
                    },
                });
            }
        }
        Ok(())
    }
}
