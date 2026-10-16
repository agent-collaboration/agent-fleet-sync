use crate::TableLayout;
use std::collections::HashMap;
use turso_parser::{
    ast::{Cmd, InsertBody, Stmt, UpsertDo},
    parser::Parser,
};

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StatementForm {
    #[error("upsert assigns a declared primary-key column of {table}")]
    KeyChangingUpsert { table: String },
    #[error("invalid SQL: {message}")]
    InvalidSql { message: String },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("writer statement refused: {form}")]
pub struct StatementRefusal {
    pub form: StatementForm,
}

/// Lives with one schema version. Replacing its catalog also discards its SQL cache.
#[derive(Debug)]
pub struct StatementGuard {
    tables: Vec<TableLayout>,
    checked: HashMap<String, Result<(), StatementRefusal>>,
}

impl StatementGuard {
    pub fn new(tables: Vec<TableLayout>) -> Self {
        Self {
            tables,
            checked: HashMap::new(),
        }
    }

    pub fn check(&mut self, sql: &str) -> Result<(), StatementRefusal> {
        if let Some(result) = self.checked.get(sql) {
            return result.clone();
        }
        let result = check_statements(sql, &self.tables);
        self.checked.insert(sql.to_owned(), result.clone());
        result
    }
}

fn check_statements(sql: &str, tables: &[TableLayout]) -> Result<(), StatementRefusal> {
    let mut parser = Parser::new(sql.as_bytes());
    while let Some(command) = parser.next_cmd().map_err(|error| StatementRefusal {
        form: StatementForm::InvalidSql {
            message: error.to_string(),
        },
    })? {
        if let Cmd::Stmt(Stmt::Insert {
            tbl_name,
            body: InsertBody::Select(_, upsert),
            ..
        }) = command
            && let Some(table) = tables
                .iter()
                .find(|table| table.name().eq_ignore_ascii_case(tbl_name.name.as_str()))
        {
            let mut clause = upsert.as_deref();
            while let Some(upsert) = clause {
                if let UpsertDo::Set { sets, .. } = &upsert.do_clause
                    && sets
                        .iter()
                        .flat_map(|assignment| &assignment.col_names)
                        .any(|column| table.is_primary_key_column(column.as_str()))
                {
                    return Err(StatementRefusal {
                        form: StatementForm::KeyChangingUpsert {
                            table: table.name().to_owned(),
                        },
                    });
                }
                clause = upsert.next.as_deref();
            }
        }
    }
    Ok(())
}
