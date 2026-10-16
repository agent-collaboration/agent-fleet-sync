use crate::migration_ownership::{MigrationKind, validate_ownership};
use crate::{OpenError, SchemaForm, TableLayout};
use fleet_sync_protocol::{SchemaEntry, TableName};
use std::collections::{BTreeMap, HashMap};
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
    #[error("writes are not admitted on a reader")]
    WriteOnReader,
    #[error("schema changes must run through migrate")]
    SchemaOutsideMigrate,
    #[error("a consumer statement cannot change connection pragmas")]
    PragmaWrite,
    #[error("a consumer statement cannot attach or detach databases")]
    Attachment,
    #[error("the transaction belongs to ReplicatedDatabase")]
    TransactionControl,
    #[error("consumer writes cannot change protected table {table}")]
    ProtectedTable { table: String },
    #[error("capture-off bookkeeping cannot change replicated table {table}")]
    ReplicatedTableOnMaintenance { table: String },
    #[error("statement form is not admitted")]
    UnsupportedStatement,
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
    checked: HashMap<String, CheckedProfiles>,
}

#[derive(Clone, Debug)]
struct CheckedProfiles {
    read: Result<(), StatementRefusal>,
    write: Result<(), StatementRefusal>,
    maintenance: Result<(), StatementRefusal>,
    migration: Result<Vec<MigrationStatement>, OpenError>,
}

#[derive(Clone, Debug)]
pub(crate) struct MigrationStatement {
    pub sql: String,
    pub command: Cmd,
    pub inspection: crate::migration_inspection::MigrationInspection,
}

impl StatementGuard {
    pub fn new(tables: Vec<TableLayout>) -> Self {
        Self {
            tables,
            checked: HashMap::new(),
        }
    }

    pub(crate) fn layouts(&self) -> &[TableLayout] {
        &self.tables
    }

    pub fn check(&mut self, sql: &str) -> Result<(), StatementRefusal> {
        self.checked_profiles(sql).write.clone()
    }

    pub(crate) fn check_maintenance(&mut self, sql: &str) -> Result<(), StatementRefusal> {
        self.checked_profiles(sql).maintenance.clone()
    }

    pub fn check_read(&mut self, sql: &str) -> Result<(), StatementRefusal> {
        self.checked_profiles(sql).read.clone()
    }

    pub(crate) fn migration_statements(
        &mut self,
        sql: &str,
        declared: &[TableName],
        catalog: &[(SchemaEntry, String)],
        kind: MigrationKind,
    ) -> Result<Vec<String>, OpenError> {
        let mut views = BTreeMap::new();
        for (entry, _) in catalog
            .iter()
            .filter(|(entry, _)| entry.kind == fleet_sync_protocol::SchemaObjectKind::View)
        {
            let parsed = self
                .checked_profiles(&entry.sql)
                .migration
                .as_ref()
                .map_err(Clone::clone)?;
            if let Some(statement) = parsed.first() {
                views.insert(
                    entry.name.to_ascii_lowercase(),
                    statement.inspection.clone(),
                );
            }
        }
        let statements = self
            .checked_profiles(sql)
            .migration
            .as_ref()
            .map_err(Clone::clone)?;
        validate_ownership(statements, declared, catalog, views, kind)?;
        Ok(statements
            .iter()
            .map(|statement| statement.sql.clone())
            .collect())
    }

    fn checked_profiles(&mut self, sql: &str) -> &CheckedProfiles {
        self.checked.entry(sql.to_owned()).or_insert_with(|| {
            match check_statements(sql, &self.tables) {
                Ok(profiles) => profiles,
                Err(error) => CheckedProfiles {
                    read: Err(error.clone()),
                    write: Err(error.clone()),
                    maintenance: Err(error.clone()),
                    migration: Err(OpenError::SchemaUnsupported {
                        form: SchemaForm::InvalidMigration {
                            statement: sql.into(),
                            detail: error.to_string(),
                        },
                    }),
                },
            }
        })
    }
}

fn refuse(form: StatementForm) -> StatementRefusal {
    StatementRefusal { form }
}

fn check_statements(
    sql: &str,
    tables: &[TableLayout],
) -> Result<CheckedProfiles, StatementRefusal> {
    let mut parser = Parser::new(sql.as_bytes());
    let mut profiles = CheckedProfiles {
        read: Ok(()),
        write: Ok(()),
        maintenance: Ok(()),
        migration: Ok(Vec::new()),
    };
    let mut statement_start = 0;
    while let Some(command) = parser.next_cmd().map_err(|error| StatementRefusal {
        form: StatementForm::InvalidSql {
            message: error.to_string(),
        },
    })? {
        let statement_end = parser.offset();
        let statement_sql = sql.get(statement_start..statement_end).ok_or_else(|| {
            refuse(StatementForm::InvalidSql {
                message: "invalid parser offset".into(),
            })
        })?;
        statement_start = statement_end;
        if profiles.migration.is_ok() {
            if let Err(error) = fleet_sync_protocol::admit_schema_command(&command) {
                profiles.migration = Err(OpenError::SchemaUnsupported {
                    form: SchemaForm::InvalidMigration {
                        statement: statement_sql.into(),
                        detail: error.to_string(),
                    },
                });
            } else if let Ok(statements) = &mut profiles.migration {
                statements.push(MigrationStatement {
                    sql: statement_sql.to_owned(),
                    inspection: crate::migration_inspection::inspect(&command),
                    command: command.clone(),
                });
            }
        }
        let Cmd::Stmt(statement) = &command else {
            profiles.read = Err(refuse(StatementForm::WriteOnReader));
            profiles.write = Err(refuse(StatementForm::UnsupportedStatement));
            profiles.maintenance = Err(refuse(StatementForm::UnsupportedStatement));
            continue;
        };
        let target = match statement {
            Stmt::Insert { tbl_name, .. } | Stmt::Delete { tbl_name, .. } => Some(tbl_name),
            Stmt::Update(update) => Some(&update.tbl_name),
            _ => None,
        };
        if let Some(target) = target
            && tables
                .iter()
                .any(|table| table.name().eq_ignore_ascii_case(target.name.as_str()))
        {
            profiles.maintenance = Err(refuse(StatementForm::ReplicatedTableOnMaintenance {
                table: target.name.as_str().into(),
            }));
        }
        let read_only = matches!(statement, Stmt::Select(_))
            || matches!(statement,Stmt::Pragma {name,body} if read_pragma(name.name.as_str(),body.is_some()));
        if !read_only && profiles.read.is_ok() {
            profiles.read = Err(refuse(StatementForm::WriteOnReader));
        }
        let form = match statement {
            Stmt::Select(_) => None,
            Stmt::Pragma { .. } if read_only => None,
            Stmt::Pragma { .. } => Some(StatementForm::PragmaWrite),
            Stmt::Insert { tbl_name, .. } | Stmt::Delete { tbl_name, .. } => {
                fleet_sync_protocol::reserved_namespace(tbl_name.name.as_str()).then(|| {
                    StatementForm::ProtectedTable {
                        table: tbl_name.name.as_str().into(),
                    }
                })
            }
            Stmt::Update(update) => {
                fleet_sync_protocol::reserved_namespace(update.tbl_name.name.as_str()).then(|| {
                    StatementForm::ProtectedTable {
                        table: update.tbl_name.name.as_str().into(),
                    }
                })
            }
            Stmt::CreateTable { .. }
            | Stmt::CreateIndex { .. }
            | Stmt::CreateView { .. }
            | Stmt::CreateTrigger { .. }
            | Stmt::CreateVirtualTable(_)
            | Stmt::AlterTable(_)
            | Stmt::DropTable { .. }
            | Stmt::DropIndex { .. }
            | Stmt::DropView { .. }
            | Stmt::DropTrigger { .. }
            | Stmt::CreateType { .. }
            | Stmt::DropType { .. }
            | Stmt::CreateSequence { .. }
            | Stmt::DropSequence { .. }
            | Stmt::DropDomain { .. } => Some(StatementForm::SchemaOutsideMigrate),
            Stmt::Attach { .. } | Stmt::Detach { .. } => Some(StatementForm::Attachment),
            Stmt::Begin { .. }
            | Stmt::Commit { .. }
            | Stmt::Rollback { .. }
            | Stmt::Savepoint { .. }
            | Stmt::Release { .. } => Some(StatementForm::TransactionControl),
            _ => Some(StatementForm::UnsupportedStatement),
        };
        if let Some(form) = form
            && profiles.write.is_ok()
        {
            profiles.write = Err(refuse(form));
        }
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
                    && profiles.write.is_ok()
                {
                    profiles.write = Err(StatementRefusal {
                        form: StatementForm::KeyChangingUpsert {
                            table: table.name().to_owned(),
                        },
                    });
                }
                clause = upsert.next.as_deref();
            }
        }
    }
    if profiles.write.is_err() {
        profiles.maintenance = profiles.write.clone();
    }
    Ok(profiles)
}

fn read_pragma(name: &str, has_argument: bool) -> bool {
    match name.to_ascii_lowercase().as_str() {
        "table_info" | "table_xinfo" | "foreign_key_list" | "index_list" | "index_info"
        | "index_xinfo" | "integrity_check" | "quick_check" => true,
        "foreign_keys" | "schema_version" | "user_version" | "application_id" | "database_list"
        | "collation_list" | "compile_options" | "function_list" | "module_list" | "table_list"
        | "page_count" | "page_size" | "freelist_count" | "query_only" | "busy_timeout"
        | "journal_mode" | "synchronous" => !has_argument,
        _ => false,
    }
}
