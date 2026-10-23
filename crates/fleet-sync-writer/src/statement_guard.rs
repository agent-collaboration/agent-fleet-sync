use crate::migration_ownership::{MigrationKind, writer_error};
use crate::{OpenError, TableLayout};
use fleet_sync_protocol::schema_catalog::MigrationClassifier;
use fleet_sync_protocol::{SchemaEntry, TableName};
use std::collections::HashMap;
use turso_parser::{
    ast::{Cmd, Stmt},
    parser::Parser,
};

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StatementForm {
    #[error("statement assigns an immutable primary-key column of {table}")]
    KeyChange { table: String },
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
    migrations: MigrationClassifier,
}

#[derive(Clone, Debug)]
struct CheckedProfiles {
    read: Result<(), StatementRefusal>,
    write: Result<(), StatementRefusal>,
    maintenance: Result<(), StatementRefusal>,
}

pub(crate) use fleet_sync_protocol::schema_catalog::MigrationPlan;

impl StatementGuard {
    pub fn new(tables: Vec<TableLayout>) -> Self {
        Self {
            tables,
            checked: HashMap::new(),
            migrations: MigrationClassifier::default(),
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
        Ok(self
            .migration_plan(sql, declared, catalog, kind)?
            .statements)
    }

    pub(crate) fn migration_plan(
        &mut self,
        sql: &str,
        declared: &[TableName],
        catalog: &[(SchemaEntry, String)],
        kind: MigrationKind,
    ) -> Result<MigrationPlan, OpenError> {
        let current = self
            .tables
            .iter()
            .map(|table| {
                TableName::new(table.name()).map_err(|_| OpenError::corrupt("migration.membership"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.migrations
            .plan(sql, &current, declared, catalog, kind)
            .map_err(writer_error)
    }

    fn checked_profiles(&mut self, sql: &str) -> &CheckedProfiles {
        self.checked.entry(sql.to_owned()).or_insert_with(|| {
            match check_statements(sql, &self.tables) {
                Ok(profiles) => profiles,
                Err(error) => CheckedProfiles {
                    read: Err(error.clone()),
                    write: Err(error.clone()),
                    maintenance: Err(error),
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
    };
    while let Some(command) = parser.next_cmd().map_err(|error| StatementRefusal {
        form: StatementForm::InvalidSql {
            message: error.to_string(),
        },
    })? {
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
        if let Some(target) = target
            && let Some(table) = tables
                .iter()
                .find(|table| table.name().eq_ignore_ascii_case(target.name.as_str()))
            && table
                .primary_key()
                .iter()
                .filter_map(|index| table.columns().get(*index))
                .any(|key| fleet_sync_protocol::assigns_replicated_key(&command, key))
            && profiles.write.is_ok()
        {
            profiles.write = Err(refuse(StatementForm::KeyChange {
                table: table.name().into(),
            }));
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
