use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "StoredTableLayout")]
pub struct TableLayout {
    name: String,
    columns: Vec<String>,
    primary_key: Vec<usize>,
    integer_primary_key: Option<usize>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredTableLayout {
    name: String,
    columns: Vec<String>,
    primary_key: Vec<usize>,
    integer_primary_key: Option<usize>,
}
impl TryFrom<StoredTableLayout> for TableLayout {
    type Error = LayoutError;
    fn try_from(stored: StoredTableLayout) -> Result<Self, Self::Error> {
        Self::new(
            stored.name,
            stored.columns,
            stored.primary_key,
            stored.integer_primary_key,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LayoutError {
    #[error("table and column names must not be empty")]
    EmptyName,
    #[error("reserved replicated table name: {table}")]
    ReservedTable { table: String },
    #[error("replicated table {table} has no declared primary key")]
    NoPrimaryKey { table: String },
    #[error("invalid columns or primary key for table {table}")]
    InvalidColumns { table: String },
}

impl TableLayout {
    pub fn new(
        name: String,
        columns: Vec<String>,
        primary_key: Vec<usize>,
        integer_primary_key: Option<usize>,
    ) -> Result<Self, LayoutError> {
        if name.is_empty() || columns.iter().any(String::is_empty) {
            return Err(LayoutError::EmptyName);
        }
        if fleet_sync_protocol::reserved_namespace(&name) {
            return Err(LayoutError::ReservedTable { table: name });
        }
        if primary_key.is_empty() {
            return Err(LayoutError::NoPrimaryKey { table: name });
        }
        let unique_columns: BTreeSet<_> = columns
            .iter()
            .map(|column| column.to_ascii_lowercase())
            .collect();
        let unique_key: BTreeSet<_> = primary_key.iter().copied().collect();
        if columns.is_empty()
            || unique_columns.len() != columns.len()
            || unique_key.len() != primary_key.len()
            || primary_key.iter().any(|index| *index >= columns.len())
            || integer_primary_key.is_some_and(|index| primary_key.as_slice() != [index])
        {
            return Err(LayoutError::InvalidColumns { table: name });
        }
        Ok(Self {
            name,
            columns,
            primary_key,
            integer_primary_key,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    pub fn primary_key(&self) -> &[usize] {
        &self.primary_key
    }
    pub(crate) fn has_integer_primary_key(&self) -> bool {
        self.integer_primary_key.is_some()
    }

    pub(crate) fn is_primary_key_column(&self, column: &str) -> bool {
        self.primary_key
            .iter()
            .filter_map(|index| self.columns.get(*index))
            .any(|name| name.eq_ignore_ascii_case(column))
    }
}
