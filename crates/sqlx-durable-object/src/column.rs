use sqlx_core::column::Column;

use crate::{Do, DoTypeInfo};

/// A column of a Durable Object storage result.
#[derive(Debug, Clone)]
pub struct DoColumn {
    pub(crate) name: String,
    pub(crate) ordinal: usize,
    /// The type of the column's first non-NULL value, or `Null` if it has
    /// none -- Durable Object storage reports no declared types (see [`DoTypeInfo`]).
    pub(crate) type_info: DoTypeInfo,
}

impl Column for DoColumn {
    type Database = Do;

    fn ordinal(&self) -> usize {
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &DoTypeInfo {
        &self.type_info
    }
}
