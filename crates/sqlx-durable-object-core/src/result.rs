/// What a statement did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct QueryResult {
    rows_affected: u64,
    last_insert_rowid: i64,
}

impl QueryResult {
    /// A result reporting `rows_affected` and `last_insert_rowid`, 0 when the
    /// backend reports none.
    #[must_use]
    pub fn new(rows_affected: u64, last_insert_rowid: i64) -> Self {
        Self {
            rows_affected,
            last_insert_rowid,
        }
    }

    /// Rows the statement inserted, updated or deleted.
    #[must_use]
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// The rowid of the last row inserted, or 0 when the statement inserted
    /// none.
    ///
    /// An `i64` rather than an `Option`, as sqlx-sqlite reports it, so code
    /// written against `SqliteQueryResult` compiles unchanged. Rowids start
    /// at 1 unless a row is given 0 explicitly, so 0 is unambiguous in
    /// practice; `RETURNING` is the way to be certain.
    #[must_use]
    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid
    }
}

impl Extend<QueryResult> for QueryResult {
    fn extend<T: IntoIterator<Item = QueryResult>>(&mut self, iter: T) {
        for result in iter {
            self.rows_affected += result.rows_affected;
            // The last statement that reported a rowid, not the last
            // statement: a trailing SELECT reporting none should not erase
            // the INSERT before it.
            if result.last_insert_rowid != 0 {
                self.last_insert_rowid = result.last_insert_rowid;
            }
        }
    }
}

/// What one statement of a batch did, and the rows it returned.
///
/// Generic over the driver's row type, which only the driver can name;
/// each driver exposes it as its own alias.
#[derive(Debug, Clone)]
pub struct BatchResult<R> {
    rows: Vec<R>,
    result: QueryResult,
}

impl<R> BatchResult<R> {
    /// A statement's `rows` and what it did.
    #[must_use]
    pub fn new(rows: Vec<R>, result: QueryResult) -> Self {
        Self { rows, result }
    }

    /// The rows the statement returned: a `SELECT`'s, or an `INSERT ...
    /// RETURNING`'s. Empty for a statement that returns none.
    #[must_use]
    pub fn rows(&self) -> &[R] {
        &self.rows
    }

    /// The rows, giving up the result.
    #[must_use]
    pub fn into_rows(self) -> Vec<R> {
        self.rows
    }

    /// What the statement did.
    #[must_use]
    pub fn result(&self) -> &QueryResult {
        &self.result
    }

    /// The rows and what the statement did.
    #[must_use]
    pub fn into_parts(self) -> (Vec<R>, QueryResult) {
        (self.rows, self.result)
    }
}

#[cfg(test)]
mod tests {
    use super::QueryResult;

    #[test]
    fn extend_sums_changes_and_keeps_the_last_reported_rowid() {
        let mut result = QueryResult::default();

        result.extend([
            QueryResult::new(2, 7),
            QueryResult::new(1, 9),
            QueryResult::new(0, 0),
        ]);

        assert_eq!(result.rows_affected(), 3);
        assert_eq!(result.last_insert_rowid(), 9);
    }
}
