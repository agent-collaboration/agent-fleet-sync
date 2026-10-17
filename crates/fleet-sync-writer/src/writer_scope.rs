use durable_steps::storage::TransactionScope;
use sqlx_turso::{Turso, TursoConnection};

pub(crate) struct WriterScope<'scope>(pub &'scope mut TursoConnection);
impl TransactionScope for WriterScope<'_> {
    type Database = Turso;
    type Executor<'query>
        = &'query mut TursoConnection
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
