use crate::checked_queries;
use durable_steps::storage::TransactionScope;
use fleet_sync_writer::WriteTransaction;
use sqlx_turso::Turso;
struct CapturedScope<'a>(&'a mut WriteTransaction);
impl TransactionScope for CapturedScope<'_> {
    type Database = Turso;
    type Executor<'a>
        = &'a mut WriteTransaction
    where
        Self: 'a;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
pub async fn insert_sample(
    write: &mut WriteTransaction,
    id: String,
    label: String,
    exact_value: i64,
    payload: Vec<u8>,
) -> Result<(), sqlx_core::Error> {
    checked_queries::insert_sample(&mut CapturedScope(write), id, label, exact_value, payload)
        .await?;
    Ok(())
}
pub async fn insert_reading(
    write: &mut WriteTransaction,
    id: String,
    measured: f64,
) -> Result<(), sqlx_core::Error> {
    checked_queries::insert_reading(&mut CapturedScope(write), id, measured).await?;
    Ok(())
}
pub async fn insert_source(
    write: &mut WriteTransaction,
    source_id: String,
    event_id: String,
    sample_id: String,
) -> Result<(), sqlx_core::Error> {
    checked_queries::insert_source(&mut CapturedScope(write), source_id, event_id, sample_id)
        .await?;
    Ok(())
}
