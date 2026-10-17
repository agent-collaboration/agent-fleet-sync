use sqlx_core::pool::{Pool, PoolConnection};
use sqlx_turso::Turso;
use std::sync::Mutex;

/// A private pool slot. Closing removes the old generation before draining it;
/// callers can only acquire from the generation currently owned by this file.
#[derive(Debug)]
pub(crate) struct NativePool(Mutex<Option<Pool<Turso>>>);
impl NativePool {
    pub fn new(pool: Pool<Turso>) -> Self {
        Self(Mutex::new(Some(pool)))
    }
    pub fn current(&self) -> Result<Pool<Turso>, sqlx_core::Error> {
        self.0
            .lock()
            .map_err(|_| sqlx_core::Error::PoolClosed)?
            .as_ref()
            .cloned()
            .ok_or(sqlx_core::Error::PoolClosed)
    }
    pub async fn acquire(&self) -> Result<PoolConnection<Turso>, sqlx_core::Error> {
        self.current()?.acquire().await
    }
    pub fn is_closed(&self) -> bool {
        self.current().map_or(true, |pool| pool.is_closed())
    }
    pub async fn close(&self) {
        let pool = self.0.lock().ok().and_then(|mut slot| slot.take());
        if let Some(pool) = pool {
            pool.close().await;
        }
    }
    pub fn reopen(&self, pool: Pool<Turso>) -> Result<(), sqlx_core::Error> {
        *self.0.lock().map_err(|_| sqlx_core::Error::PoolClosed)? = Some(pool);
        Ok(())
    }
}
