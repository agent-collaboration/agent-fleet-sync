use crate::native_file_owner::NativeFileOwner;
use sqlx_core::pool::{Pool, PoolConnection};
use sqlx_turso::Turso;
use std::sync::{Arc, Mutex};

pub(crate) struct PoolSlot(Mutex<Option<Pool<Turso>>>);
impl PoolSlot {
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
    pub async fn close(&self) {
        if let Ok(pool) = self.current() {
            // Keep a handle in the slot until draining finishes. Cancellation
            // must leave the next owner able to finish closing this generation.
            pool.close().await;
            if let Ok(mut slot) = self.0.lock() {
                *slot = None;
            }
        }
    }
    pub fn reopen(&self, pool: Pool<Turso>) -> Result<(), sqlx_core::Error> {
        *self.0.lock().map_err(|_| sqlx_core::Error::PoolClosed)? = Some(pool);
        Ok(())
    }
}
/// Acquisitions recover the file owner before borrowing its current generation.
pub(crate) struct NativePool {
    slot: Arc<PoolSlot>,
    owner: Option<Arc<NativeFileOwner>>,
}
impl std::fmt::Debug for NativePool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativePool").finish_non_exhaustive()
    }
}
impl NativePool {
    #[cfg(test)]
    pub fn new(pool: Pool<Turso>) -> Self {
        Self {
            slot: Arc::new(PoolSlot::new(pool)),
            owner: None,
        }
    }
    pub fn owned(slot: Arc<PoolSlot>, owner: Arc<NativeFileOwner>) -> Self {
        Self {
            slot,
            owner: Some(owner),
        }
    }
    pub async fn recover(&self) -> Result<(), sqlx_core::Error> {
        if let Some(owner) = &self.owner {
            owner.recover().await.map_err(|error| match error {
                crate::native_file::PublishError::Closed => sqlx_core::Error::PoolClosed,
                error => sqlx_core::Error::Configuration(Box::new(error)),
            })?;
        }
        Ok(())
    }
    pub fn current(&self) -> Result<Pool<Turso>, sqlx_core::Error> {
        self.slot.current()
    }
    pub async fn acquire(&self) -> Result<PoolConnection<Turso>, sqlx_core::Error> {
        self.recover().await?;
        self.current()?.acquire().await
    }
    pub fn is_closed(&self) -> bool {
        self.current().map_or(true, |pool| pool.is_closed())
    }
}
