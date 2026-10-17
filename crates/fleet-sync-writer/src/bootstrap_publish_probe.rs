#[derive(Clone, Default)]
pub(crate) struct BootstrapPublishProbe {
    #[cfg(feature = "test-support")]
    state: std::sync::Arc<State>,
}
#[cfg(feature = "test-support")]
struct State {
    reopen_blocked: std::sync::atomic::AtomicBool,
    armed: std::sync::atomic::AtomicBool,
    converted: tokio::sync::watch::Sender<Option<fleet_sync_receiver::SnapshotInstall>>,
    release: tokio::sync::Notify,
}
#[cfg(feature = "test-support")]
impl Default for State {
    fn default() -> Self {
        Self {
            reopen_blocked: Default::default(),
            armed: Default::default(),
            converted: tokio::sync::watch::channel(None).0,
            release: Default::default(),
        }
    }
}
impl BootstrapPublishProbe {
    pub fn check_reopen(&self) -> Result<(), crate::native_file::PublishError> {
        #[cfg(feature = "test-support")]
        if self
            .state
            .reopen_blocked
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(
                durable_steps::StoreError::backend("injected transient reopen failure").into(),
            );
        }
        Ok(())
    }
    #[cfg(feature = "test-support")]
    pub fn block_reopen(&self, blocked: bool) {
        self.state
            .reopen_blocked
            .store(blocked, std::sync::atomic::Ordering::SeqCst);
    }
    pub async fn converted(&self, _install: &fleet_sync_receiver::SnapshotInstall) {
        #[cfg(feature = "test-support")]
        if self
            .state
            .armed
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.state.converted.send_replace(Some(_install.clone()));
            self.state.release.notified().await;
        }
    }
    #[cfg(feature = "test-support")]
    pub fn arm(&self) {
        self.state.converted.send_replace(None);
        self.state
            .armed
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    #[cfg(feature = "test-support")]
    pub async fn wait(&self) -> Result<fleet_sync_receiver::SnapshotInstall, crate::OpenError> {
        let mut changed = self.state.converted.subscribe();
        loop {
            if let Some(install) = changed.borrow().clone() {
                return Ok(install);
            }
            changed
                .changed()
                .await
                .map_err(|error| crate::OpenError::Storage {
                    message: error.to_string(),
                })?;
        }
    }
    #[cfg(feature = "test-support")]
    pub fn release(&self) {
        self.state.release.notify_one();
    }
}
