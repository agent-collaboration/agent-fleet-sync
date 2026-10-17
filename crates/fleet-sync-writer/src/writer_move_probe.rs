use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MoveAsidePhase {
    BeforeLink,
    AfterLink,
    AfterUnlink,
}
#[derive(Clone, Default)]
pub(crate) struct MoveAsideProbe {
    #[cfg(feature = "test-support")]
    state: std::sync::Arc<State>,
}
#[cfg(feature = "test-support")]
#[derive(Default)]
struct State {
    armed: std::sync::Mutex<Option<MoveAsidePhase>>,
    observed: Observed,
    release: tokio::sync::Notify,
}
#[cfg(feature = "test-support")]
struct Observed(tokio::sync::watch::Sender<Option<MoveAsidePhase>>);
#[cfg(feature = "test-support")]
impl Default for Observed {
    fn default() -> Self {
        Self(tokio::sync::watch::channel(None).0)
    }
}
impl MoveAsideProbe {
    pub async fn at(&self, _phase: MoveAsidePhase) {
        #[cfg(feature = "test-support")]
        {
            let held = self
                .state
                .armed
                .lock()
                .map(|mut armed| {
                    if *armed == Some(_phase) {
                        armed.take();
                        true
                    } else {
                        false
                    }
                })
                .unwrap_or(false);
            if held {
                self.state.observed.0.send_replace(Some(_phase));
                self.state.release.notified().await;
            }
        }
    }
    #[cfg(feature = "test-support")]
    pub fn arm(&self, phase: MoveAsidePhase) {
        self.state.observed.0.send_replace(None);
        if let Ok(mut armed) = self.state.armed.lock() {
            *armed = Some(phase);
        }
    }
    #[cfg(feature = "test-support")]
    pub async fn wait(&self) -> Result<MoveAsidePhase, crate::OpenError> {
        let mut changed = self.state.observed.0.subscribe();
        loop {
            if let Some(phase) = *changed.borrow() {
                return Ok(phase);
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
