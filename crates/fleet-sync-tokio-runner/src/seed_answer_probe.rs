use durable_steps::Uuid;
use fleet_sync_protocol::{PageDigest, SeedId};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, watch};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SeedPageObservation {
    pub generation: Uuid,
    pub seed: SeedId,
    pub page: u32,
    pub digest: PageDigest,
    pub rows: u64,
}
#[derive(Clone, Default)]
enum State {
    #[default]
    Idle,
    Armed {
        page: u32,
    },
    Committed(SeedPageObservation),
}
pub(crate) struct SeedAnswerProbe {
    state: watch::Sender<State>,
    release: Notify,
    renewed: watch::Sender<u64>,
}
impl Default for SeedAnswerProbe {
    fn default() -> Self {
        Self {
            state: watch::channel(State::Idle).0,
            release: Notify::new(),
            renewed: watch::channel(0).0,
        }
    }
}
impl SeedAnswerProbe {
    pub fn periodic_renewed(&self) {
        self.renewed
            .send_modify(|count| *count = count.saturating_add(1));
    }
    pub async fn wait_periodic_renewal(&self) -> Result<(), durable_steps::StoreError> {
        let mut changed = self.renewed.subscribe();
        loop {
            if *changed.borrow() > 0 {
                return Ok(());
            }
            changed
                .changed()
                .await
                .map_err(durable_steps::StoreError::backend)?;
        }
    }
    pub fn arm(&self) {
        self.arm_page(0);
    }
    pub fn arm_page(&self, page: u32) {
        self.state.send_replace(State::Armed { page });
    }
    pub async fn after_commit(&self, observation: SeedPageObservation) {
        if self.state.send_if_modified(|state| {
            if matches!(state, State::Armed {page} if *page==observation.page) {
                *state = State::Committed(observation);
                true
            } else {
                false
            }
        }) {
            self.release.notified().await;
        }
    }
    pub async fn wait(&self) -> Result<SeedPageObservation, durable_steps::StoreError> {
        let mut changed = self.state.subscribe();
        loop {
            if let State::Committed(observation) = changed.borrow().clone() {
                return Ok(observation);
            }
            changed
                .changed()
                .await
                .map_err(durable_steps::StoreError::backend)?;
        }
    }
    pub fn release(&self) {
        self.release.notify_one();
    }
}
