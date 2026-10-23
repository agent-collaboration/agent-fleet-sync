//! Test-only rollback after finalization has staged its registration and local journal step.
use durable_steps::StoreError;
use fleet_sync_protocol::{ConsumerProtocol, PeerId};
use fleet_sync_session::SessionEffect;
use tokio::sync::watch;

#[derive(Clone, Copy, Default)]
enum State {
    #[default]
    Idle,
    Armed(PeerId),
    Rejecting(PeerId),
    RolledBack(PeerId),
}
pub(crate) struct SchemaFinalizeProbe {
    state: watch::Sender<State>,
}
impl Default for SchemaFinalizeProbe {
    fn default() -> Self {
        Self {
            state: watch::channel(State::Idle).0,
        }
    }
}
impl SchemaFinalizeProbe {
    pub fn arm(&self, peer: PeerId) -> Result<(), StoreError> {
        if self.state.send_if_modified(|state| {
            if matches!(state, State::Idle) {
                *state = State::Armed(peer);
                true
            } else {
                false
            }
        }) {
            Ok(())
        } else {
            Err(StoreError::Aborted {
                reason: "schema finalization fault already armed".into(),
            })
        }
    }
    pub fn rejects<P: ConsumerProtocol>(&self, effects: &[SessionEffect<P>]) -> bool {
        self.state.send_if_modified(|state| {
            if let State::Armed(peer) = *state && effects.iter().any(|effect| matches!(effect, SessionEffect::Welcome { peer: target, .. } if *target == peer)) {
                *state = State::Rejecting(peer); true
            } else { false }
        })
    }
    pub async fn after_failed_transaction(&self) {
        let peer = match *self.state.borrow() {
            State::Rejecting(peer) => peer,
            _ => return,
        };
        let mut changed = self.state.subscribe();
        self.state.send_replace(State::RolledBack(peer));
        while matches!(*changed.borrow(), State::RolledBack(current) if current == peer) {
            if changed.changed().await.is_err() {
                break;
            }
        }
    }
    pub async fn wait_rollback(&self) -> Result<PeerId, StoreError> {
        let mut changed = self.state.subscribe();
        loop {
            if let State::RolledBack(peer) = *changed.borrow() {
                return Ok(peer);
            }
            changed.changed().await.map_err(StoreError::backend)?;
        }
    }
    pub fn release(&self) {
        self.state.send_replace(State::Idle);
    }
}
