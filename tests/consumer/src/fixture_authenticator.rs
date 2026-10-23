use crate::TestResult;
use fleet_sync_protocol::{AuthRefusal, PeerId};
use fleet_sync_session::{PeerAuthenticator, VerifiedPeer};
use runtime_primitives::{Clock, SystemClock};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tokio::sync::{Mutex, Notify, RwLock, watch};

pub struct FixtureAuthenticator {
    denied: RwLock<BTreeSet<PeerId>>,
    routes: RwLock<BTreeMap<PeerId, String>>,
    hold_next: Mutex<Option<PeerId>>,
    observed: watch::Sender<Option<PeerId>>,
    resume: Notify,
}
impl Default for FixtureAuthenticator {
    fn default() -> Self {
        Self {
            denied: RwLock::new(BTreeSet::new()),
            routes: RwLock::new(BTreeMap::new()),
            hold_next: Mutex::new(None),
            observed: watch::channel(None).0,
            resume: Notify::new(),
        }
    }
}
impl FixtureAuthenticator {
    pub async fn set_route(&self, peer: PeerId, route: String) {
        self.routes.write().await.insert(peer, route);
    }
    pub async fn set_allowed(&self, peer: PeerId, allowed: bool) {
        let mut denied = self.denied.write().await;
        if allowed {
            denied.remove(&peer);
        } else {
            denied.insert(peer);
        }
    }
    pub async fn hold_next(&self, peer: PeerId) {
        self.observed.send_replace(None);
        *self.hold_next.lock().await = Some(peer);
    }
    pub async fn wait_verification(&self, peer: PeerId) -> TestResult {
        let mut observed = self.observed.subscribe();
        loop {
            if *observed.borrow() == Some(peer) {
                return Ok(());
            }
            observed.changed().await?;
        }
    }
    pub fn resume_verification(&self) {
        self.resume.notify_one();
    }
}
impl PeerAuthenticator for FixtureAuthenticator {
    type Material = String;
    type Route = String;
    async fn verify(&self, material: String) -> Result<(VerifiedPeer, String), AuthRefusal> {
        let peer = material
            .strip_prefix("Bearer peer-")
            .and_then(|value| value.parse::<u128>().ok())
            .and_then(|value| {
                PeerId::new(durable_steps::Uuid::from_u128(
                    (value) | 0x019eef00000070008000000000000000,
                ))
                .ok()
            })
            .ok_or(AuthRefusal::Unknown)?;
        if self.denied.read().await.contains(&peer) {
            return Err(AuthRefusal::Revoked);
        }
        let verified = VerifiedPeer {
            peer,
            expires_at: SystemClock.now().after(Duration::from_secs(3600)),
        };
        let route = self
            .routes
            .read()
            .await
            .get(&peer)
            .cloned()
            .unwrap_or_else(|| "fixture-route".into());
        let hold = {
            let mut next = self.hold_next.lock().await;
            if *next == Some(peer) {
                next.take();
                true
            } else {
                false
            }
        };
        if hold {
            // The permission read already succeeded; the host must detect a
            // revocation while this actual verification future is unanswered.
            self.observed.send_replace(Some(peer));
            self.resume.notified().await;
        }
        Ok((verified, route))
    }
}
