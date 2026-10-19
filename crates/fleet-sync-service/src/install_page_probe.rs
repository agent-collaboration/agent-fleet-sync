use crate::{ServiceConsumer, ServiceError, ServiceHandle};
use fleet_sync_protocol::ConsumerProtocol;
use fleet_sync_receiver::InstallPageReceipt;
use fleet_sync_session::PeerAuthenticator;
use tokio::sync::{Notify, watch};

#[derive(Clone, Default)]
enum ProbeState {
    #[default]
    Idle,
    Armed,
    Committed(InstallPageReceipt),
}
pub(crate) struct InstallPageProbe {
    state: watch::Sender<ProbeState>,
    release: Notify,
    missing: watch::Sender<Option<fleet_sync_protocol::ReceiverId>>,
}
impl Default for InstallPageProbe {
    fn default() -> Self {
        Self {
            state: watch::channel(ProbeState::Idle).0,
            release: Notify::new(),
            missing: watch::channel(None).0,
        }
    }
}
impl InstallPageProbe {
    pub(crate) fn source_missing(&self, receiver: fleet_sync_protocol::ReceiverId) {
        self.missing.send_replace(Some(receiver));
    }
    pub(crate) async fn after_commit(&self, receipt: InstallPageReceipt) {
        if self.state.send_if_modified(|state| {
            if matches!(state, ProbeState::Armed) {
                *state = ProbeState::Committed(receipt);
                true
            } else {
                false
            }
        }) {
            self.release.notified().await;
        }
    }
}
impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> ServiceHandle<P, C, A>
{
    pub async fn wait_source_unattached(
        &self,
        receiver: fleet_sync_protocol::ReceiverId,
    ) -> Result<(), ServiceError> {
        let mut changes = self.state.install_page_probe.missing.subscribe();
        loop {
            if *changes.borrow() == Some(receiver) {
                return Ok(());
            }
            changes.changed().await.map_err(ServiceError::rule)?;
        }
    }
    pub fn hold_next_install_page_answer(&self) {
        self.state
            .install_page_probe
            .state
            .send_replace(ProbeState::Armed);
    }
    pub async fn wait_staged_install_page(&self) -> Result<InstallPageReceipt, ServiceError> {
        let mut changes = self.state.install_page_probe.state.subscribe();
        loop {
            if let ProbeState::Committed(receipt) = changes.borrow().clone() {
                return Ok(receipt);
            }
            changes.changed().await.map_err(ServiceError::rule)?;
        }
    }
    pub fn release_install_page_answer(&self) {
        self.state.install_page_probe.release.notify_one();
    }
}
