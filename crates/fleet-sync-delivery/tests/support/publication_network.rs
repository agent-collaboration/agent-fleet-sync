use super::*;
use std::{
    collections::BTreeMap,
    sync::{Mutex, Weak},
};

/// Transport addresses are interpreted by this host fixture, never by delivery rules.
pub struct PublicationRoutes {
    streams: Mutex<BTreeMap<String, (PeerId, Weak<TestStore>)>>,
    peers: Mutex<BTreeMap<String, (PeerId, Weak<TestStore>)>>,
    pub calls: Mutex<Vec<(CallId, PublicationTarget<String>, serde_json::Value)>>,
    pub after_append: Mutex<Option<PublicationBarrier>>,
}
pub struct PublicationBarrier {
    pub item: ItemId,
    pub reached: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}
impl PublicationRoutes {
    pub fn new(sender: &Arc<TestStore>, stream: &Arc<TestStore>) -> Self {
        let routes = Self {
            streams: Mutex::new(BTreeMap::new()),
            peers: Mutex::new(BTreeMap::new()),
            calls: Mutex::new(Vec::new()),
            after_append: Mutex::new(None),
        };
        routes.register("fixture-route", peer(1), sender, stream);
        routes
    }
    pub fn register(
        &self,
        route: &str,
        owner: PeerId,
        sender: &Arc<TestStore>,
        stream: &Arc<TestStore>,
    ) {
        self.streams
            .lock()
            .unwrap()
            .insert(route.to_owned(), (owner, Arc::downgrade(stream)));
        self.peers
            .lock()
            .unwrap()
            .insert(route.to_owned(), (owner, Arc::downgrade(sender)));
    }
    pub async fn dispatch(
        &self,
        target: PublicationTarget<String>,
        publication: StatusPublication<Acceptance>,
        call: CallId,
    ) {
        self.calls.lock().unwrap().push((
            call,
            target.clone(),
            serde_json::to_value(&publication).unwrap(),
        ));
        match target {
            PublicationTarget::StatusStream { sender, route } => {
                let store = {
                    let endpoints = self.streams.lock().unwrap();
                    let (owner, store) = endpoints.get(&route).unwrap();
                    assert_eq!(*owner, sender);
                    store.upgrade().unwrap()
                };
                let item = publication.item;
                store
                    .transaction(move |scope| {
                        Box::pin(async move {
                            StatusStream::<_, TestTypes>::new(scope, sender, route)
                                .append_and_admit::<network::PublishTask>(&publication)
                                .await
                                .map_err(storage_error)?;
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
                let barrier = {
                    let mut pending = self.after_append.lock().unwrap();
                    if pending.as_ref().is_some_and(|barrier| barrier.item == item) {
                        pending.take()
                    } else {
                        None
                    }
                };
                if let Some(barrier) = barrier {
                    barrier.reached.notify_one();
                    barrier.release.notified().await;
                }
            }
            PublicationTarget::Peer { peer, route } => {
                let store = {
                    let endpoints = self.peers.lock().unwrap();
                    let (owner, store) = endpoints.get(&route).unwrap();
                    assert_eq!(*owner, peer);
                    store.upgrade().unwrap()
                };
                store
                    .transaction(move |scope| {
                        Box::pin(async move {
                            Outbox::<_, TestTypes>::new(scope, limits())
                                .publish_status(publication)
                                .await
                                .map_err(storage_error)
                        })
                    })
                    .await
                    .unwrap();
            }
        }
    }
}
