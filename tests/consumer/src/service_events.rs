use crate::TestResult;
use fleet_sync_protocol::PeerId;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::Notify;
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceEvent {
    pub fields: BTreeMap<String, String>,
}
pub(crate) struct ServiceEvents {
    rows: Mutex<Vec<ServiceEvent>>,
    changed: Notify,
}
impl ServiceEvents {
    pub fn install() -> TestResult<Arc<Self>> {
        let events = Arc::new(Self {
            rows: Mutex::new(Vec::new()),
            changed: Notify::new(),
        });
        tracing::subscriber::set_global_default(Capture {
            events: events.clone(),
            spans: AtomicU64::new(1),
        })?;
        Ok(events)
    }
    pub fn snapshot(&self) -> TestResult<Vec<ServiceEvent>> {
        Ok(self
            .rows
            .lock()
            .map_err(|_| "service event capture lock poisoned")?
            .clone())
    }
    pub async fn wait_mailbox(&self, mailbox: PeerId, after: usize) -> TestResult<ServiceEvent> {
        let identity = mailbox.as_uuid().to_string();
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(event) = self
                .snapshot()?
                .into_iter()
                .filter(|event| {
                    event
                        .fields
                        .get("mailbox")
                        .is_some_and(|value| value.contains(&identity))
                })
                .nth(after)
            {
                return Ok(event);
            }
            notified.await;
        }
    }
}
struct Capture {
    events: Arc<ServiceEvents>,
    spans: AtomicU64,
}
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
}
impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("fleet_sync_service")
    }
    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(self.spans.fetch_add(1, Ordering::Relaxed))
    }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        match self.events.rows.lock() {
            Ok(mut rows) => rows.push(ServiceEvent { fields: fields.0 }),
            Err(_) => eprintln!("service event capture lock poisoned"),
        }
        self.events.changed.notify_waiters();
    }
}
