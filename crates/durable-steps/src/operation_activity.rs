//! Per-operation drive ownership and transient, committed terminal receipts.

use crate::{OperationId, OperationState, OperationStatus, storage::OperationOutcome};
use futures_util::lock::Mutex;
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};

pub(super) type OperationActivity = Mutex<Option<OperationReceipt>>;

pub(super) struct OperationReceipt {
    kind: String,
    outcome: OperationOutcome,
}

impl OperationReceipt {
    pub(super) fn new(kind: String, outcome: OperationOutcome) -> Self {
        Self { kind, outcome }
    }

    pub(super) fn status(&self, id: OperationId) -> OperationStatus {
        OperationStatus {
            id,
            kind: self.kind.clone(),
            state: match &self.outcome {
                OperationOutcome::Completed { result } => OperationState::Completed {
                    result: result.clone(),
                },
                OperationOutcome::Failed { reason } => OperationState::Failed {
                    reason: reason.clone(),
                },
            },
        }
    }
}

#[derive(Default)]
pub(super) struct OperationActivities {
    entries: Mutex<BTreeMap<OperationId, Weak<OperationActivity>>>,
}

impl OperationActivities {
    pub(super) async fn attach(&self, operation: OperationId) -> Arc<OperationActivity> {
        let mut entries = self.entries.lock().await;
        // The catalog owns no receipt. Only a drive or admitted settler does.
        entries.retain(|_, activity| activity.strong_count() > 0);
        if let Some(activity) = entries.get(&operation).and_then(Weak::upgrade) {
            return activity;
        }
        let activity = Arc::new(Mutex::new(None));
        entries.insert(operation, Arc::downgrade(&activity));
        activity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipts_drop_with_the_last_holder_and_the_catalog_stays_bounded()
    -> Result<(), Box<dyn std::error::Error>> {
        futures_executor::block_on(async {
            let activities = OperationActivities::default();
            for ordinal in 1..=512 {
                let id = OperationId::new(crate::Uuid::from_u128(
                    0x019eef00000070008000000000000000 + ordinal,
                ))?;
                let activity = activities.attach(id).await;
                let second = activities.attach(id).await;
                if !Arc::ptr_eq(&activity, &second) {
                    return Err("the same operation received two activity owners".into());
                }
                *activity.lock().await = Some(OperationReceipt::new(
                    "pruned-memory-case".into(),
                    OperationOutcome::Completed {
                        result: vec![7; 1024],
                    },
                ));
                let weak = Arc::downgrade(&activity);
                drop(activity);
                if weak.upgrade().is_none() {
                    return Err("another holder must retain the receipt".into());
                }
                drop(second);
                if weak.upgrade().is_some() {
                    return Err("no receipt may survive its last holder".into());
                }
                if activities.entries.lock().await.len() != 1 {
                    return Err("the activity catalog retained inactive entries".into());
                }
            }
            Ok(())
        })
    }
}
