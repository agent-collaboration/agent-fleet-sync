use crate::{CancelResult, DeliveryRefusal, DeliveryStatus, ItemDigest, SyncTime};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum PushResult<A = ()> {
    Accepted {
        accepted_at: SyncTime,
    },
    Replayed {
        status: DeliveryStatus<A>,
        cancel: Option<CancelResult>,
    },
    Conflict {
        existing: ItemDigest,
    },
    Refused {
        reason: DeliveryRefusal,
    },
}
