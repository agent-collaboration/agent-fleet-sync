use serde::{Deserialize, Serialize};

use crate::{CommitSequence, Epoch};

/// A position in one holder epoch. Ordering is epoch first, then commit ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationPosition {
    pub epoch: Epoch,
    pub sequence: CommitSequence,
}

impl ReplicationPosition {
    #[must_use]
    pub const fn new(epoch: Epoch, sequence: CommitSequence) -> Self {
        Self { epoch, sequence }
    }
}
