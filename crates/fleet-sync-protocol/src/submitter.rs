use crate::{DatabaseId, FleetId, PeerId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The owner asserted by a consumer's sender; the host authorizes that assertion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum Submitter {
    Machine(PeerId),
    Project(DatabaseId),
    Fleet(FleetId),
}

impl Submitter {
    pub const fn as_uuid(self) -> Uuid {
        match self {
            Self::Machine(peer) => peer.as_uuid(),
            Self::Project(project) => project.as_uuid(),
            Self::Fleet(fleet) => fleet.as_uuid(),
        }
    }
}

#[cfg(test)]
#[path = "submitter_test.rs"]
mod tests;
