use crate::{ApplyFollowUp, FollowUpInput, InstalledSnapshot};
use durable_steps::{Admitted, AtomicStore, JournalScope, RequestKey, StartRefusal};
use fleet_sync_protocol::{DatabaseId, ReplicationPosition};
use serde::{Serialize, de::DeserializeOwned};

/// Snapshot and piece producers may owe work at the same position in the retained journal.
pub fn snapshot_follow_up_key(
    database: DatabaseId,
    position: ReplicationPosition,
    index: u32,
) -> Result<RequestKey, StartRefusal> {
    let key = serde_json::to_string(&("snapshot", database, position, index))
        .map_err(durable_steps::StoreError::backend)?;
    RequestKey::new(key)
        .map_err(durable_steps::StoreError::backend)
        .map_err(Into::into)
}

/// Admit only a first publication's returned items, before the host commits its transaction.
pub async fn admit_snapshot_follow_ups<S, F>(
    scope: &mut S::Scope<'_>,
    installed: &InstalledSnapshot,
    items: Vec<F>,
) -> Result<Vec<Admitted>, StartRefusal>
where
    S: AtomicStore,
    F: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let mut admitted = Vec::with_capacity(items.len());
    for (index, item) in items.into_iter().enumerate() {
        let index = u32::try_from(index).map_err(durable_steps::StoreError::backend)?;
        let key = snapshot_follow_up_key(installed.database, installed.position, index)?;
        admitted.push(
            scope
                .admit::<ApplyFollowUp<S, F>>(
                    &key,
                    &FollowUpInput {
                        database: installed.database,
                        epoch: installed.position.epoch,
                        through: installed.position,
                        index,
                        item,
                    },
                )
                .await?,
        );
    }
    Ok(admitted)
}
