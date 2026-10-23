//! Seed replies pass through the real storage rollback boundary and classifier.
use durable_steps::{AtomicStore, durable_object_store::DurableObjectStore};
use fleet_sync_protocol::{DatabaseId, ReplicationAnswer, SeedAttemptId, SeedComplete, SeedPage};
use fleet_sync_receiver::{
    SeedAuthority, SeedDecision, classify_copy_store_error, receiver_rollback,
};
use runtime_primitives::AlarmOwner;
use sqlx_durable_object::DoConnection;
use std::sync::Arc;
use worker::State;

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum SeedInput {
    Begin {
        database: DatabaseId,
        attempt: SeedAttemptId,
    },
    Page {
        page: SeedPage,
    },
    Complete {
        done: SeedComplete,
    },
}
pub async fn run(state: &State, input: SeedInput) -> worker::Result<ReplicationAnswer> {
    let store = DurableObjectStore::new(
        DoConnection::new(state.storage()),
        Arc::new(crate::copy_certainty::ProbeHost),
        AlarmOwner::new(state.storage()),
    );
    store.migrate().await.map_err(crate::error)?;
    let result = store
        .transaction(move |scope| {
            Box::pin(async move {
                let authority = SeedAuthority::Admin;
                match input {
                    SeedInput::Begin { database, attempt } => Ok(ReplicationAnswer::SeedStarted {
                        seed: fleet_sync_receiver::seed_begin(scope, database, attempt, &authority)
                            .await
                            .map_err(receiver_rollback)?,
                    }),
                    SeedInput::Page { page } => Ok(ReplicationAnswer::SeedPage(
                        fleet_sync_receiver::seed_page(scope, &page, &authority)
                            .await
                            .map_err(receiver_rollback)?,
                    )),
                    SeedInput::Complete { done } => {
                        let answer = fleet_sync_receiver::seed_complete(
                            scope,
                            &done,
                            &crate::ProbeHook { refusal: None },
                            &authority,
                        )
                        .await
                        .map_err(receiver_rollback)?;
                        Ok(ReplicationAnswer::SeedComplete(match answer {
                            SeedDecision::Published { position, .. } => {
                                fleet_sync_protocol::SeedAnswer::Published { position }
                            }
                            SeedDecision::Answer(answer) => answer,
                        }))
                    }
                }
            })
        })
        .await;
    Ok(match result {
        Ok(answer) => answer,
        Err(error) => classify_copy_store_error(error).answer(),
    })
}
