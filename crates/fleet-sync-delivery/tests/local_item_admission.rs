#![allow(clippy::unwrap_used)]
#[path = "support/local_cancellation.rs"]
mod local_cancellation;
#[path = "support/projection.rs"]
mod projection;
mod support;

use durable_steps::{AtomicStore, storage::JournalStorage};
use fleet_sync_delivery::*;
use local_cancellation::{enqueue_cancel, local_item};
use projection::{ProjectionConsumer, prepare_projection_table, read_committed_state};
use support::{network::DeliverTask, *};

#[derive(Clone, Copy)]
enum AdmissionApi {
    Custody,
    AndAdmit,
}
async fn admit_item(fixture: &Fixture, api: AdmissionApi) -> LocalItemAdmission {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                match api {
                    AdmissionApi::Custody => {
                        inbox
                            .enqueue_local(
                                local_item(1),
                                &ProjectionConsumer::default(),
                                SyncTime::from_unix_millis(0),
                            )
                            .await
                    }
                    AdmissionApi::AndAdmit => {
                        inbox
                            .enqueue_local_and_admit::<DeliverTask, _>(
                                local_item(1),
                                &ProjectionConsumer::default(),
                                SyncTime::from_unix_millis(0),
                            )
                            .await
                    }
                }
                .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}
async fn due_count(fixture: &Fixture) -> usize {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move { Ok(scope.due(SyncTime::from_unix_millis(0)).await?.len()) })
        })
        .await
        .unwrap()
}
fn assert_prevented_projection(state: &projection::CommittedState) {
    let original = item_id(1).as_uuid().to_string();
    let (_, _, value) = state
        .projections
        .iter()
        .find(|(item, kind, _)| item == &original && kind == "outcome")
        .unwrap();
    let outcome: RecipientOutcome<Acceptance> = serde_json::from_str(value).unwrap();
    assert!(matches!(outcome, RecipientOutcome::Prevented { cancel } if cancel == item_id(2)));
    assert_eq!(
        state.projections.len(),
        2,
        "one outcome for the original and one for its cancel"
    );
}

#[tokio::test]
async fn a_fresh_pending_local_item_returns_its_attempt() {
    for api in [AdmissionApi::Custody, AdmissionApi::AndAdmit] {
        let fixture = Fixture::new().await;
        prepare_projection_table(&fixture).await;
        let admission = admit_item(&fixture, api).await;
        assert!(matches!(admission, LocalItemAdmission::Attempt(_)));
        let input = require_local_attempt(admission).unwrap();
        assert_eq!(input.item, item_id(1));
        assert_eq!(input.attempt, 0);
        assert_eq!(input.not_before, SyncTime::from_unix_millis(0));
        let expected_due = match api {
            AdmissionApi::Custody => 0,
            AdmissionApi::AndAdmit => 1,
        };
        assert_eq!(due_count(&fixture).await, expected_due);
        assert!(read_committed_state(&fixture).await.projections.is_empty());
    }
}

#[tokio::test]
async fn a_fresh_settled_local_item_returns_settled_after_projecting_its_outcome() {
    for api in [AdmissionApi::Custody, AdmissionApi::AndAdmit] {
        let fixture = Fixture::new().await;
        prepare_projection_table(&fixture).await;
        enqueue_cancel(&fixture, 2, 1).await;
        let admission = admit_item(&fixture, api).await;
        let state = read_committed_state(&fixture).await;
        assert_prevented_projection(&state);
        assert_eq!(
            due_count(&fixture).await,
            0,
            "no delivery attempt was admitted"
        );
        assert!(
            matches!(admission, LocalItemAdmission::SettledOnAdmission),
            "fresh settlement must not be classified as replay"
        );
    }
}

#[tokio::test]
async fn replay_after_local_settlement_adds_no_rows_hooks_or_attempt() {
    for api in [AdmissionApi::Custody, AdmissionApi::AndAdmit] {
        let fixture = Fixture::new().await;
        prepare_projection_table(&fixture).await;
        enqueue_cancel(&fixture, 2, 1).await;
        // Arrange actual terminal custody; the next call is the replay under test.
        admit_item(&fixture, api).await;
        let before = read_committed_state(&fixture).await;
        assert_prevented_projection(&before);
        let replay = admit_item(&fixture, api).await;
        assert!(matches!(replay, LocalItemAdmission::Replayed));
        assert_eq!(read_committed_state(&fixture).await, before);
        assert_eq!(due_count(&fixture).await, 0);
    }
}
