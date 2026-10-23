#![allow(clippy::unwrap_used)]
mod support;

use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use serde_json::{Value, json};
use sqlx_core::query::query;
use sqlx_turso::Turso;
use support::*;

fn cancel_body(id: u64) -> DeliveryCancel<TestTypes> {
    DeliveryCancel::new(
        item_id(id),
        "sender".into(),
        Recipient {
            peer: peer(2),
            address: "inbox".into(),
        },
        None,
        CancelBody { target: item_id(1) },
    )
    .unwrap()
}

fn first_reference() -> DeliveryItemRef {
    DeliveryItemRef {
        item: item_id(2),
        digest: cancel(2, 1).digest().clone(),
    }
}

async fn enqueue_cancel_at(fixture: &Fixture, id: u64) -> OutboxAdmission<Acceptance> {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes>::new(scope, limits())
                    .enqueue_cancel(cancel_body(id), peer(3), &TestConsumer)
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

async fn relay_submit(fixture: &Fixture, id: u64) -> SubmitResult<Acceptance> {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .submit(
                        peer(1),
                        cancel(id, 1),
                        SyncTime::from_unix_millis(0),
                        "fixture-route".to_owned(),
                        None,
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

async fn stored_first_cancel(fixture: &Fixture, relayed: bool) -> String {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let statement = if relayed {
                    "SELECT record FROM _fleet_sync_mail_items WHERE item_id=?1"
                } else {
                    "SELECT record FROM _fleet_sync_outbox_items WHERE item_id=?1"
                };
                use sqlx_core::row::Row;
                Ok(query::<Turso>(statement)
                    .bind(item_id(2).as_uuid().to_string())
                    .fetch_one(scope.executor())
                    .await?
                    .try_get("record")?)
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn another_outbox_cancel_names_the_first_and_does_not_take_new_custody() {
    for original_present in [false, true] {
        let fixture = Fixture::new().await;
        if original_present {
            fixture
                .store
                .transaction(|scope| {
                    Box::pin(async move {
                        let original = DeliveryItem::new(
                            item_id(1),
                            "sender".into(),
                            Recipient {
                                peer: peer(2),
                                address: "inbox".into(),
                            },
                            None,
                            "queued original".into(),
                        )
                        .unwrap();
                        Outbox::<_, TestTypes>::new(scope, limits())
                            .enqueue(original, peer(3))
                            .await
                            .map_err(storage_error)?;
                        Ok(())
                    })
                })
                .await
                .unwrap();
        }
        let first = enqueue_cancel_at(&fixture, 2).await;
        assert!(matches!(first, OutboxAdmission::Queued { .. }));
        let first_before = stored_first_cancel(&fixture, false).await;
        let same_id = serde_json::to_value(enqueue_cancel_at(&fixture, 2).await).unwrap();
        let another_id = serde_json::to_value(enqueue_cancel_at(&fixture, 3).await).unwrap();
        let new_custody = fixture
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    Outbox::<_, TestTypes>::new(scope, limits())
                        .status(item_id(3))
                        .await
                        .map_err(storage_error)
                })
            })
            .await
            .unwrap();

        assert!(matches!(new_custody.state, OutboxStatusState::Unknown));
        assert_eq!(stored_first_cancel(&fixture, false).await, first_before);
        let status = if original_present {
            json!({"kind":"cancelled"})
        } else {
            json!({"kind":"queued"})
        };
        assert_eq!(
            same_id,
            json!({"kind":"replayed","value":{"status":status,"version":0}})
        );
        assert_eq!(
            another_id,
            json!({"kind":"cancelAlreadyRequested","value":{
                "existing":first_reference(),"status":status,"version":0
            }}),
            "a different cancel ID must not masquerade as a same-ID replay"
        );
    }
}

#[tokio::test]
async fn relay_duplicate_cancel_names_canonical_identity_without_new_custody() {
    let relay = Fixture::new().await;
    let first = relay_submit(&relay, 2).await;
    assert!(matches!(first, SubmitResult::Accepted { .. }));
    let first_before = stored_first_cancel(&relay, true).await;
    let same_id = serde_json::to_value(relay_submit(&relay, 2).await).unwrap();
    let another_id = serde_json::to_value(relay_submit(&relay, 3).await).unwrap();
    let queried = relay
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .query(&[item_id(2), item_id(3)])
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();

    assert!(matches!(
        queried.statuses[1].state,
        ItemStatusState::Unknown
    ));
    assert_eq!(stored_first_cancel(&relay, true).await, first_before);
    assert_eq!(same_id["kind"], "replayed");
    assert_eq!(
        same_id["value"]["status"],
        json!({"kind":"sent","value":{"seq":1}})
    );
    assert_eq!(
        another_id,
        json!({"kind":"cancelAlreadyRequested","value":{
            "existing":first_reference(),"status":{"kind":"sent","value":{"seq":1}},
            "version":1,"cancel":{"kind":"pending"}
        }}),
        "relay must distinguish a different cancel ID from same-ID replay"
    );
}

async fn distinct_sender_status(recovered: bool) -> Value {
    let relay = Fixture::new().await;
    let sender = Fixture::new().await;
    relay_submit(&relay, 2).await;
    assert!(matches!(
        enqueue_cancel_at(&sender, 3).await,
        OutboxAdmission::Queued { .. }
    ));
    let answer = relay_submit(&relay, 3).await;
    sender
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                if recovered {
                    query::<Turso>("DELETE FROM _fleet_sync_outbox_items WHERE item_id=?1")
                        .bind(item_id(3).as_uuid().to_string())
                        .execute(scope.executor())
                        .await?;
                }
                let mut outbox = Outbox::<_, TestTypes>::new(scope, limits());
                outbox
                    .mark_submitted(
                        &SubmitInput {
                            item: item_id(3),
                            digest: cancel(3, 1).digest().clone(),
                            relay: peer(3),
                            mailbox: peer(2),
                        },
                        answer,
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let state = sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes>::new(scope, limits())
                    .status(item_id(3))
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    let state = serde_json::to_value(state).unwrap();
    let same_id = serde_json::to_value(enqueue_cancel_at(&sender, 3).await).unwrap();
    assert_eq!(
        same_id,
        json!({"kind":"replayed","value":{
            "status":state.pointer("/state/value/status").unwrap(),
            "version":state.pointer("/state/value/version").unwrap()
        }})
    );
    state
}

fn assert_distinct_refusal(state: Value) {
    assert_eq!(
        *state.pointer("/state/value/status").unwrap(),
        json!({"kind":"refused","value":{"reason":{
            "kind":"cancelAlreadyRequested","value":{"existing":first_reference()}
        }}}),
        "distinct sender cancel must not inherit canonical status"
    );
    assert_eq!(*state.pointer("/state/value/version").unwrap(), 0);
}

#[tokio::test]
async fn sender_refuses_its_distinct_cancel_instead_of_copying_first_status() {
    assert_distinct_refusal(distinct_sender_status(false).await);
}

#[tokio::test]
async fn recovered_sender_refuses_distinct_cancel_instead_of_copying_first_status() {
    assert_distinct_refusal(distinct_sender_status(true).await);
}

#[tokio::test]
async fn recovered_canonical_cancel_still_names_its_stored_identity() {
    let fixture = Fixture::new().await;
    enqueue_cancel_at(&fixture, 2).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes>::new(scope, limits())
                    .record_submission_failure(
                        &SubmitInput {
                            item: item_id(2),
                            digest: first_reference().digest,
                            relay: peer(3),
                            mailbox: peer(2),
                        },
                        DeliveryFailureKind::CustodyMissing,
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    assert!(matches!(enqueue_cancel_at(&fixture, 3).await,
        OutboxAdmission::CancelAlreadyRequested {
            existing, status: DeliveryStatus::Refused {
                reason: DeliveryRefusal::ProcessingFailed {
                    kind: DeliveryFailureKind::CustodyMissing,
                },
            }, version,
        } if existing == first_reference() && version.value() == 0
    ));
    assert!(matches!(
        enqueue_cancel_at(&fixture, 2).await,
        OutboxAdmission::Replayed { .. }
    ));
}

fn assert_strict_cancel_answer<T: serde::de::DeserializeOwned + serde::Serialize>(wire: Value) {
    let answer: T = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(answer).unwrap(), wire);
    let mut malformed = Vec::new();
    let mut outer_extra = wire.clone();
    outer_extra
        .as_object_mut()
        .unwrap()
        .insert("unexpected".into(), json!(true));
    malformed.push(outer_extra);
    let mut payload_extra = wire.clone();
    payload_extra
        .pointer_mut("/value")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("unexpected".into(), json!(true));
    malformed.push(payload_extra);
    let mut reference_extra = wire.clone();
    reference_extra
        .pointer_mut("/value/existing")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("unexpected".into(), json!(true));
    malformed.push(reference_extra);
    let mut bad_digest = wire.clone();
    *bad_digest.pointer_mut("/value/existing/digest").unwrap() = json!("not a digest");
    malformed.push(bad_digest);
    let mut missing_reference = wire.clone();
    missing_reference
        .pointer_mut("/value")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove("existing");
    malformed.push(missing_reference);
    let mut wrong_case = wire;
    *wrong_case.get_mut("kind").unwrap() = json!("cancel_already_requested");
    malformed.push(wrong_case);
    for value in malformed {
        assert!(
            serde_json::from_value::<T>(value.clone()).is_err(),
            "accepted {value}"
        );
    }
}

#[test]
fn canonical_cancel_answers_and_refusal_use_strict_camel_case_wire_contracts() {
    let local = json!({"kind":"cancelAlreadyRequested","value":{
        "existing":first_reference(),"status":{"kind":"queued"},"version":0
    }});
    let mut relayed = local.clone();
    relayed["value"]["cancel"] = json!({"kind":"pending"});
    assert_strict_cancel_answer::<OutboxAdmission<Acceptance>>(local);
    assert_strict_cancel_answer::<SubmitResult<Acceptance>>(relayed);
    assert_strict_cancel_answer::<DeliveryRefusal>(json!({
        "kind":"cancelAlreadyRequested","value":{"existing":first_reference()}
    }));
}
