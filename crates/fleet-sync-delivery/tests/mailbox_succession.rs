#![allow(clippy::unwrap_used)]
mod support;

use durable_steps::{AtomicStore, SyncTime, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use std::sync::Arc;
use support::*;

const ORIGINAL_ROUTE: &str = "6b99f3e1-a002-4f42-9101-338578c72101";
const SUCCESSOR_ROUTE: &str = "6b99f3e1-a002-4f42-9101-338578c72102";

async fn submit_mail(
    store: &TestStore,
    origin: PeerId,
    route: &str,
    rank: Option<u64>,
    identity: u64,
    body: &str,
) -> SubmitResult<Acceptance> {
    let route = route.to_owned();
    let envelope = item(identity, body);
    store
        .transaction(move |scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .submit(origin, envelope, SyncTime::from_unix_millis(0), route, rank)
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

async fn stored_mail(store: &TestStore, identity: u64) -> serde_json::Value {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_mail_items WHERE item_id=?1",
                )
                .bind(item_id(identity).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await?;
                let record = row.try_get::<String, _>("record")?;
                serde_json::from_str(&record).map_err(durable_steps::StoreError::backend)
            })
        })
        .await
        .unwrap()
}

fn status_reference(
    origin: PeerId,
    route: &str,
    identity: u64,
    version: u64,
) -> PublicationRef<String> {
    PublicationRef {
        item: item_id(identity),
        version: StatusVersion::new(version),
        target: PublicationTarget::StatusStream {
            sender: origin,
            route: route.to_owned(),
        },
    }
}

async fn resolve_status(
    store: &TestStore,
    reference: PublicationRef<String>,
) -> Option<StatusPublication<Acceptance>> {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                resolve_publication::<_, TestTypes>(scope, reference)
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn ranked_replay_preserves_origin_and_old_publications_while_future_status_uses_the_successor()
 {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    assert!(PeerId::new(ORIGINAL_ROUTE.parse().unwrap()).is_err());
    assert!(
        matches!(submit_mail(&store, peer(1), ORIGINAL_ROUTE, Some(4), 1, "original body").await,
        SubmitResult::Accepted { seq, version, .. } if seq.value()==1 && version.value()==1)
    );
    let original = stored_mail(&store, 1).await;
    let old_reference = status_reference(peer(1), ORIGINAL_ROUTE, 1, 1);
    assert!(
        resolve_status(&store, old_reference.clone())
            .await
            .is_some()
    );

    assert!(
        matches!(submit_mail(&store, peer(3), SUCCESSOR_ROUTE, Some(5), 1, "original body").await,
        SubmitResult::Replayed { status: DeliveryStatus::Sent { seq }, version, .. }
            if seq.value()==1 && version.value()==1)
    );
    for (origin, route, rank) in [
        (peer(1), ORIGINAL_ROUTE, Some(4)),
        (peer(4), ORIGINAL_ROUTE, Some(5)),
        (peer(1), ORIGINAL_ROUTE, None),
    ] {
        assert!(
            matches!(submit_mail(&store, origin, route, rank, 1, "original body").await,
            SubmitResult::Replayed { version, .. } if version.value()==1)
        );
    }
    assert!(matches!(
        submit_mail(
            &store,
            peer(4),
            ORIGINAL_ROUTE,
            Some(100),
            1,
            "conflicting body"
        )
        .await,
        SubmitResult::Refused {
            reason: DeliveryRefusal::ItemConflict { .. }
        }
    ));
    let moved = stored_mail(&store, 1).await;
    assert_eq!(moved["value"]["origin"], original["value"]["origin"]);
    assert_eq!(moved["value"]["payload"], original["value"]["payload"]);
    assert_eq!(
        moved["value"]["statusTarget"],
        serde_json::json!({
            "peer":peer(3), "route":SUCCESSOR_ROUTE, "rank":5,
        })
    );
    assert!(
        resolve_status(&store, status_reference(peer(3), SUCCESSOR_ROUTE, 1, 1))
            .await
            .is_none()
    );

    drop(store);
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
            .await
            .unwrap(),
    );
    store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .report(
                        peer(2),
                        RecipientReport {
                            item: item_id(1),
                            report_seq: ReportSeq::new(1),
                            outcome: RecipientOutcome::Held,
                        },
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let successor_reference = status_reference(peer(3), SUCCESSOR_ROUTE, 1, 2);
    assert!(matches!(
        resolve_status(&store, successor_reference.clone())
            .await
            .unwrap()
            .status,
        DeliveryStatus::Held
    ));
    assert!(
        resolve_status(&store, old_reference.clone())
            .await
            .is_some()
    );
    store
        .transaction(move |scope| {
            Box::pin(async move {
                retire_publication::<_, TestTypes>(scope, old_reference)
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    assert!(resolve_status(&store, successor_reference).await.is_some());
}

#[tokio::test]
async fn absent_rank_is_lower_than_zero_and_none_never_moves_a_live_target() {
    let fixture = Fixture::new().await;
    submit_mail(&fixture.store, peer(1), ORIGINAL_ROUTE, None, 1, "body").await;
    submit_mail(&fixture.store, peer(3), SUCCESSOR_ROUTE, Some(0), 1, "body").await;
    submit_mail(&fixture.store, peer(1), ORIGINAL_ROUTE, None, 1, "body").await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let answer = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .retire(
                        peer(3),
                        &call("current nonterminal"),
                        &[item_id(1), item_id(99)],
                    )
                    .await
                    .map_err(storage_error)?;
                assert_eq!(answer.unknown, [item_id(1), item_id(99)]);
                assert!(answer.retired.is_empty());
                assert!(answer.not_current_target.is_empty());
                Ok(())
            })
        })
        .await
        .unwrap();
    let record = stored_mail(&fixture.store, 1).await;
    assert_eq!(record["value"]["origin"], serde_json::json!(peer(1)));
    assert_eq!(
        record["value"]["statusTarget"],
        serde_json::json!({
            "peer":peer(3), "route":SUCCESSOR_ROUTE, "rank":0,
        })
    );
}

#[tokio::test]
async fn predecessor_prefix_and_successor_suffix_retire_per_item_and_compacted_replay_never_resurrects()
 {
    let fixture = Fixture::new().await;
    for identity in [1, 2, 3] {
        submit_mail(
            &fixture.store,
            peer(1),
            ORIGINAL_ROUTE,
            Some(1),
            identity,
            "body",
        )
        .await;
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .report(
                            peer(2),
                            RecipientReport {
                                item: item_id(identity),
                                report_seq: ReportSeq::new(1),
                                outcome: RecipientOutcome::Delivered {
                                    acceptance: Acceptance {
                                        receipt: "done".into(),
                                    },
                                },
                            },
                        )
                        .await
                        .map_err(storage_error)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
    // The predecessor may settle its acknowledged prefix before takeover.
    let prefix = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .retire(peer(1), &call("prefix retirement"), &[item_id(1)])
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    assert_eq!(prefix.retired, [item_id(1)]);
    let compacted_prefix = stored_mail(&fixture.store, 1).await;
    assert!(
        matches!(submit_mail(&fixture.store, peer(3), SUCCESSOR_ROUTE, Some(2), 1, "body").await,
        SubmitResult::Replayed { status:DeliveryStatus::Delivered { .. }, version, .. }
            if version.value()==2)
    );
    assert_eq!(stored_mail(&fixture.store, 1).await, compacted_prefix);
    submit_mail(&fixture.store, peer(3), SUCCESSOR_ROUTE, Some(2), 2, "body").await;
    // A still-unretired prefix remains authorized after the suffix moves.
    let mixed = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .retire(
                        peer(1),
                        &call("mixed after takeover"),
                        &[item_id(1), item_id(2), item_id(3), item_id(99)],
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    assert_eq!(mixed.retired, [item_id(1), item_id(3)]);
    assert_eq!(mixed.not_current_target, [item_id(2)]);
    assert_eq!(mixed.unknown, [item_id(99)]);
    assert_eq!(stored_mail(&fixture.store, 2).await["kind"], "live");

    // Replay the exact committed identity after takeover.
    let suffix = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .retire(peer(3), &call("suffix retirement"), &[item_id(2)])
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    assert_eq!(suffix.retired, [item_id(2)]);
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                assert_eq!(
                    mailbox
                        .retire(peer(3), &call("suffix retirement"), &[item_id(2)])
                        .await
                        .map_err(storage_error)?,
                    suffix
                );
                assert!(matches!(
                    mailbox
                        .retire(peer(1), &call("suffix retirement"), &[item_id(2)])
                        .await,
                    Err(DeliveryError::CallConflict)
                ));
                assert_eq!(
                    mailbox
                        .retire(peer(1), &call("prefix retirement"), &[item_id(1)])
                        .await
                        .map_err(storage_error)?,
                    prefix
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(
        matches!(submit_mail(&fixture.store, peer(4), ORIGINAL_ROUTE, Some(100), 2, "body").await,
        SubmitResult::Replayed { status:DeliveryStatus::Delivered { acceptance }, version, .. }
            if acceptance.receipt=="done" && version.value()==2)
    );
    let compacted = stored_mail(&fixture.store, 2).await;
    assert_eq!(compacted["kind"], "compacted");
    assert!(compacted["value"].get("statusTarget").is_none());
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                let fresh = mailbox
                    .retire(peer(4), &call("already compacted"), &[item_id(2)])
                    .await
                    .map_err(storage_error)?;
                assert_eq!(fresh.retired, [item_id(2)]);
                let controls = mailbox
                    .fetch(
                        peer(2),
                        DeliveryStream::Controls,
                        DeliverySeq::new(0),
                        10,
                        100_000,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert_eq!(controls.entries.len(), 3);
                assert_eq!(controls.through.value(), 3);
                Ok(())
            })
        })
        .await
        .unwrap();
}
