#![allow(clippy::unwrap_used)]
use super::support;
use durable_steps::{AtomicStore, Engine, storage::TransactionScope, turso_store::TursoScope};
use fleet_sync_delivery::*;
use serde_json::{Value, json};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};
use support::{network::*, *};

struct ProjectionConsumer;
impl DeliveryConsumer<TursoScope<'_, TestHost>> for ProjectionConsumer {
    type Protocol = TestProtocol;
    async fn release_gate(
        &self,
        _: &mut TursoScope<'_, TestHost>,
        _: &DeliveryItem<TestTypes>,
    ) -> Result<GateDecision, DeliveryError> {
        Ok(GateDecision::Release)
    }
    async fn deliver_locally(
        &self,
        _: &DeliveryItem<TestTypes>,
        _: durable_steps::CallId,
    ) -> LocalDelivery<TestTypes> {
        // The established network host supplies the controlled recipient answer.
        LocalDelivery {
            outcome: LocalOutcome::Uncertain,
            observation: (),
        }
    }
    async fn on_outcome(
        &self,
        scope: &mut TursoScope<'_, TestHost>,
        item: ItemId,
        outcome: &RecipientOutcome<Acceptance>,
        _: Option<&()>,
    ) -> Result<(), DeliveryError> {
        query::<Turso>("INSERT INTO consumer_refusals(item,outcome) VALUES(?,?)")
            .bind(item.as_uuid().to_string())
            .bind(serde_json::to_string(outcome).unwrap())
            .execute(scope.executor())
            .await
            .map_err(|error| DeliveryError::Storage(error.to_string()))?;
        Ok(())
    }
    fn may_cancel(&self, sender: &String, item: &DeliveryHeader<TestTypes>) -> bool {
        sender == item.sender()
    }
}
type ProjectedDeliver =
    DeliverLocally<TestStore, Turso, TestTypes, ProjectionConsumer, FixtureBindings>;

fn refusal_wire(code: &str) -> Value {
    json!({"kind":"recipientRefused","value":{"code":code}})
}
fn refusal(code: &str) -> DeliveryRefusal {
    serde_json::from_value(refusal_wire(code)).unwrap()
}

#[tokio::test]
async fn coded_recipient_refusal_survives_native_delivery_report_status_and_replay() {
    for code in [
        "unsupportedMode",
        "busy",
        "staleGuard",
        "permissionDenied",
        "ownedElsewhere",
        "rejectedUnclassified",
        &"R".repeat(64),
    ] {
        let network = NetworkFixture::new().await;
        network
            .recipient
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    query::<Turso>(
                        "CREATE TABLE consumer_refusals(item TEXT NOT NULL,outcome TEXT NOT NULL)",
                    )
                    .execute(scope.executor())
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        // Only the external local-recipient answer is controlled. All custody,
        // report, publication, journal and status owners below remain real.
        network
            .routes
            .local_answers
            .lock()
            .unwrap()
            .push_back(LocalOutcome::Refused {
                reason: refusal(code),
            });
        network.enqueue(1, "body refused by native recipient").await;
        drain(&network.engine(&network.sender)).await;
        assert!(matches!(
            sender_status(&network, 1).await,
            OutboxStatusState::Known {
                status: DeliveryStatus::Sent,
                ..
            }
        ));
        let mut recipient = Engine::new(
            network.recipient.store.clone(),
            network.recipient.host.clone(),
        );
        recipient
            .register(ProjectedDeliver::new(
                config().local,
                Arc::new(ProjectionConsumer),
                network.recipient.host.clone(),
                std::sync::Arc::new(FixtureBindings),
            ))
            .unwrap();
        recipient
            .register(ReportTask::new(
                config(),
                network.recipient.host.clone(),
                std::sync::Arc::new(FixtureBindings),
            ))
            .unwrap();
        drain(&network.engine(&network.relay)).await;
        for _ in 0..1 {
            let report = recipient.drive_due().await.unwrap();
            assert!(report.failed.is_empty() && report.retried.is_empty());
        }
        assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
        let expected_outcome = json!({"kind":"refused","value":{"reason":refusal_wire(code)}});
        let expected_status = expected_outcome.clone();
        let outcome = network
            .recipient
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let mut inbox =
                        Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings);
                    let outcome = inbox
                        .local_outcome(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .unwrap();
                    let report = inbox
                        .resolve_report(ReportRef {
                            item: item_id(1),
                            report_seq: ReportSeq::new(1),
                        })
                        .await
                        .map_err(storage_error)?;
                    Ok((
                        serde_json::to_value(outcome).unwrap(),
                        serde_json::to_value(report.outcome).unwrap(),
                    ))
                })
            })
            .await
            .unwrap();
        assert_eq!(outcome.0, expected_outcome);
        assert_eq!(
            outcome.1, expected_outcome,
            "real owed report changed the code"
        );
        drain(&recipient).await;
        drain(&network.engine(&network.relay)).await;
        drain(&network.engine(&network.status)).await;
        let mailbox = network
            .relay
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let result = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .relay_query(Submitter::Machine(peer(1)), item_id(1))
                        .await
                        .map_err(storage_error)?;
                    let ItemStatusState::Known { status, .. } = &result.state else {
                        return Err(durable_steps::StoreError::backend(
                            "missing real mailbox status",
                        ));
                    };
                    Ok(serde_json::to_value(status).unwrap())
                })
            })
            .await
            .unwrap();
        assert_eq!(mailbox, expected_status);
        let remote = sender_status(&network, 1).await;
        let OutboxStatusState::Known { status, .. } = remote else {
            panic!("missing sender status");
        };
        assert!(status.is_terminal());
        assert_eq!(serde_json::to_value(status).unwrap(), expected_status);
        let fetched = network
            .relay
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let events = StatusStream::<_, TestTypes>::new(
                        scope,
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        "fixture-route".into(),
                    )
                    .fetch(
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        StatusCursor::new(0),
                        100,
                    )
                    .await
                    .map_err(storage_error)?;
                    Ok(events
                        .events
                        .into_iter()
                        .map(|event| serde_json::to_value(event.status).unwrap())
                        .collect::<Vec<_>>())
                })
            })
            .await
            .unwrap();
        assert_eq!(
            fetched.last(),
            Some(&expected_status),
            "remote status stream changed code"
        );
        let projection = network
            .recipient
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let rows = query::<Turso>("SELECT outcome FROM consumer_refusals")
                        .fetch_all(scope.executor())
                        .await?;
                    Ok(rows
                        .into_iter()
                        .map(|row| {
                            serde_json::from_str::<Value>(
                                &row.try_get::<String, _>("outcome").unwrap(),
                            )
                            .unwrap()
                        })
                        .collect::<Vec<_>>())
                })
            })
            .await
            .unwrap();
        assert_eq!(projection, [expected_outcome]);
        let replay = network
            .recipient
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    accept_fixture_push(
                        scope,
                        limits(),
                        item(1, "body refused by native recipient"),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)
                })
            })
            .await
            .unwrap();
        assert!(matches!(replay, PushResult::Replayed { .. }));
        assert_eq!(
            serde_json::to_value(replay).unwrap()["value"]["status"],
            expected_status
        );
        network.advance();
        drain(&recipient).await;
        drain(&network.engine(&network.sender)).await;
        assert_eq!(
            network.routes.local_calls.load(Ordering::SeqCst),
            1,
            "terminal refusal retried recipient"
        );
        assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn maximum_coded_settlement_fits_the_exact_minimum_and_lower_configuration_refuses() {
    assert_settlement_fits_minimum::<TestTypes>(refusal(&"R".repeat(64))).await;
}

struct UnitAcceptanceTypes;
impl DeliveryTypes for UnitAcceptanceTypes {
    const MAX_SENDER_BYTES: u32 = 64;
    const MAX_ADDRESS_BYTES: u32 = 64;
    const MAX_ACCEPTANCE_BYTES: u32 = 4;
    const MAX_STATUS_ROUTE_BYTES: u32 = 64;
    const MAX_LOCAL_OBSERVATION_BYTES: u32 = 4;
    type Body = String;
    type Sender = String;

    type Address = String;
    type Acceptance = ();
    type StatusRoute = String;
    type LocalObservation = ();
}

#[tokio::test]
async fn canonical_cancel_refusal_fits_the_exact_minimum() {
    assert_settlement_fits_minimum::<UnitAcceptanceTypes>(
        DeliveryRefusal::CancelAlreadyRequested {
            existing: DeliveryItemRef {
                item: item_id(2),
                digest: cancel(2, 1).digest().clone(),
            },
        },
    )
    .await;
}

async fn assert_settlement_fits_minimum<
    D: DeliveryTypes<Sender = String, Body = String, Address = String, StatusRoute = String>,
>(
    terminal_refusal: DeliveryRefusal,
) {
    let minimum = minimum_settlement_allowance::<D>().unwrap();
    assert!(
        matches!(DeliveryLimits::new::<D>(100_000,20,100_000,minimum-1),Err(DeliveryConfigError::SettlementAllowanceTooSmall {supplied,minimum:required}) if supplied==minimum-1&&required==minimum)
    );
    let fixture = Fixture::new().await;
    fixture.store.transaction(move |scope|Box::pin(async move {
        let configured=DeliveryLimits::new::<D>(100_000,20,100_000,minimum).unwrap();
        for identity in [1,2] {
            let envelope=if identity==1 {item(1,"body")} else {cancel(2,1)};
            let envelope:DeliveryEnvelope<D>=serde_json::from_value(serde_json::to_value(envelope).unwrap()).unwrap();
            let reference=DeliveryItemRef {item:envelope.id(),digest:envelope.digest().clone()};
            let mut relay=Outbox::<_,D,_>::new(scope,configured,&FixtureBindings);
            assert!(matches!(relay.submit_relay::<FixturePush<D>,RecordRemoval>(Submitter::Machine(peer(1)),envelope,peer(3),"a".repeat(62),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?,SubmitResult::Accepted {..}));
            relay.receipt::<FixturePublication<D>>(peer(2),&reference,PushResult::Accepted {accepted_at:SyncTime::from_unix_millis(0)},SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
            for (sequence,outcome) in [(1,RecipientOutcome::Held),(2,RecipientOutcome::Refused {reason:terminal_refusal.clone()})] {
                assert!(matches!(relay.recipient_report::<FixturePublication<D>>(peer(2),RecipientReport {item:item_id(identity),report_seq:ReportSeq::new(sequence),outcome},SyncTime::from_unix_millis(0)).await.map_err(storage_error)?,ReportResult::Ack {..}));
            }
        }
        let publications=query::<Turso>(include_str!("../support/sql/read_publication_headers.sql")).fetch_all(scope.executor()).await?;
        assert_eq!(publications.len(),2,"one current publication per item at the fixed Project/Machine route");
        let expected=json!({"kind":"refused","value":{"reason":terminal_refusal}});
        let mut actual_bytes=serde_json::to_vec(&cancel(2,1)).unwrap().len();
        let mut maximum_publication_bytes=0;
        for row in publications {
            let target:String=row.try_get("target")?;
            let record:String=row.try_get("record")?;
            let mut publication:Value=serde_json::from_str(&record).unwrap();
            assert_eq!(*publication.get("status").unwrap(),expected);
            actual_bytes+=target.len()+record.len()+3;
            *publication.get_mut("version").unwrap()=json!(i64::MAX);
            maximum_publication_bytes=maximum_publication_bytes.max(target.len()+serde_json::to_vec(&publication).unwrap().len()+3);
        }
        // Read actual typed row values independently of the allowance's Rust
        // placeholder representation. This is a logical value-byte bound, not
        // a SQLite page or index-size claim.
        let rows=query::<Turso>("SELECT status_kind,settlement_bytes,CAST(json_array(status_route,item_id,live,payload_bytes,settlement_charge_bytes,payload,capacity_slot,record_kind,consumer_sender,digest,relay_machine_id,submitter_kind,submitter_id,recipient_machine_id,valid_until,status_kind,status_sequence,status_acceptance,refusal_kind,refusal_code,refusal_failure,refusal_size_field,refusal_digest,refusal_item_id,status_version,last_report_seq,submit_attempted,cancel_outcome,cancel_too_late_at,cancel_target_item_id,acceptance_bound,settlement_bytes,failure_kind,created_at,admitted_at,handed_off_at,updated_at) AS TEXT) AS retained FROM _fleet_sync_outbox_items").fetch_all(scope.executor()).await?;
        assert_eq!(rows.len(),2);
        let mut reserved=0;
        let mut header_bytes=0;
        for row in rows {
            assert_eq!(row.try_get::<String,_>("status_kind")?,"refused");
            reserved+=row.try_get::<i64,_>("settlement_bytes")?;
            header_bytes+=row.try_get::<String,_>("retained")?.len();
        }
        actual_bytes+=header_bytes;
        assert_eq!(reserved+i64::try_from(serde_json::to_vec(&cancel(2,1)).unwrap().len()).unwrap(),i64::from(minimum));
        assert!(actual_bytes<=usize::try_from(minimum).unwrap(),"retained typed settlement {actual_bytes} exceeds minimum {minimum}");
        let required=header_bytes+8*maximum_publication_bytes+serde_json::to_vec(&cancel(2,1)).unwrap().len();
        assert!(required<=usize::try_from(minimum).unwrap(),"bounded status snapshots require {required}, exceeding minimum {minimum}");
        Ok(())
    })).await.unwrap();
}
