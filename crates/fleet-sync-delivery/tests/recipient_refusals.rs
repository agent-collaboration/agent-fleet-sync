#![allow(clippy::unwrap_used)]
mod support;
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
type ProjectedReceive = ReceiveItems<TestStore, Turso, TestTypes, ProjectionConsumer>;
type ProjectedDeliver = DeliverLocally<TestStore, Turso, TestTypes, ProjectionConsumer>;

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
                status: DeliveryStatus::Sent { .. },
                ..
            }
        ));
        let mut recipient = Engine::new(
            network.recipient.store.clone(),
            network.recipient.host.clone(),
        );
        recipient
            .register(ProjectedReceive::new(
                config(),
                Arc::new(ProjectionConsumer),
            ))
            .unwrap();
        recipient
            .register(ProjectedDeliver::new(
                config().local,
                Arc::new(ProjectionConsumer),
                network.recipient.host.clone(),
            ))
            .unwrap();
        recipient.register(ReportTask::new(config())).unwrap();
        network.receive(DeliveryStream::Items).await;
        for _ in 0..2 {
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
                    let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
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
        drain(&network.engine(&network.mailbox)).await;
        drain(&network.engine(&network.status)).await;
        let mailbox = network
            .mailbox
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let result = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .query(&[item_id(1)])
                        .await
                        .map_err(storage_error)?;
                    let ItemStatusState::Known { status, .. } =
                        &result.statuses.first().unwrap().state
                    else {
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
            .status
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let events =
                        StatusStream::<_, TestTypes>::new(scope, peer(1), "fixture-route".into())
                            .fetch(peer(1), StatusCursor::new(0), 100)
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
        network.receive(DeliveryStream::Items).await;
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
    let minimum = minimum_settlement_allowance::<TestTypes>().unwrap();
    assert!(
        matches!(DeliveryLimits::new::<TestTypes>(100_000,20,100_000,20,100_000,minimum-1),
        Err(DeliveryConfigError::SettlementAllowanceTooSmall { supplied, minimum:required })
        if supplied==minimum-1 && required==minimum)
    );
    let fixture = Fixture::new().await;
    fixture.store.transaction(move |scope| Box::pin(async move {
        let configured=DeliveryLimits::new::<TestTypes>(100_000,20,100_000,20,100_000,minimum).unwrap();
        let mut mailbox=Mailbox::<_,TestTypes>::new(scope,peer(2),configured);
        for identity in [1,2] {
            let envelope=if identity==1 { item(1,"body") } else { cancel(2,1) };
            mailbox.submit(peer(1),envelope,SyncTime::from_unix_millis(0),"a".repeat(62),Some(0)).await.map_err(storage_error)?;
        }
        for (rank,marker) in [(1,'b'),(2,'c'),(3,'d')] {
            for identity in [1,2] {
                let envelope=if identity==1 { item(1,"body") } else { cancel(2,1) };
                mailbox.submit(peer(u128::from(rank)+2),envelope,SyncTime::from_unix_millis(0),
                    marker.to_string().repeat(62),Some(rank)).await.map_err(storage_error)?;
                if rank==1 {
                    let stream=if identity==1 { DeliveryStream::Items } else { DeliveryStream::Controls };
                    let batch=mailbox.fetch(peer(2),stream,DeliverySeq::new(0),10,100_000,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
                    mailbox.received(peer(2),stream,batch.through,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
                } else {
                    let outcome=if rank==2 { RecipientOutcome::Held } else {
                        RecipientOutcome::Refused { reason:refusal(&"R".repeat(64)) }
                    };
                    mailbox.report(peer(2),RecipientReport { item:item_id(identity),report_seq:ReportSeq::new(rank),outcome }).await.map_err(storage_error)?;
                }
            }
        }
        let publications=query::<Turso>("SELECT target,record FROM _fleet_sync_status_owed").fetch_all(scope.executor()).await?;
        assert_eq!(publications.len(),8,"four retained routes for each item");
        let expected=json!({"kind":"refused","value":{"reason":refusal_wire(&"R".repeat(64))}});
        let mut bytes=serde_json::to_vec(&cancel(2,1)).unwrap().len();
        let mut coded_publications=0;
        for row in publications {
            let target:String=row.try_get("target")?; let record:String=row.try_get("record")?;
            let publication:Value=serde_json::from_str(&record).unwrap();
            if publication["status"]["kind"]=="refused" {
                assert_eq!(publication["status"],expected);
                coded_publications+=1;
            }
            bytes+=target.len()+record.len()+3;
        }
        assert_eq!(coded_publications,2,"both final item/control statuses keep the maximum code");
        let mut reserved=0;
        for row in query::<Turso>("SELECT record,settlement_bytes FROM _fleet_sync_mail_items").fetch_all(scope.executor()).await? {
            let record:String=row.try_get("record")?;
            assert_eq!(serde_json::from_str::<Value>(&record).unwrap()["value"]["status"],expected);
            reserved+=row.try_get::<i64,_>("settlement_bytes")?;
            bytes+=record.len();
        }
        assert_eq!(reserved+i64::try_from(serde_json::to_vec(&cancel(2,1)).unwrap().len()).unwrap(),i64::from(minimum));
        assert!(bytes<=usize::try_from(minimum).unwrap(),"actual maximum coded settlement {bytes} exceeds exact configured minimum {minimum}");
        Ok(())
    })).await.unwrap();
}
