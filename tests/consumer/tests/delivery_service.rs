#[cfg(feature = "test-support")]
#[path = "support/delivery_node.rs"]
#[expect(dead_code, reason = "shared process helpers serve several journeys")]
mod node;
#[expect(
    dead_code,
    unused_imports,
    reason = "shared native fixture helpers serve several independent journeys"
)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, SampleDelivery, SampleProtocol, TestResult,
};
use fleet_sync_transport_websocket::WebSocketConnection;
use support::*;

struct DeliveryPeer {
    socket: WebSocketConnection,
    publications: Vec<StatusPublication<String>>,
}
impl DeliveryPeer {
    async fn publication(
        &mut self,
        frame: FrameId,
        publication: StatusPublication<String>,
    ) -> TestResult {
        self.publications.push(publication);
        self.socket
            .send(
                serde_json::to_vec(&ClientFrame::<SampleProtocol>::Answer {
                    frame,
                    body: ClientAnswer::Crate(ServerCallAnswer::Published(
                        StatusPublicationResult::Applied,
                    )),
                })?
                .into(),
            )
            .await?;
        Ok(())
    }
    async fn request(
        &mut self,
        request: DeliveryRequest<SampleDelivery>,
    ) -> TestResult<DeliveryAnswer<SampleDelivery>> {
        let kind = serde_json::to_value(&request)?["kind"].clone();
        println!("OBS requesting {kind}");
        let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
        self.socket
            .send(
                serde_json::to_vec(&ClientFrame::<SampleProtocol>::Request {
                    frame,
                    body: OperationRequest::Delivery(request),
                })?
                .into(),
            )
            .await?;
        loop {
            match server_frame(&mut self.socket).await? {
                ServerFrame::Answer {
                    frame: answered,
                    body: OperationAnswer::Delivery(answer),
                } if answered == frame => {
                    println!("OBS answered {kind}");
                    return Ok(answer);
                }
                ServerFrame::Request {
                    frame,
                    body: ServerRequest::Crate(ServerCall::PublishStatus { publication, .. }),
                } => self.publication(frame, publication).await?,
                ServerFrame::Notice { .. } => {}
                _ => return Err("unexpected delivery peer frame".into()),
            }
        }
    }
    async fn wait_delivered_publication(&mut self, item: ItemId) -> TestResult {
        loop {
            if self.publications.iter().any(|publication| {
                publication.item == item
                    && matches!(publication.status, DeliveryStatus::Delivered { .. })
            }) {
                return Ok(());
            }
            match server_frame(&mut self.socket).await? {
                ServerFrame::Request {
                    frame,
                    body: ServerRequest::Crate(ServerCall::PublishStatus { publication, .. }),
                } => self.publication(frame, publication).await?,
                ServerFrame::Notice { .. } => {}
                _ => return Err("expected status publication".into()),
            }
        }
    }
}
fn call(name: &str) -> TestResult<CallId> {
    Ok(CallId {
        operation: OperationId::new(durable_steps::Uuid::now_v7())?,
        step: StepName::new(name)?,
    })
}
fn peer(value: u128) -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::from_u128(
        (value) | 0x019eef00000070008000000000000000,
    ))?)
}
fn item(id: ItemId, recipient: PeerId, body: &str) -> TestResult<DeliveryEnvelope<SampleDelivery>> {
    Ok(DeliveryEnvelope::Item(DeliveryItem::new(
        id,
        format!("{}|authorized sender", peer(2)?),
        Recipient {
            peer: recipient,
            address: "inbox".into(),
        },
        None,
        body.into(),
    )?))
}
async fn connect(process: &ServiceProcess, id: u128) -> TestResult<DeliveryPeer> {
    Ok(DeliveryPeer {
        socket: process
            .peer(id, SessionId::new(durable_steps::Uuid::now_v7())?)
            .await?,
        publications: Vec::new(),
    })
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn native_relay_custody_and_status_stream_are_bound_to_the_authenticated_origin() -> TestResult
{
    use fleet_sync_test_consumer::delivery_node::CustodySide;
    let directory = tempfile::tempdir()?;
    let process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut sender = connect(&process, 2).await?;
    let mut other = connect(&process, 6).await?;
    let mut recipient = node::DeliveryNode::start(&directory.path().join("recipient4"), 4).await?;
    let mut second_recipient =
        node::DeliveryNode::start(&directory.path().join("recipient5"), 5).await?;
    recipient.connect(&process).await?;
    second_recipient.connect(&process).await?;
    let first = ItemId::new(durable_steps::Uuid::now_v7())?;
    let second = ItemId::new(durable_steps::Uuid::now_v7())?;
    for (identity, target) in [(first, peer(4)?), (second, peer(5)?)] {
        assert!(
            matches!(sender.request(DeliveryRequest::Submit {call:call("submit")?,item:item(identity,target,"one body")?}).await?,DeliveryAnswer::Submit(SubmitResult::Accepted {version,..}) if version.value()==1)
        );
    }
    let delivered = recipient
        .wait(first, CustodySide::Inbox, "delivered")
        .await?;
    assert_eq!(node::effect_count(&delivered, first), 1);
    assert_eq!(
        node::effect_count(
            &second_recipient
                .wait(second, CustodySide::Inbox, "delivered")
                .await?,
            second
        ),
        1
    );
    sender.wait_delivered_publication(first).await?;
    sender.wait_delivered_publication(second).await?;
    let fetched = sender
        .request(DeliveryRequest::StatusFetch {
            call: call("status")?,
            after: StatusCursor::new(0),
            limit: 100,
        })
        .await?;
    let DeliveryAnswer::StatusFetch(page) = fetched else {
        return Err("missing status page".into());
    };
    assert_eq!(
        page.events
            .iter()
            .map(|event| event.item)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([first, second])
    );
    assert!(
        page.events
            .windows(2)
            .all(|events| events[0].cursor < events[1].cursor)
    );
    assert!(
        matches!(other.request(DeliveryRequest::StatusFetch {call:call("foreign status")?,after:StatusCursor::new(0),limit:100}).await?,DeliveryAnswer::StatusFetch(page) if page.events.is_empty())
    );
    assert!(matches!(
        other
            .request(DeliveryRequest::Query {
                call: call("foreign query")?,
                mailbox: peer(4)?,
                items: vec![first]
            })
            .await?,
        DeliveryAnswer::DeliveryUnavailable {
            reason: DeliveryRefusal::NotSubmitter
        }
    ));
    assert!(matches!(
        sender
            .request(DeliveryRequest::Submit {
                call: call("replay")?,
                item: item(first, peer(4)?, "one body")?
            })
            .await?,
        DeliveryAnswer::Submit(SubmitResult::Replayed {
            status: DeliveryStatus::Delivered { .. },
            ..
        })
    ));
    assert!(matches!(
        sender
            .request(DeliveryRequest::Submit {
                call: call("conflict")?,
                item: item(first, peer(4)?, "changed body")?
            })
            .await?,
        DeliveryAnswer::Submit(SubmitResult::Refused {
            reason: DeliveryRefusal::ItemConflict { .. }
        })
    ));
    // The real recipient has already committed its outcome. A later oversized
    // report must still be refused before the sequence or canonical state moves.
    recipient.stop().await?;
    second_recipient.stop().await?;
    let mut reporting = connect(&process, 4).await?;
    assert!(matches!(
        reporting
            .request(DeliveryRequest::Report {
                call: call("large report")?,
                report: RecipientReport {
                    item: first,
                    report_seq: ReportSeq::new(99),
                    outcome: RecipientOutcome::Delivered {
                        acceptance: "x".repeat(256)
                    }
                }
            })
            .await?,
        DeliveryAnswer::Report(ReportResult::Refused {
            reason: DeliveryRefusal::TooLarge {
                field: DeliverySizeField::Acceptance
            }
        })
    ));
    assert!(matches!(
        reporting
            .request(DeliveryRequest::Report {
                call: call("unadmitted cancel")?,
                report: RecipientReport {
                    item: first,
                    report_seq: ReportSeq::new(99),
                    outcome: RecipientOutcome::Prevented {
                        cancel: ItemId::new(durable_steps::Uuid::now_v7())?
                    }
                }
            })
            .await?,
        DeliveryAnswer::Report(ReportResult::Refused {
            reason: DeliveryRefusal::ProcessingFailed {
                kind: DeliveryFailureKind::ExecutionFailed
            }
        })
    ));
    let canonical = sender
        .request(DeliveryRequest::Query {
            call: call("retained status")?,
            mailbox: peer(4)?,
            items: vec![first],
        })
        .await?;
    assert!(
        matches!(canonical,DeliveryAnswer::Query(result) if matches!(result.statuses[0].state,ItemStatusState::Known {status:DeliveryStatus::Delivered {..},version,..} if (2..=3).contains(&version.value())))
    );
    for entry in std::fs::read_dir(directory.path().join("service"))? {
        assert!(
            !entry?
                .file_name()
                .to_string_lossy()
                .starts_with("delivery-"),
            "per-peer delivery object survived"
        );
    }
    drop(reporting);
    drop(sender);
    drop(other);
    process.stop(&mut control).await?;
    Ok(())
}

#[cfg(feature = "test-support")]
#[cfg(feature = "test-support")]
#[tokio::test]
async fn native_status_route_comes_from_the_verified_binding_and_ignores_declared_mail_addresses()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let verified_route = "6b99f3e1-a002-4f42-9101-338578c72101";
    let forged_route = "6b99f3e1-a002-4f42-9101-338578c72999";
    assert!(PeerId::new(verified_route.parse()?).is_err());
    assert!(matches!(
        control_request(
            &mut control,
            ControlRequest::ConfigureStatusRoute {
                peer: peer(2)?,
                route: verified_route.into(),
            }
        )
        .await?,
        ControlAnswer::Ok
    ));
    let mut sender = connect(&process, 2).await?;
    let identity = ItemId::new(durable_steps::Uuid::now_v7())?;
    let envelope = DeliveryEnvelope::Item(DeliveryItem::<SampleDelivery>::new(
        identity,
        format!("{}|authorized sender", peer(2)?),
        Recipient {
            peer: peer(4)?,
            address: forged_route.into(),
        },
        None,
        serde_json::json!({"route":forged_route,"message":"untrusted mail address"}).to_string(),
    )?);
    assert!(matches!(
        sender
            .request(DeliveryRequest::Submit {
                call: call("forged declared route")?,
                item: envelope,
            })
            .await?,
        DeliveryAnswer::Submit(SubmitResult::Accepted { .. })
    ));
    let (record, _) = support::delivery_mail::relay_row(&mut control, identity).await?;
    assert_eq!(
        record["value"]["submitter"],
        serde_json::json!({"kind":"machine","value":peer(2)?})
    );
    let route: String = serde_json::from_str(
        record["value"]["statusRoute"]
            .as_str()
            .ok_or("missing stored route")?,
    )?;
    assert_eq!(route, verified_route);
    drop(sender);
    process.stop(&mut control).await?;
    Ok(())
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn corrupt_typed_relay_row_is_unavailable_without_poisoning_other_items() -> TestResult {
    use sqlx_turso::sqlx::{ConnectOptions, Connection};
    let directory = tempfile::tempdir()?;
    let process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut sender = connect(&process, 2).await?;
    let identity = ItemId::new(durable_steps::Uuid::now_v7())?;
    let digest = item(identity, peer(4)?, "not submitted")?.digest().clone();
    assert!(matches!(
        sender
            .request(DeliveryRequest::Settle {
                call: call("seed negative")?,
                submitter: Submitter::Machine(peer(2)?),
                mailbox: peer(4)?,
                item: identity,
                digest
            })
            .await?,
        DeliveryAnswer::Settle(_)
    ));
    drop(sender);
    process.stop(&mut control).await?;
    let mut db = sqlx_turso::TursoConnectOptions::new()
        .filename(directory.path().join("service/registry.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    // Deliberately seed a non-v7 stored identity in the stopped isolated fixture.
    sqlx_core::query::query::<sqlx_turso::Turso>("UPDATE _fleet_sync_outbox_items SET submitter_id='00000000-0000-4000-8000-000000000002' WHERE item_id=?1").bind(identity.as_uuid().to_string()).execute(&mut db).await?;
    db.close().await?;
    let process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut sender = connect(&process, 2).await?;
    assert!(matches!(
        sender
            .request(DeliveryRequest::Query {
                call: call("corrupt query")?,
                mailbox: peer(4)?,
                items: vec![identity]
            })
            .await?,
        DeliveryAnswer::DeliveryUnavailable {
            reason: DeliveryRefusal::ProcessingFailed {
                kind: DeliveryFailureKind::CustodyCorrupt
            }
        }
    ));
    let other = ItemId::new(durable_steps::Uuid::now_v7())?;
    let digest = item(other, peer(4)?, "another negative")?.digest().clone();
    assert!(matches!(
        sender
            .request(DeliveryRequest::Settle {
                call: call("independent negative")?,
                submitter: Submitter::Machine(peer(2)?),
                mailbox: peer(4)?,
                item: other,
                digest
            })
            .await?,
        DeliveryAnswer::Settle(_)
    ));
    drop(sender);
    process.stop(&mut control).await?;
    let mut db = sqlx_turso::TursoConnectOptions::new()
        .filename(directory.path().join("service/registry.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    sqlx_core::query::query::<sqlx_turso::Turso>(
        "UPDATE _fleet_sync_outbox_items SET submitter_id=?1 WHERE item_id=?2",
    )
    .bind(peer(2)?.as_uuid().to_string())
    .bind(identity.as_uuid().to_string())
    .execute(&mut db)
    .await?;
    db.close().await?;
    let process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut sender = connect(&process, 2).await?;
    assert!(
        matches!(sender.request(DeliveryRequest::Query {call:call("restored query")?,mailbox:peer(4)?,items:vec![identity]}).await?,DeliveryAnswer::Query(answer) if matches!(answer.statuses[0].state,ItemStatusState::Known {..}))
    );
    drop(sender);
    process.stop(&mut control).await?;
    Ok(())
}
