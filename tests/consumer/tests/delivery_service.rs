#[cfg(feature = "test-support")]
#[path = "support/delivery_atomic.rs"]
mod atomic;
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
    async fn wait_publication(&mut self, item: ItemId, minimum: StatusVersion) -> TestResult {
        println!("OBS waiting publication version {}", minimum.value());
        loop {
            if self
                .publications
                .iter()
                .any(|publication| publication.item == item && publication.version >= minimum)
            {
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
        "sample sender".into(),
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

#[tokio::test]
async fn native_relay_custody_and_status_stream_are_bound_to_the_authenticated_origin() -> TestResult
{
    let temporary = tempfile::tempdir()?;
    let directory = std::env::var_os("FLEET_DELIVERY_EVIDENCE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| temporary.path().to_path_buf());
    tokio::fs::create_dir_all(&directory).await?;
    let process = ServiceProcess::start(&directory).await?;
    println!("OBS relay process ready");
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut sender = connect(&process, 2).await?;
    let mut recipient = connect(&process, 4).await?;
    let mut other = connect(&process, 6).await?;
    let first = ItemId::new(durable_steps::Uuid::now_v7())?;
    let second = ItemId::new(durable_steps::Uuid::now_v7())?;
    for (id, mailbox) in [(first, peer(4)?), (second, peer(5)?)] {
        assert!(
            matches!(sender.request(DeliveryRequest::Submit{call:call("submit")?,item:item(id,mailbox,"one body")?}).await?,DeliveryAnswer::Submit(SubmitResult::Accepted{version,..}) if version.value()==1)
        );
        sender.wait_publication(id, StatusVersion::new(1)).await?;
    }
    match sender
        .request(DeliveryRequest::StatusFetch {
            call: call("status")?,
            after: StatusCursor::new(0),
            limit: 100,
        })
        .await?
    {
        DeliveryAnswer::StatusFetch(batch) => {
            let ids: std::collections::BTreeSet<_> =
                batch.events.iter().map(|event| event.item).collect();
            assert_eq!(ids, std::collections::BTreeSet::from([first, second]));
            assert!(
                batch
                    .events
                    .windows(2)
                    .all(|events| events[0].cursor < events[1].cursor)
            );
        }
        _ => return Err("status fetch did not answer".into()),
    }
    assert!(
        matches!(other.request(DeliveryRequest::StatusFetch{call:call("foreign-status")?,after:StatusCursor::new(0),limit:100}).await?,DeliveryAnswer::StatusFetch(batch) if batch.events.is_empty()&&batch.through.value()==0)
    );
    assert!(matches!(
        sender
            .request(DeliveryRequest::Submit {
                call: call("replay")?,
                item: item(first, peer(4)?, "one body")?
            })
            .await?,
        DeliveryAnswer::Submit(SubmitResult::Replayed { .. })
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
    let through = match recipient
        .request(DeliveryRequest::Fetch {
            call: call("fetch")?,
            stream: DeliveryStream::Items,
            after: DeliverySeq::new(0),
            max_items: 10,
            max_bytes: 10_000,
        })
        .await?
    {
        DeliveryAnswer::Fetch(batch) => {
            assert_eq!(batch.entries.len(), 1);
            assert!(
                matches!(batch.entries.first(),Some(FetchEntry::Item{item,..}) if item.id()==first)
            );
            batch.through
        }
        _ => return Err("fetch did not answer".into()),
    };
    assert!(
        matches!(recipient.request(DeliveryRequest::Received{call:call("received")?,stream:DeliveryStream::Items,through}).await?,DeliveryAnswer::Received(answer) if answer.released_through==through)
    );
    assert!(matches!(
        recipient
            .request(DeliveryRequest::Report {
                call: call("large-report")?,
                report: RecipientReport {
                    item: first,
                    report_seq: ReportSeq::new(1),
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
        recipient
            .request(DeliveryRequest::Report {
                call: call("report")?,
                report: RecipientReport {
                    item: first,
                    report_seq: ReportSeq::new(1),
                    outcome: RecipientOutcome::Delivered {
                        acceptance: "delivered".into()
                    }
                }
            })
            .await?,
        DeliveryAnswer::Report(ReportResult::Applied {
            status: DeliveryStatus::Delivered { .. },
            ..
        })
    ));
    sender
        .wait_publication(first, StatusVersion::new(3))
        .await?;
    assert!(
        matches!(other.request(DeliveryRequest::Retire{call:call("foreign-retire")?,mailbox:peer(4)?,items:vec![first]}).await?,DeliveryAnswer::Retire(RetireResult{retired,unknown,not_current_target}) if retired.is_empty()&&unknown.is_empty()&&not_current_target==[first])
    );
    assert!(
        matches!(sender.request(DeliveryRequest::Retire{call:call("retire")?,mailbox:peer(4)?,items:vec![first]}).await?,DeliveryAnswer::Retire(RetireResult{retired,..}) if retired==[first])
    );
    let controls = match recipient
        .request(DeliveryRequest::Fetch {
            call: call("retired-control")?,
            stream: DeliveryStream::Controls,
            after: DeliverySeq::new(0),
            max_items: 10,
            max_bytes: 10_000,
        })
        .await?
    {
        DeliveryAnswer::Fetch(batch) => {
            assert!(
                matches!(batch.entries.first(),Some(FetchEntry::Retired{items,..}) if items==&[first])
            );
            batch.through
        }
        _ => return Err("controls fetch did not answer".into()),
    };
    recipient
        .request(DeliveryRequest::Received {
            call: call("control-receipt")?,
            stream: DeliveryStream::Controls,
            through: controls,
        })
        .await?;
    println!(
        "OBS real process/WebSocket relay: custody receipts release payload; bounded report refusal stores nothing; authenticated-origin status spans two mailboxes; foreign Retire answers notCurrentTarget; retirement crosses Controls"
    );
    drop(sender);
    drop(recipient);
    drop(other);
    process.stop(&mut control).await?;
    Ok(())
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn native_mailbox_failures_match_the_final_atomic_store_result() -> TestResult {
    use fleet_sync_delivery::MailboxOperation;
    let directory = tempfile::tempdir()?;
    let process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut sender = connect(&process, 2).await?;
    let mut recipient = connect(&process, 4).await?;
    let identity = ItemId::new(durable_steps::Uuid::now_v7())?;
    let oversized = DeliveryEnvelope::Item(DeliveryItem::<SampleDelivery>::new(
        identity,
        "x".repeat(256),
        Recipient {
            peer: peer(4)?,
            address: "inbox".into(),
        },
        None,
        "body".into(),
    )?);
    let refused = sender
        .request(DeliveryRequest::Submit {
            call: call("oversized sender")?,
            item: oversized,
        })
        .await?;
    assert_eq!(
        serde_json::to_value(refused)?,
        atomic::failed_answer(MailboxOperation::Submit, peer(4)?, peer(2)?, identity).await?
    );
    assert!(matches!(
        sender
            .request(DeliveryRequest::Submit {
                call: call("healthy before report")?,
                item: item(identity, peer(4)?, "body")?
            })
            .await?,
        DeliveryAnswer::Submit(SubmitResult::Accepted { .. })
    ));
    sender
        .wait_publication(identity, StatusVersion::new(1))
        .await?;
    let refused = recipient
        .request(DeliveryRequest::Report {
            call: call("oversized acceptance")?,
            report: RecipientReport {
                item: identity,
                report_seq: ReportSeq::new(1),
                outcome: RecipientOutcome::Delivered {
                    acceptance: "x".repeat(256),
                },
            },
        })
        .await?;
    assert_eq!(
        serde_json::to_value(refused)?,
        atomic::failed_answer(MailboxOperation::Report, peer(4)?, peer(2)?, identity).await?
    );
    assert!(matches!(
        control_request(
            &mut control,
            ControlRequest::CorruptDeliveryCustody {
                mailbox: peer(4)?,
                item: identity
            }
        )
        .await?,
        ControlAnswer::CorruptedDelivery { .. }
    ));
    let unavailable = sender
        .request(DeliveryRequest::Query {
            call: call("corrupt custody")?,
            mailbox: peer(4)?,
            items: vec![identity],
        })
        .await?;
    assert_eq!(
        serde_json::to_value(unavailable)?,
        atomic::failed_answer(MailboxOperation::Query, peer(4)?, peer(2)?, identity).await?
    );
    drop(sender);
    drop(recipient);
    process.stop(&mut control).await?;
    Ok(())
}

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
        forged_route.into(),
        Recipient {
            peer: peer(4)?,
            address: "inbox".into(),
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
    sender
        .wait_publication(identity, StatusVersion::new(1))
        .await?;
    let (record, _) =
        support::delivery_mail::mail_row(&mut control, peer(4)?, peer(2)?, identity).await?;
    assert_eq!(record["value"]["origin"], serde_json::json!(peer(2)?));
    assert_eq!(record["value"]["statusTarget"]["route"], verified_route);
    assert_eq!(
        record["value"]["statusTarget"]["peer"],
        serde_json::json!(peer(2)?)
    );
    assert_eq!(
        record["value"]["statusTarget"]["rank"],
        serde_json::Value::Null
    );
    drop(sender);
    process.stop(&mut control).await?;
    Ok(())
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn native_retirement_reply_lost_after_commit_replays_once_after_process_restart() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let mut process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut sender = connect(&process, 2).await?;
    let mut recipient = connect(&process, 4).await?;
    let identity = ItemId::new(durable_steps::Uuid::now_v7())?;
    assert!(matches!(
        sender
            .request(DeliveryRequest::Submit {
                call: call("submit before lost retirement")?,
                item: item(identity, peer(4)?, "body")?
            })
            .await?,
        DeliveryAnswer::Submit(SubmitResult::Accepted { .. })
    ));
    sender
        .wait_publication(identity, StatusVersion::new(1))
        .await?;
    assert!(matches!(
        recipient
            .request(DeliveryRequest::Report {
                call: call("terminal before lost retirement")?,
                report: RecipientReport {
                    item: identity,
                    report_seq: ReportSeq::new(1),
                    outcome: RecipientOutcome::Delivered {
                        acceptance: "done".into()
                    }
                }
            })
            .await?,
        DeliveryAnswer::Report(ReportResult::Applied { .. })
    ));
    sender
        .wait_publication(identity, StatusVersion::new(2))
        .await?;
    assert!(matches!(
        control_request(&mut control, ControlRequest::HoldNextRetirementReply).await?,
        ControlAnswer::Ok
    ));
    let retirement = call("retirement reply lost")?;
    sender
        .socket
        .send(
            serde_json::to_vec(&ClientFrame::<SampleProtocol>::Request {
                frame: FrameId::new(durable_steps::Uuid::now_v7())?,
                body: OperationRequest::Delivery(DeliveryRequest::Retire {
                    call: retirement.clone(),
                    mailbox: peer(4)?,
                    items: vec![identity],
                }),
            })?
            .into(),
        )
        .await?;
    assert!(matches!(
        control_request(&mut control, ControlRequest::WaitRetirementReplyCommitted).await?,
        ControlAnswer::Ok
    ));
    let (before, _) =
        support::delivery_mail::mail_row(&mut control, peer(4)?, peer(2)?, identity).await?;
    assert_eq!(before["kind"], "compacted");
    // This is an actual process death before the held socket answer, not a discarded API result.
    process.kill().await?;
    drop(sender);
    drop(recipient);
    drop(control);
    let process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut sender = connect(&process, 2).await?;
    let mut recipient = connect(&process, 4).await?;
    for _ in 0..2 {
        match sender
            .request(DeliveryRequest::Retire {
                call: retirement.clone(),
                mailbox: peer(4)?,
                items: vec![identity],
            })
            .await?
        {
            DeliveryAnswer::Retire(result) => {
                assert_eq!(result.retired, [identity]);
                assert!(result.not_current_target.is_empty());
                assert!(result.unknown.is_empty());
            }
            _ => return Err("retirement receipt did not replay".into()),
        }
    }
    let (after, _) =
        support::delivery_mail::mail_row(&mut control, peer(4)?, peer(2)?, identity).await?;
    assert_eq!(before, after);
    match recipient
        .request(DeliveryRequest::Fetch {
            call: call("one retirement control")?,
            stream: DeliveryStream::Controls,
            after: DeliverySeq::new(0),
            max_items: 10,
            max_bytes: 10_000,
        })
        .await?
    {
        DeliveryAnswer::Fetch(batch) => {
            assert_eq!(batch.through.value(), 1);
            assert_eq!(batch.entries.len(), 1);
            assert!(
                matches!(batch.entries.first().ok_or("missing retirement control")?, FetchEntry::Retired { items, .. } if items==&[identity])
            );
        }
        _ => return Err("retirement control did not survive restart".into()),
    }
    drop(sender);
    drop(recipient);
    process.stop(&mut control).await?;
    Ok(())
}
