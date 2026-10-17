#[expect(
    dead_code,
    unused_imports,
    reason = "shared native fixture helpers serve several independent journeys"
)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{SampleDelivery, SampleProtocol, TestResult};
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
        operation: OperationId(durable_steps::Uuid::now_v7()),
        step: StepName::new(name)?,
    })
}
fn peer(value: u128) -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::from_u128(value))?)
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
        matches!(other.request(DeliveryRequest::Retire{call:call("foreign-retire")?,mailbox:peer(4)?,items:vec![first]}).await?,DeliveryAnswer::Retire(RetireResult{retired,unknown}) if retired.is_empty()&&unknown==[first])
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
        "OBS real process/WebSocket relay: custody receipts release payload; bounded report refusal stores nothing; authenticated-origin status spans two mailboxes; foreign Retire reveals only unknown; retirement crosses Controls"
    );
    drop(sender);
    drop(recipient);
    drop(other);
    process.stop(&mut control).await?;
    Ok(())
}
