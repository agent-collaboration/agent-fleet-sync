//! Crafted frames cross the real native session boundary without reconnecting.
use crate::{Observations, fixture_service::*, support::*};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, SampleProtocol, SampleRequest, TestResult,
};
use fleet_sync_transport_websocket::WebSocketConnection;
use serde_json::{Value, json};
use std::path::Path;

fn identified_cases(frame: FrameId) -> [(Value, String); 5] {
    // The consumer README declares a 64 Unicode-character diagnostic cap.
    // Mixed byte widths distinguish that claim from truncating UTF-8 bytes.
    let long_field = "界🙂a".repeat(40);
    // Twenty-one triples plus one character spell the expected 64-char prefix.
    let bounded_field = format!("{}界", "界🙂a".repeat(21));
    let mut oversized = json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Echo":{"text":"must not route"}}}}});
    oversized["value"][long_field.as_str()] = json!(true);
    [
        (
            json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Echo":{}}}}}),
            "text".to_owned(),
        ),
        (
            json!({"kind":"request","value":{"frame":frame}}),
            "body".to_owned(),
        ),
        (
            json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Echo":{"text":"must not route"}}},"unexpected":true}}),
            "unexpected".to_owned(),
        ),
        (
            json!({"kind":"answer","value":{"frame":frame,"body":{"kind":"consumer","value":{"notUnit":true}}}}),
            "body".to_owned(),
        ),
        (oversized, bounded_field),
    ]
}

async fn request_count(control: &mut WebSocketConnection) -> TestResult<u64> {
    match control_request(control, ControlRequest::RequestCount).await? {
        ControlAnswer::RequestCount(count) => Ok(count),
        _ => Err("consumer request count missing".into()),
    }
}

async fn identified_bodies(
    service: &ServiceProcess,
    control: &mut WebSocketConnection,
    observations: &mut Observations,
) -> TestResult {
    let mut socket = attach_peer(service, new_peer().await?, 100).await?;
    let result: TestResult = async {
        for index in 0..5 {
            // Correlation IDs are non-nil, not entity keys. Each case gets a
            // distinct public fixture UUIDv4, followed by a fresh valid frame.
            let frame = FrameId::new(durable_steps::Uuid::from_u128(
                0x019eef00000040008000000000000100 + index as u128,
            ))?;
            let (malformed, field) = identified_cases(frame)
                .into_iter()
                .nth(index)
                .ok_or("identified frame fixture missing")?;
            let response: TestResult<Value> = async {
                socket.send(serde_json::to_vec(&malformed)?.into()).await?;
                receive_json(&mut socket).await
            }
            .await;
            let observed = match response {
                Ok(value) => value,
                Err(error) => json!({"error":error.to_string()}),
            };
            let expected = json!({"kind":"answer","value":{"frame":frame,"body":{"kind":"invalidShape","value":{"field":field}}}});
            observations.check(
                &format!("identified-bad-body-{index}"),
                observed == expected,
                expected,
                observed,
            );

            let follow_up_frame = FrameId::new(durable_steps::Uuid::now_v7())?;
            let text = format!("after-bad-body-{index}");
            let follow_up: ClientFrame<SampleProtocol> = ClientFrame::Request {
                frame: follow_up_frame,
                body: OperationRequest::Consumer(SampleRequest::Echo { text: text.clone() }),
            };
            let response: TestResult<Value> = async {
                socket.send(serde_json::to_vec(&follow_up)?.into()).await?;
                receive_json(&mut socket).await
            }
            .await;
            let observed = match response {
                Ok(value) => value,
                Err(error) => json!({"error":error.to_string()}),
            };
            observations.check(
                &format!("same-session-after-bad-body-{index}"),
                observed["kind"] == "answer"
                    && observed["value"]["frame"] == json!(follow_up_frame)
                    && observed["value"]["body"]["kind"] == "consumer"
                    && observed["value"]["body"]["value"]["Echo"]["text"] == text,
                json!({"frame":follow_up_frame,"text":text,"sameConnection":true}),
                observed,
            );
        }
        let count = request_count(control).await?;
        observations.check(
            "malformed-bodies-never-route",
            count == 5,
            json!({"validFollowUps":5,"consumerRequests":5}),
            json!({"consumerRequests":count}),
        );
        Ok(())
    }
    .await;
    socket.close(CloseReason::ClientClosed).await;
    result
}

async fn unreadable_envelopes(
    service: &ServiceProcess,
    control: &mut WebSocketConnection,
    observations: &mut Observations,
) -> TestResult {
    let before = request_count(control).await?;
    let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
    let malformed = [
        "{unreadable".to_owned(),
        json!({"kind":"request","value":{"frame":"not-an-id","body":{"kind":"consumer","value":{"Echo":{"text":"must not route"}}}}}).to_string(),
        json!({"kind":"request","value":{"body":null}}).to_string(),
        json!({"kind":"unknown","value":{"frame":frame,"body":null}}).to_string(),
    ];
    for (index, offered) in malformed.into_iter().enumerate() {
        let mut socket = attach_peer(service, new_peer().await?, 100).await?;
        let response: TestResult<Value> = async {
            socket.send(offered.into_bytes().into()).await?;
            receive_json(&mut socket).await
        }
        .await;
        let observed = match response {
            Ok(value) => value,
            Err(error) => json!({"error":error.to_string()}),
        };
        let expected = json!({"kind":"closed","value":{"reason":{"kind":"malformedFrame"}}});
        observations.check(
            &format!("unreadable-envelope-{index}"),
            observed == expected,
            expected,
            observed,
        );
        socket.close(CloseReason::ClientClosed).await;
    }
    let after = request_count(control).await?;
    observations.check(
        "unreadable-envelopes-never-route",
        after == before,
        json!({"consumerRequests":before,"additionalRequests":0}),
        json!({"consumerRequests":after}),
    );
    Ok(())
}

pub async fn run(root: &Path, observations: &mut Observations) -> TestResult {
    let service = start_service(root).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let result: TestResult = async {
        identified_bodies(&service, &mut control, observations).await?;
        unreadable_envelopes(&service, &mut control, observations).await
    }
    .await;
    service.stop(&mut control).await?;
    result
}
