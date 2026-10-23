#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use fleet_sync_transport_websocket::WebSocketConnection;
use serde_json::{Value, json};
use std::time::Duration;
use support::*;

async fn next_json(socket: &mut WebSocketConnection) -> TestResult<Value> {
    loop {
        let bytes = tokio::time::timeout(Duration::from_secs(15), socket.receive())
            .await?
            .ok_or("session closed before response")??;
        let value: Value = serde_json::from_slice(&bytes)?;
        if value["kind"] != "notice" {
            return Ok(value);
        }
    }
}

#[tokio::test]
async fn identified_bad_bodies_are_correlated_and_the_same_session_stays_usable() -> TestResult {
    let directory = tempfile::tempdir()?;
    let service = ServiceProcess::start(&directory.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = service
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let result: TestResult = async {
        // Frame IDs remain non-nil correlation IDs; they need not be UUIDv7.
        let frame = FrameId::new(durable_steps::Uuid::from_u128(0x019eef00000040008000000000000011))?;
        let long_field = "界".repeat(100);
        let bounded_field = "界".repeat(64);
        let mut oversized = json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Echo":{"text":"must not route"}}}}});
        oversized["value"][long_field.as_str()] = json!(true);
        let malformed = [
            (json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Echo":{}}}}}), "text"),
            (json!({"kind":"request","value":{"frame":frame}}), "body"),
            (json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Echo":{"text":"must not route"}}},"unexpected":true}}), "unexpected"),
            (json!({"kind":"answer","value":{"frame":frame,"body":{"kind":"consumer","value":{"notUnit":true}}}}), "body"),
            (oversized, bounded_field.as_str()),
        ];
        for (index, (offered, field)) in malformed.into_iter().enumerate() {
            socket.send(serde_json::to_vec(&offered)?.into()).await?;
            let answer = next_json(&mut socket).await?;
            if answer["kind"] != "answer" || answer["value"]["frame"] != json!(frame)
                || answer["value"]["body"] != json!({"kind":"invalidShape","value":{"field":field}})
            {
                return Err(format!("identified body {index} must get InvalidShape({field}): {answer}").into());
            }
            let text = format!("valid after malformed body {index}");
            if !matches!(request(&mut socket, OperationRequest::Consumer(SampleRequest::Echo { text: text.clone() })).await?,
                OperationAnswer::Consumer(SampleAnswer::Echo { text: echoed, .. }) if echoed == text)
            {
                return Err("valid follow-up was not answered on the same session".into());
            }
        }
        if !matches!(control_request(&mut control, ControlRequest::RequestCount).await?, ControlAnswer::RequestCount(5)) {
            return Err("malformed operations must never reach the consumer".into());
        }
        Ok(())
    }.await;
    service.stop(&mut control).await?;
    result
}

#[tokio::test]
async fn unreadable_kind_or_frame_id_closes_as_malformed_without_routing() -> TestResult {
    let directory = tempfile::tempdir()?;
    let service = ServiceProcess::start(&directory.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let result: TestResult = async {
        let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
        let malformed = [
            "{unreadable".to_owned(),
            json!({"kind":"request","value":{"frame":"not-an-id","body":{"kind":"consumer","value":{"Echo":{"text":"must not route"}}}}}).to_string(),
            json!({"kind":"request","value":{"body":null}}).to_string(),
            json!({"kind":"unknown","value":{"frame":frame,"body":null}}).to_string(),
        ];
        for offered in malformed {
            let mut socket = service.peer(2, SessionId::new(durable_steps::Uuid::now_v7())?).await?;
            socket.send(offered.into_bytes().into()).await?;
            if !matches!(server_frame(&mut socket).await?, ServerFrame::Closed { reason: CloseReason::MalformedFrame }) {
                return Err("unreadable envelope must close as MalformedFrame".into());
            }
        }
        if !matches!(control_request(&mut control, ControlRequest::RequestCount).await?, ControlAnswer::RequestCount(0)) {
            return Err("unreadable frames must never route".into());
        }
        Ok(())
    }.await;
    service.stop(&mut control).await?;
    result
}

#[tokio::test]
async fn unknown_fields_with_backticks_keep_their_complete_correlated_names() -> TestResult {
    let directory = tempfile::tempdir()?;
    let service = ServiceProcess::start(&directory.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = service
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let result: TestResult = async {
        let mut failures = Vec::new();
        for field in ["client`tag", "`leading", "client`, expected `tag"] {
            let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
            let mut offered = json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Echo":{"text":"must not route"}}}}});
            offered["value"][field] = json!(true);
            socket.send(serde_json::to_vec(&offered)?.into()).await?;
            let answer = next_json(&mut socket).await?;
            println!("OBS backtick field {field:?}: {answer}");
            if answer["kind"] != "answer" || answer["value"]["frame"] != json!(frame)
                || answer["value"]["body"] != json!({"kind":"invalidShape","value":{"field":field}}) {
                failures.push(format!("InvalidShape must name exact field {field:?} and frame {frame}: {answer}"));
            }
            let text = format!("valid after {field}");
            if !matches!(request(&mut socket, OperationRequest::Consumer(SampleRequest::Echo { text: text.clone() })).await?,
                OperationAnswer::Consumer(SampleAnswer::Echo { text: echoed, .. }) if echoed == text) {
                return Err("valid follow-up was not answered on the same session".into());
            }
        }
        if !matches!(control_request(&mut control, ControlRequest::RequestCount).await?, ControlAnswer::RequestCount(3)) {
            failures.push("malformed operations reached the consumer".into());
        }
        if failures.is_empty() { Ok(()) } else { Err(failures.join("\n").into()) }
    }.await;
    service.stop(&mut control).await?;
    result
}

#[tokio::test]
async fn unknown_fields_choose_the_actual_empty_or_nonempty_diagnostic_suffix() -> TestResult {
    let directory = tempfile::tempdir()?;
    let service = ServiceProcess::start(&directory.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = service
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let result: TestResult = async {
        let mut failures = Vec::new();
        for empty in [true, false] {
            for field in ["client`, expected `tag", "client`, there are no fields", "client at line 3 column 7"] {
                let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
                let mut offered = json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Empty":{}}}}});
                if empty { offered["value"]["body"]["value"]["Empty"][field] = json!(true); }
                else { offered["value"][field] = json!(true); }
                socket.send(serde_json::to_vec(&offered)?.into()).await?;
                let answer = next_json(&mut socket).await?;
                println!("OBS unknown field empty={empty}, field={field:?}: {answer}");
                if answer["kind"] != "answer" || answer["value"]["frame"] != json!(frame)
                    || answer["value"]["body"] != json!({"kind":"invalidShape","value":{"field":field}}) {
                    failures.push(format!("empty={empty}: expected exact field {field:?} and frame {frame}: {answer}"));
                }
                if !matches!(request(&mut socket, OperationRequest::Consumer(SampleRequest::Empty(SampleEmptyRequest {}))).await?, OperationAnswer::Consumer(SampleAnswer::Empty { .. })) {
                    return Err("valid empty-request follow-up failed on the same session".into());
                }
            }
        }
        if !matches!(control_request(&mut control, ControlRequest::RequestCount).await?, ControlAnswer::RequestCount(6)) {
            failures.push("malformed empty/nonempty operations reached the consumer".into());
        }
        if failures.is_empty() { Ok(()) } else { Err(failures.join("\n").into()) }
    }.await;
    service.stop(&mut control).await?;
    result
}

#[tokio::test]
async fn unknown_fields_are_not_confused_with_declared_names_or_nested_input_keys() -> TestResult {
    let directory = tempfile::tempdir()?;
    let service = ServiceProcess::start(&directory.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = service
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let result: TestResult = async {
        let mut failures = Vec::new();
        for (case, value, content_first) in [
            ("ordinary", json!(true), false),
            ("nested collision", json!({"unexpected`, expected `declared":true}), false),
            ("content first", json!(true), true),
            ("content first nested collision", json!({"unexpected`, expected `declared":true}), true),
        ] {
            let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
            let offered = json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Renamed":{"unexpected":value}}}}});
            let bytes = if content_first {
                format!("{{\"value\":{},\"kind\":\"request\"}}", offered["value"]).into_bytes()
            } else { serde_json::to_vec(&offered)? };
            let diagnostic = serde_json::from_slice::<ClientFrame<SampleProtocol>>(&bytes).err().ok_or("malformed fixture decoded successfully")?;
            let raw = std::str::from_utf8(&bytes)?;
            let key_end = raw.find("\"unexpected\"").ok_or("unknown key missing")? + "\"unexpected\"".len();
            println!("OBS renamed {case} decoder position line={} column={} unknown_key_end={key_end}", diagnostic.line(), diagnostic.column());
            socket.send(bytes.into()).await?;
            let answer = next_json(&mut socket).await?;
            println!("OBS renamed {case}: {answer}");
            if answer["kind"] != "answer" || answer["value"]["frame"] != json!(frame)
                || answer["value"]["body"] != json!({"kind":"invalidShape","value":{"field":"unexpected"}}) {
                failures.push(format!("{case}: exact unknown field unexpected and frame {frame} required: {answer}"));
            }
            if !matches!(request(&mut socket, OperationRequest::Consumer(SampleRequest::Renamed(SampleRenamedRequest { known:true }))).await?, OperationAnswer::Consumer(SampleAnswer::Renamed { known:true, .. })) {
                return Err("valid renamed-field follow-up failed on the same socket".into());
            }
        }
        if !matches!(control_request(&mut control, ControlRequest::RequestCount).await?, ControlAnswer::RequestCount(4)) {
            failures.push("malformed renamed requests reached the consumer".into());
        }
        if failures.is_empty() { Ok(()) } else { Err(failures.join("\n").into()) }
    }.await;
    service.stop(&mut control).await?;
    result
}

#[tokio::test]
async fn discarded_backtracking_errors_do_not_replace_the_final_reported_field() -> TestResult {
    let directory = tempfile::tempdir()?;
    let service = ServiceProcess::start(&directory.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = service
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let result: TestResult = async {
        for (payload, outer_unknown, field) in [(json!({"second":true}), true, "outerAfter"), (json!({"neither":true}), false, "body")] {
            let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
            let mut offered = json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Backtracking":payload}}}});
            if outer_unknown { offered["value"]["outerAfter"] = json!(true); }
            socket.send(serde_json::to_vec(&offered)?.into()).await?;
            let answer = next_json(&mut socket).await?;
            println!("OBS backtracking outer_unknown={outer_unknown}: {answer}");
            if answer["kind"] != "answer" || answer["value"]["frame"] != json!(frame)
                || answer["value"]["body"] != json!({"kind":"invalidShape","value":{"field":field}}) {
                return Err(format!("discarded candidate error replaced final field {field}: {answer}").into());
            }
            if !matches!(request(&mut socket, OperationRequest::Consumer(SampleRequest::Backtracking(SampleBacktrackingRequest::Second { second:true }))).await?, OperationAnswer::Consumer(SampleAnswer::Backtracking { accepted:true, .. })) {
                return Err("valid backtracking follow-up failed on the same socket".into());
            }
        }
        if !matches!(control_request(&mut control, ControlRequest::RequestCount).await?, ControlAnswer::RequestCount(2)) {
            return Err("malformed backtracking requests reached the consumer".into());
        }
        Ok(())
    }.await;
    service.stop(&mut control).await?;
    result
}

#[tokio::test]
async fn missing_and_duplicate_declared_fields_keep_their_exact_names() -> TestResult {
    let directory = tempfile::tempdir()?;
    let service = ServiceProcess::start(&directory.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = service
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let result: TestResult = async {
        for duplicate in [false, true] {
            let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
            let payload = if duplicate { r#"{"declared`, expected `tail":true,"declared`, expected `tail":false}"# } else { "{}" };
            let offered = format!(r#"{{"kind":"request","value":{{"frame":"{frame}","body":{{"kind":"consumer","value":{{"Renamed":{payload}}}}}}}}}"#);
            socket.send(offered.into_bytes().into()).await?;
            let answer = next_json(&mut socket).await?;
            println!("OBS declared duplicate={duplicate}: {answer}");
            if answer["kind"] != "answer" || answer["value"]["frame"] != json!(frame)
                || answer["value"]["body"] != json!({"kind":"invalidShape","value":{"field":"declared`, expected `tail"}}) {
                return Err(format!("missing/duplicate declared field was not exact: {answer}").into());
            }
            if !matches!(request(&mut socket, OperationRequest::Consumer(SampleRequest::Renamed(SampleRenamedRequest { known:true }))).await?, OperationAnswer::Consumer(SampleAnswer::Renamed { known:true, .. })) {
                return Err("valid follow-up after missing/duplicate field failed".into());
            }
        }
        if !matches!(control_request(&mut control, ControlRequest::RequestCount).await?, ControlAnswer::RequestCount(2)) {
            return Err("missing/duplicate field reached the consumer".into());
        }
        Ok(())
    }.await;
    service.stop(&mut control).await?;
    result
}
