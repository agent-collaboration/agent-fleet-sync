use crate::{Observations, fixture_service::*};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{SampleProtocol, TestResult};
use serde_json::json;
use std::path::Path;

pub async fn run(root: &Path, observations: &mut Observations) -> TestResult {
    let service = start_service(root).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let result: TestResult = async {
        let mut socket = attach_peer(&service, new_peer()?, 100).await?;
        let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
        let malformed = json!({"kind":"request","value":{"frame":frame,"body":{"kind":"consumer","value":{"Echo":{}}}}});
        socket.send(serde_json::to_vec(&malformed)?.into()).await?;
        let response = receive_json(&mut socket).await;
        let observed = match &response {Ok(value)=>value.clone(),Err(error)=>json!({"error":error.to_string()})};
        let correlated = observed["kind"] == "answer" && observed["value"]["frame"] == json!(frame)
            && observed["value"]["body"]["kind"] == "invalidShape"
            && observed["value"]["body"]["value"]["field"].as_str().is_some_and(|field| field == "text" || field.ends_with(".text"));
        observations.check("identified-bad-body",correlated,json!({"frame":frame,"answer":"invalidShape","field":"text or a path ending in .text"}),observed);
        let follow_up: ClientFrame<SampleProtocol> = ClientFrame::Request {
            frame: FrameId::new(durable_steps::Uuid::now_v7())?,
            body: OperationRequest::Consumer(fleet_sync_test_consumer::SampleRequest::Echo {text:"after-bad-body".into()}),
        };
        let follow_up_frame = match &follow_up {ClientFrame::Request {frame,..}=>*frame,_=>return Err("bad follow-up fixture".into())};
        let echo: TestResult<_> = async {
            socket.send(serde_json::to_vec(&follow_up)?.into()).await?;
            receive_json(&mut socket).await
        }.await;
        let observed = match echo {Ok(value)=>value,Err(error)=>json!({"error":error.to_string()})};
        observations.check("same-session-after-bad-body",observed["kind"] == "answer" && observed["value"]["frame"] == json!(follow_up_frame)
            && observed["value"]["body"]["value"]["Echo"]["text"] == "after-bad-body",json!({"frame":follow_up_frame,"text":"after-bad-body"}),observed);
        socket.close(CloseReason::ClientClosed).await;
        let mut unreadable = attach_peer(&service,new_peer()?,100).await?;
        unreadable.send(b"{unreadable".to_vec().into()).await?;
        let closed = receive_json(&mut unreadable).await?;
        observations.check("unreadable-envelope",closed["kind"]=="closed" && closed["value"]["reason"]["kind"]=="malformedFrame",json!({"close":"malformedFrame"}),closed);
        unreadable.close(CloseReason::ClientClosed).await;
        Ok(())
    }.await;
    service.stop(&mut control).await?;
    result
}
