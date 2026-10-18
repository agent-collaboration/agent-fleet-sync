use crate::{Observations, fixture_service::*, import_fixture::*};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::TestResult;
use serde_json::json;
use std::path::Path;

pub async fn run(root: &Path, observations: &mut Observations) -> TestResult {
    let service = start_service(&root.join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let result:TestResult = async {
        let (database,_) = publish_unheld(&service,&mut control,&root.join("backup.db")).await?;
        let claimant = new_peer().await?;
        let mut low = attach_peer(&service,claimant,4).await?;
        let attempt = RequestId::new(durable_steps::Uuid::now_v7())?;
        let first = serde_json::to_value(claim(&mut low,&mut control,claimant,database,attempt).await?)?;
        let schema_refusal = first["kind"]=="refused" && first["value"]["kind"]=="schemaTooNew" && first["value"]["value"]["store"]["version"]==5 && first["value"]["value"]["supported"]==4;
        observations.check("below-target-schema",schema_refusal,json!({"refusal":"schemaTooNew","store":5,"supported":4}),first.clone());
        low.close(CloseReason::ClientClosed).await;
        let mut raised = attach_peer(&service,claimant,5).await?;
        let repeated = serde_json::to_value(claim(&mut raised,&mut control,claimant,database,attempt).await?)?;
        observations.check("same-attempt-frozen-refusal",schema_refusal && repeated==first,json!({"recorded":first}),repeated);
        let (independent,_) = publish_unheld(&service,&mut control,&root.join("control-backup.db")).await?;
        let fresh = serde_json::to_value(claim(&mut raised,&mut control,claimant,independent,RequestId::new(durable_steps::Uuid::now_v7())?).await?)?;
        observations.check("supported-unheld-positive-control",fresh["kind"]=="granted",json!({"kind":"granted","schema":5}),fresh);
        raised.close(CloseReason::ClientClosed).await;
        let holder=new_peer().await?;
        let mut holder_socket=attach_peer(&service,holder,5).await?;
        let low_target=new_peer().await?;
        let low_socket=attach_peer(&service,low_target,4).await?;
        let high_target=new_peer().await?;
        let high_socket=attach_peer(&service,high_target,5).await?;
        for (case,target,supported) in [("move-below-target-schema",low_target,Some(4_u64)),("move-supported-target",high_target,Some(5)),("move-no-accepted-report",new_peer().await?,None)] {
            let (move_database,position)=publish_unheld(&service,&mut control,&root.join(format!("{case}.db"))).await?;
            let epoch=match claim(&mut holder_socket,&mut control,holder,move_database,RequestId::new(durable_steps::Uuid::now_v7())?).await? {
                HolderChangeAnswer::Granted {epoch,..}=>epoch,
                _=>return Err("move fixture holder was not granted".into()),
            };
            let change=HolderChangeRequest::Move {attempt:TransferId::new(durable_steps::Uuid::now_v7())?,database:move_database,from_epoch:epoch,to:target};
            let answer=serde_json::to_value(answer_move_from_holder(&mut control,&mut holder_socket,change,ReleaseAnswer::Released {final_position:position}).await?)?;
            let expected=if supported==Some(5) {json!({"kind":"granted"})} else {json!({"kind":"schemaTooNew","store":5,"supported":supported})};
            let passed=if supported==Some(5) {answer["kind"]=="granted"} else {answer["kind"]=="refused" && answer["value"]["kind"]=="schemaTooNew" && answer["value"]["value"]["store"]["version"]==5 && answer["value"]["value"].get("supported")==Some(&json!(supported))};
            observations.check(case,passed,expected,answer);
        }
        holder_socket.close(CloseReason::ClientClosed).await;
        low_socket.close(CloseReason::ClientClosed).await;
        high_socket.close(CloseReason::ClientClosed).await;
        for (case,detail) in [
            ("admission-before-check","C3 must supply the controlled drive barrier; then verify frozen support across publication and restart."),
            ("old-report-late","C3 must supply held report delivery; inspect the persisted attach sequence."),
            ("welcome-publication-barrier","C3 must supply the held new report; observe no Welcome before release and before/after admission."),
        ] {observations.gap(case,detail);}
        Ok(())
    }.await;
    service.stop(&mut control).await?;
    result
}
