use crate::{Observations, fixture_service::*, import_fixture::*, support::*};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest, TestResult};
use serde_json::json;
use std::path::Path;

pub async fn run(root: &Path, observations: &mut Observations) -> TestResult {
    let service = start_service(&root.join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let result:TestResult = async {
        let (database,_) = publish_unheld(&service,&mut control,&root.join("backup.db")).await?;
        let holder = new_peer()?;
        let claimant = new_peer()?;
        let next_holder = new_peer()?;
        let mut holder_socket = attach_peer(&service,holder,5).await?;
        let mut claimant_socket = attach_peer(&service,claimant,5).await?;
        let next_socket = attach_peer(&service,next_holder,5).await?;
        let granted = claim(&mut holder_socket,&mut control,holder,database,RequestId::new(durable_steps::Uuid::now_v7())?).await?;
        if !matches!(granted,HolderChangeAnswer::Granted {..}) {return Err("initial holder control did not grant".into());}
        let attempt = RequestId::new(durable_steps::Uuid::now_v7())?;
        let refused = claim(&mut claimant_socket,&mut control,claimant,database,attempt).await?;
        let first = serde_json::to_value(&refused)?;
        observations.check("already-held-detail",first["kind"]=="refused" && first["value"]["kind"]=="alreadyHeld" && first["value"]["value"]["holder"]==json!(holder),json!({"kind":"alreadyHeld","holder":holder}),first.clone());
        let stored = match control_request(&mut control,ControlRequest::HolderChangeView {attempt:TransferId::new(attempt.as_uuid())?}).await? {
            ControlAnswer::HolderChangeView(Some(view))=>serde_json::to_value(view.outcome)?,_=>return Err("stored refusal view missing".into()),
        };
        observations.check("stored-refusal",stored==first,first.clone(),stored);
        let preview = match control_request(&mut control,ControlRequest::PreviewTakeover {database,attempt:durable_steps::Uuid::now_v7()}).await? {
            ControlAnswer::TakeoverPreview(preview)=>preview,_=>return Err("takeover preview missing".into()),
        };
        let takeover = HolderChangeRequest::Takeover {attempt:TransferId::new(durable_steps::Uuid::now_v7())?,database,preview,to:next_holder};
        control_request(&mut control,ControlRequest::HolderChange(takeover.clone())).await?;
        let changed = match control_request(&mut control,ControlRequest::WaitHolderChange(takeover)).await? {ControlAnswer::HolderChange(answer)=>answer,_=>return Err("takeover answer missing".into())};
        if !matches!(changed,HolderChangeAnswer::Granted {..}) {return Err("holder change control did not grant".into());}
        let repeated = serde_json::to_value(claim(&mut claimant_socket,&mut control,claimant,database,attempt).await?)?;
        observations.check("refusal-after-holder-change",repeated==first && repeated["value"]["value"]["holder"]==json!(holder),json!({"recorded":first,"holder":holder}),repeated);
        for (migration_state, suffix) in [(MigrationState::Pending,"pending"),(MigrationState::Failed,"failed")] {
            let (migration_database,_) = publish_unheld(&service,&mut control,&root.join(format!("{suffix}-backup.db"))).await?;
            let epoch = match claim(&mut holder_socket,&mut control,holder,migration_database,RequestId::new(durable_steps::Uuid::now_v7())?).await? {
                HolderChangeAnswer::Granted {epoch,..}=>epoch,
                _=>return Err("migration fixture holder was not granted".into()),
            };
            let change=HolderChangeRequest::Move {attempt:TransferId::new(durable_steps::Uuid::now_v7())?,database:migration_database,from_epoch:epoch,to:next_holder};
            let answer=answer_move_from_holder(&mut control,&mut holder_socket,change,ReleaseAnswer::ReleaseRefused(ReleaseRefusal::MigrationPending {migration:7,state:migration_state})).await?;
            let observed=serde_json::to_value(answer)?;
            observations.check(&format!("{suffix}-migration-detail"),observed["kind"]=="refused" && observed["value"]["kind"]=="migrationPending" && observed["value"]["value"]["migration"]==7 && observed["value"]["value"]["state"]==json!(migration_state),json!({"migration":7,"state":migration_state}),observed);
        }
        holder_socket.close(CloseReason::ClientClosed).await;
        claimant_socket.close(CloseReason::ClientClosed).await;
        next_socket.close(CloseReason::ClientClosed).await;
        observations.gap("native-writer-migration-production","Migration states were supplied by an owned fixture holder over the real session. Native writer pending/failed state production was not observed.");
        Ok(())
    }.await;
    service.stop(&mut control).await?;
    result
}
