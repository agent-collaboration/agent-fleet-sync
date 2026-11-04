use super::support::*;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::TestResult;

fn peer(number: u128) -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000000 | number,
    ))?)
}
fn call() -> TestResult<CallId> {
    Ok(CallId {
        operation: OperationId::new(durable_steps::Uuid::now_v7())?,
        step: StepName::new("settle claim")?,
    })
}
#[tokio::test]
async fn legacy_settle_refuses_other_machine_and_project_claims_before_custody() -> TestResult {
    let directory = tempfile::tempdir()?;
    let service = ServiceProcess::start(directory.path()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = service
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    for claim in [
        Submitter::Machine(peer(9)?),
        Submitter::Project(DatabaseId::try_from(peer(2)?.as_uuid().to_string())?),
    ] {
        let item = ItemId::new(durable_steps::Uuid::now_v7())?;
        let digest = ItemDigest::try_from("a".repeat(64))?;
        let answer = request(
            &mut socket,
            OperationRequest::Delivery(DeliveryRequest::Settle {
                call: call()?,
                mailbox: peer(4)?,
                submitter: claim,
                item,
                digest: digest.clone(),
            }),
        )
        .await?;
        let observed = serde_json::to_value(&answer)?;
        assert!(
            matches!(
                answer,
                OperationAnswer::Delivery(DeliveryAnswer::DeliveryUnavailable {
                    reason: DeliveryRefusal::NotSubmitter
                })
            ),
            "an unauthorized/unsupported claim must be refused before settling: {observed}"
        );
        let query = request(
            &mut socket,
            OperationRequest::Delivery(DeliveryRequest::Query {
                call: call()?,
                mailbox: peer(4)?,
                items: vec![item],
            }),
        )
        .await?;
        assert!(
            matches!(query,OperationAnswer::Delivery(DeliveryAnswer::Query(result)) if result.statuses.first().is_some_and(|status|matches!(status.state,ItemStatusState::Unknown)))
        );
        let accepted = request(
            &mut socket,
            OperationRequest::Delivery(DeliveryRequest::Settle {
                call: call()?,
                mailbox: peer(4)?,
                submitter: Submitter::Machine(peer(2)?),
                item,
                digest,
            }),
        )
        .await?;
        assert!(matches!(
            accepted,
            OperationAnswer::Delivery(DeliveryAnswer::Settle(SettleResult::Settled {
                reason: DeliveryFailureKind::CustodyUnresolvable,
                ..
            }))
        ));
    }
    drop(socket);
    service.stop(&mut control).await?;
    Ok(())
}
