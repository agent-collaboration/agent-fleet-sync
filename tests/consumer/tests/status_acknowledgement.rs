#![allow(clippy::unwrap_used)]
#![cfg(feature = "test-support")]
#[expect(
    dead_code,
    unused_imports,
    reason = "shared native process helpers serve several journeys"
)]
mod support;
use durable_steps::{CallId, OperationId, StepName, Uuid};
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::TestResult;
use support::*;

#[tokio::test]
async fn empty_status_acknowledgement_uses_the_status_owner_and_replays_after_service_restart()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("relay");
    let mut relay = ServiceProcess::start(&path).await?;
    let call = CallId {
        operation: OperationId::new(Uuid::now_v7())?,
        step: StepName::new("status-ack")?,
    };
    let mut socket = relay.peer(2, SessionId::new(Uuid::now_v7())?).await?;
    for through in [0, 1, u64::MAX, 0] {
        let answer = request(
            &mut socket,
            OperationRequest::Delivery(DeliveryRequest::StatusAck {
                call: call.clone(),
                through: StatusCursor::new(through),
            }),
        )
        .await?;
        let OperationAnswer::Delivery(DeliveryAnswer::StatusAck(result)) = answer else {
            return Err("wrong status acknowledgement answer".into());
        };
        let expected = if through == 0 {
            StatusAckResult::Acknowledged {
                floor: StatusCursor::new(0),
            }
        } else {
            StatusAckResult::Refused {
                head: StatusCursor::new(0),
            }
        };
        assert_eq!(result, expected);
    }
    drop(socket);
    relay.kill().await?;
    let mut relay = ServiceProcess::start(&path).await?;
    let mut socket = relay.peer(2, SessionId::new(Uuid::now_v7())?).await?;
    let answer = request(
        &mut socket,
        OperationRequest::Delivery(DeliveryRequest::StatusAck {
            call,
            through: StatusCursor::new(0),
        }),
    )
    .await?;
    assert!(
        matches!(answer, OperationAnswer::Delivery(DeliveryAnswer::StatusAck(StatusAckResult::Acknowledged { floor })) if floor == StatusCursor::new(0))
    );
    let answer = request(
        &mut socket,
        OperationRequest::Delivery(DeliveryRequest::StatusFetch {
            call: CallId {
                operation: OperationId::new(Uuid::now_v7())?,
                step: StepName::new("status-fetch")?,
            },
            after: StatusCursor::new(0),
            limit: 10,
        }),
    )
    .await?;
    assert!(
        matches!(answer, OperationAnswer::Delivery(DeliveryAnswer::StatusFetch(batch)) if batch.events.is_empty() && batch.through == StatusCursor::new(0))
    );
    drop(socket);
    relay.kill().await?;
    Ok(())
}
