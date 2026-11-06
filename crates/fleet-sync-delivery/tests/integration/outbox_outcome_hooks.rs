//! Every terminal writer must project once, including body-less recovery.
use super::support::{network::PublishTask, outbox_projection::*, *};
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

type OutcomePush = PushItem<TestStore, Turso, TestTypes, PublishTask, OutcomeBindings>;

#[derive(Clone, Copy)]
enum Terminal {
    Delivered,
    Uncertain,
    Expired,
    ExpiredAtUpgrade,
    Cancelled,
    Refused,
}
const TERMINALS: [Terminal; 6] = [
    Terminal::Delivered,
    Terminal::Uncertain,
    Terminal::Expired,
    Terminal::ExpiredAtUpgrade,
    Terminal::Cancelled,
    Terminal::Refused,
];

impl Terminal {
    fn status(self) -> DeliveryStatus<Acceptance> {
        match self {
            Self::Delivered => DeliveryStatus::Delivered {
                acceptance: Acceptance {
                    receipt: "projected receipt".into(),
                },
            },
            Self::Uncertain => DeliveryStatus::Uncertain,
            Self::Expired => DeliveryStatus::Expired,
            Self::ExpiredAtUpgrade => DeliveryStatus::ExpiredAtUpgrade,
            Self::Cancelled => DeliveryStatus::Cancelled,
            Self::Refused => DeliveryStatus::Refused {
                reason: DeliveryRefusal::RecipientRevoked,
            },
        }
    }
    // Literal contract values: independent of the status serializer under test.
    fn expected(self) -> serde_json::Value {
        match self {
            Self::Delivered => {
                serde_json::json!({"kind":"delivered","value":{"acceptance":{"receipt":"projected receipt"}}})
            }
            Self::Uncertain => serde_json::json!({"kind":"uncertain"}),
            Self::Expired => serde_json::json!({"kind":"expired"}),
            Self::ExpiredAtUpgrade => serde_json::json!({"kind":"expiredAtUpgrade"}),
            Self::Cancelled => serde_json::json!({"kind":"cancelled"}),
            Self::Refused => {
                serde_json::json!({"kind":"refused","value":{"reason":{"kind":"recipientRevoked"}}})
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Transition {
    Submitted(Terminal),
    ImmediateRefusal,
    CancelConflict,
    SubmissionFailure,
    TaskFailure,
    Publication(Terminal),
    Batch(Terminal),
    Receipt(Terminal),
    Report(Terminal),
}

async fn transition(fixture: &Fixture, change: Transition, refuse: bool) -> Result<(), StoreError> {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let binding = OutcomeBindings { refuse };
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &binding);
                let now = SyncTime::from_unix_millis(0);
                match change {
                    Transition::Submitted(terminal) => {
                        outbox
                            .mark_submitted(
                                &submit_input(),
                                SubmitResult::Accepted {
                                    status: terminal.status(),
                                    version: StatusVersion::new(1),
                                    cancel: None,
                                },
                                now,
                            )
                            .await
                    }
                    Transition::ImmediateRefusal => {
                        outbox
                            .mark_submitted(
                                &submit_input(),
                                SubmitResult::Refused {
                                    reason: DeliveryRefusal::RecipientRevoked,
                                },
                                now,
                            )
                            .await
                    }
                    Transition::CancelConflict => {
                        outbox
                            .mark_submitted(
                                &submit_input(),
                                SubmitResult::CancelAlreadyRequested {
                                    existing: DeliveryItemRef {
                                        item: item_id(9),
                                        digest: item(9, "cancel").digest().clone(),
                                    },
                                    status: DeliveryStatus::Sent,
                                    version: StatusVersion::new(1),
                                    cancel: None,
                                },
                                now,
                            )
                            .await
                    }
                    Transition::SubmissionFailure => {
                        outbox
                            .record_submission_failure(
                                &submit_input(),
                                DeliveryFailureKind::CustodyMissing,
                                now,
                            )
                            .await
                    }
                    Transition::TaskFailure => {
                        outbox
                            .record_failure(item_id(1), DeliveryFailureKind::ExecutionFailed, now)
                            .await
                    }
                    Transition::Publication(terminal) => {
                        outbox
                            .publish_status(
                                StatusPublication {
                                    item: item_id(1),
                                    status: terminal.status(),
                                    version: StatusVersion::new(2),
                                    cancel: None,
                                },
                                now,
                            )
                            .await
                    }
                    Transition::Batch(terminal) => {
                        outbox
                            .apply_status_batch(
                                StatusCursor::new(0),
                                StatusFetchResult {
                                    events: vec![StatusEvent {
                                        cursor: StatusCursor::new(1),
                                        item: item_id(1),
                                        status: terminal.status(),
                                        version: StatusVersion::new(2),
                                        cancel: None,
                                    }],
                                    through: StatusCursor::new(1),
                                },
                                now,
                            )
                            .await
                    }
                    Transition::Receipt(terminal) => outbox
                        .receipt::<PublishTask>(
                            peer(2),
                            &submit_input().reference(),
                            PushResult::Replayed {
                                status: terminal.status(),
                                cancel: None,
                            },
                            now,
                        )
                        .await
                        .map(|_| ()),
                    Transition::Report(terminal) => {
                        let outcome = match terminal {
                            Terminal::Delivered => RecipientOutcome::Delivered {
                                acceptance: Acceptance {
                                    receipt: "projected receipt".into(),
                                },
                            },
                            Terminal::Uncertain => RecipientOutcome::Uncertain,
                            Terminal::Expired => RecipientOutcome::Expired,
                            Terminal::Refused => RecipientOutcome::Refused {
                                reason: DeliveryRefusal::RecipientRevoked,
                            },
                            Terminal::Cancelled | Terminal::ExpiredAtUpgrade => {
                                return Err(StoreError::backend(
                                    "report fixture requires a recipient outcome",
                                ));
                            }
                        };
                        outbox
                            .recipient_report::<PublishTask>(
                                peer(2),
                                RecipientReport {
                                    item: item_id(1),
                                    report_seq: ReportSeq::new(1),
                                    outcome,
                                },
                                now,
                            )
                            .await
                            .map(|_| ())
                    }
                }
                .map_err(storage_error)
            })
        })
        .await
}

async fn assert_once(fixture: &Fixture, change: Transition, expected: serde_json::Value) {
    transition(fixture, change, false).await.unwrap();
    let first = projected(fixture).await;
    assert_eq!(first, vec![(item_id(1).as_uuid().to_string(), expected)]);
    transition(fixture, change, false).await.unwrap();
    assert_eq!(
        projected(fixture).await,
        first,
        "replay must not project a second time"
    );
}

async fn sender_fixture() -> Fixture {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    enqueue(&fixture).await;
    fixture
}

async fn relay_fixture(expiry: Option<SyncTime>, now: SyncTime) -> Fixture {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let original = DeliveryItem::<TestTypes>::new(
                    item_id(1),
                    "sender".into(),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    expiry,
                    "outbox body".into(),
                )
                .unwrap();
                Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS)
                    .submit_relay::<OutcomePush, RecordRemoval>(
                        Submitter::Machine(peer(1)),
                        DeliveryEnvelope::Item(original),
                        peer(3),
                        "route".into(),
                        now,
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture
}

#[tokio::test]
async fn submit_answer_projects_each_terminal_status_once() {
    for terminal in TERMINALS {
        let fixture = sender_fixture().await;
        assert_once(
            &fixture,
            Transition::Submitted(terminal),
            terminal.expected(),
        )
        .await;
    }
}

#[tokio::test]
async fn immediate_submit_refusal_projects_once() {
    let fixture = sender_fixture().await;
    assert_once(
        &fixture,
        Transition::ImmediateRefusal,
        Terminal::Refused.expected(),
    )
    .await;
}

#[tokio::test]
async fn duplicate_cancel_refusal_projects_once() {
    let fixture = sender_fixture().await;
    let expected = serde_json::json!({"kind":"refused","value":{"reason":{"kind":"cancelAlreadyRequested","value":{"existing":{"item":item_id(9),"digest":item(9,"cancel").digest()}}}}});
    assert_once(&fixture, Transition::CancelConflict, expected).await;
}

async fn assert_submission_failure(present: bool) {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    if present {
        enqueue(&fixture).await;
    }
    assert_once(&fixture,Transition::SubmissionFailure,serde_json::json!({"kind":"refused","value":{"reason":{"kind":"processingFailed","value":{"kind":"custodyMissing"}}}})).await;
}

#[tokio::test]
async fn submission_failure_projects_existing_custody() {
    assert_submission_failure(true).await;
}

#[tokio::test]
async fn submission_failure_projects_missing_custody() {
    assert_submission_failure(false).await;
}

async fn assert_task_failure(state: &'static str) {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    if state != "missing" {
        enqueue(&fixture).await;
    }
    if state == "corrupt" {
        fixture
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    query::<Turso>("UPDATE _fleet_sync_outbox_items SET payload='broken'")
                        .execute(scope.executor())
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
    if state == "corrupt-header" {
        fixture
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    query::<Turso>("UPDATE _fleet_sync_outbox_items SET digest=NULL")
                        .execute(scope.executor())
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
    assert_once(&fixture,Transition::TaskFailure,serde_json::json!({"kind":"refused","value":{"reason":{"kind":"processingFailed","value":{"kind":"executionFailed"}}}})).await;
}

#[tokio::test]
async fn task_failure_projects_intact_custody() {
    assert_task_failure("intact").await;
}

#[tokio::test]
async fn task_failure_projects_missing_custody() {
    assert_task_failure("missing").await;
}

#[tokio::test]
async fn task_failure_projects_corrupt_body() {
    assert_task_failure("corrupt").await;
}

#[tokio::test]
async fn task_failure_projects_corrupt_header() {
    assert_task_failure("corrupt-header").await;
}

#[tokio::test]
async fn terminal_replay_after_reopen_and_newer_publication_projects_nothing() {
    let fixture = sender_fixture().await;
    assert_once(
        &fixture,
        Transition::Publication(Terminal::Delivered),
        Terminal::Delivered.expected(),
    )
    .await;
    let Fixture {
        store,
        host,
        directory,
    } = fixture;
    drop(store);
    let fixture = Fixture {
        store: Arc::new(
            TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
                .await
                .unwrap(),
        ),
        host,
        directory,
    };
    transition(
        &fixture,
        Transition::Publication(Terminal::Delivered),
        false,
    )
    .await
    .unwrap();
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS)
                    .publish_status(
                        StatusPublication {
                            item: item_id(1),
                            status: Terminal::Delivered.status(),
                            version: StatusVersion::new(3),
                            cancel: None,
                        },
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    assert_eq!(
        projected(&fixture).await,
        vec![(
            item_id(1).as_uuid().to_string(),
            Terminal::Delivered.expected()
        )]
    );
}

#[tokio::test]
async fn recovery_of_a_terminal_submit_answer_projects_without_a_body() {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    assert_once(
        &fixture,
        Transition::Submitted(Terminal::Delivered),
        Terminal::Delivered.expected(),
    )
    .await;
}

#[tokio::test]
async fn publication_projects_after_nonterminal_handoff_compacted_the_payload() {
    for terminal in TERMINALS {
        let fixture = sender_fixture().await;
        handoff(&fixture).await;
        assert_once(
            &fixture,
            Transition::Publication(terminal),
            terminal.expected(),
        )
        .await;
    }
}

async fn handoff(fixture: &Fixture) {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS)
                    .mark_submitted(
                        &submit_input(),
                        SubmitResult::Accepted {
                            status: DeliveryStatus::Sent,
                            version: StatusVersion::new(1),
                            cancel: None,
                        },
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                let row = query::<Turso>("SELECT payload FROM _fleet_sync_outbox_items")
                    .fetch_one(scope.executor())
                    .await?;
                assert!(row.try_get::<Option<String>, _>("payload")?.is_none());
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(
        projected(fixture).await.is_empty(),
        "nonterminal handoff must not project"
    );
}

#[tokio::test]
async fn batch_projects_terminal_status_and_replay_does_not_project() {
    let fixture = sender_fixture().await;
    assert_once(
        &fixture,
        Transition::Batch(Terminal::Uncertain),
        Terminal::Uncertain.expected(),
    )
    .await;
}

#[tokio::test]
async fn recovered_nonterminal_publication_projects_on_first_terminal_advance() {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    handoff(&fixture).await;
    assert_once(
        &fixture,
        Transition::Publication(Terminal::Delivered),
        Terminal::Delivered.expected(),
    )
    .await;
}

#[tokio::test]
async fn recovered_nonterminal_task_failure_projects_once() {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    handoff(&fixture).await;
    assert_once(&fixture,Transition::TaskFailure,serde_json::json!({"kind":"refused","value":{"reason":{"kind":"processingFailed","value":{"kind":"executionFailed"}}}})).await;
}

#[tokio::test]
async fn relay_admission_expiry_projects_once() {
    let fixture = relay_fixture(
        Some(SyncTime::from_unix_millis(0)),
        SyncTime::from_unix_millis(0),
    )
    .await;
    assert_eq!(
        projected(&fixture).await,
        vec![(
            item_id(1).as_uuid().to_string(),
            Terminal::Expired.expected()
        )]
    );
}

#[tokio::test]
async fn relay_receipt_projects_each_terminal_answer_once() {
    for terminal in [
        Terminal::Delivered,
        Terminal::Uncertain,
        Terminal::Expired,
        Terminal::Refused,
    ] {
        let fixture = relay_fixture(None, SyncTime::from_unix_millis(0)).await;
        assert_once(&fixture, Transition::Receipt(terminal), terminal.expected()).await;
    }
}

#[tokio::test]
async fn relay_recipient_report_projects_terminal_outcome_once() {
    for terminal in [
        Terminal::Delivered,
        Terminal::Uncertain,
        Terminal::Expired,
        Terminal::Refused,
    ] {
        let fixture = relay_fixture(None, SyncTime::from_unix_millis(0)).await;
        assert_once(&fixture, Transition::Report(terminal), terminal.expected()).await;
    }
}

#[tokio::test]
async fn withdrawing_before_submit_projects_original_and_control_once() {
    let fixture = sender_fixture().await;
    for _ in 0..2 {
        fixture
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let DeliveryEnvelope::Cancel(control) = cancel(2, 1) else {
                        return Err(StoreError::backend("fixture control"));
                    };
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS)
                        .enqueue_cancel(
                            control,
                            peer(3),
                            Submitter::Machine(peer(1)),
                            SyncTime::from_unix_millis(0),
                            &TestConsumer,
                        )
                        .await
                        .map_err(storage_error)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_eq!(
            projected(&fixture).await,
            vec![
                (
                    item_id(1).as_uuid().to_string(),
                    Terminal::Cancelled.expected()
                ),
                (
                    item_id(2).as_uuid().to_string(),
                    Terminal::Cancelled.expected()
                )
            ]
        );
    }
}

#[tokio::test]
async fn push_deadline_projects_expired_inside_before_resume_transaction() {
    let fixture = relay_fixture(
        Some(SyncTime::from_unix_millis(5)),
        SyncTime::from_unix_millis(0),
    )
    .await;
    fixture.host.clock.store(5, Ordering::SeqCst);
    let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
    engine
        .register(OutcomePush::new(
            limits(),
            fixture.host.clone(),
            Arc::new(OUTCOME_BINDINGS),
        ))
        .unwrap();
    let result = engine.drive_due().await.unwrap();
    assert_eq!(result.completed.len(), 1);
    assert!(result.failed.is_empty());
    assert_eq!(
        projected(&fixture).await,
        vec![(
            item_id(1).as_uuid().to_string(),
            Terminal::Expired.expected()
        )]
    );
    assert!(engine.drive_due().await.unwrap().completed.is_empty());
    assert_eq!(projected(&fixture).await.len(), 1);
}

#[tokio::test]
async fn hook_error_rolls_back_status_cursor_and_projection_then_retry_commits_once() {
    for change in [
        Transition::ImmediateRefusal,
        Transition::SubmissionFailure,
        Transition::TaskFailure,
        Transition::Batch(Terminal::Refused),
    ] {
        let fixture = sender_fixture().await;
        assert!(
            transition(&fixture, change, true).await.is_err(),
            "hook error must abort custody"
        );
        assert!(projected(&fixture).await.is_empty());
        fixture
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let mut outbox =
                        Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS);
                    assert!(matches!(
                        outbox
                            .status(item_id(1))
                            .await
                            .map_err(storage_error)?
                            .state,
                        OutboxStatusState::Known {
                            status: DeliveryStatus::Queued,
                            ..
                        }
                    ));
                    assert_eq!(
                        outbox.status_cursor().await.map_err(storage_error)?.value(),
                        0
                    );
                    Ok(())
                })
            })
            .await
            .unwrap();
        transition(&fixture, change, false).await.unwrap();
        assert_eq!(projected(&fixture).await.len(), 1);
    }
}

#[tokio::test]
async fn expiry_gate_releases_without_terminal_projection() {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS);
                let original = DeliveryItem::<TestTypes>::new(
                    item_id(1),
                    "sender".into(),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    Some(SyncTime::from_unix_millis(0)),
                    "expired body".into(),
                )
                .unwrap();
                let input = SubmitInput {
                    item: item_id(1),
                    digest: original.digest().clone(),
                    relay: peer(3),
                    mailbox: peer(2),
                };
                outbox
                    .enqueue(
                        original,
                        peer(3),
                        Submitter::Machine(peer(1)),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(
                    outbox
                        .gate(&input, &TestConsumer, SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?,
                    GateDecision::Release
                ));
                assert!(matches!(
                    outbox
                        .status(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .state,
                    OutboxStatusState::Known {
                        status: DeliveryStatus::Queued,
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(projected(&fixture).await.is_empty());
}

#[tokio::test]
async fn retention_removes_nonterminal_custody_without_projecting_a_status() {
    let fixture = sender_fixture().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                admit_record_removal::<_, RecordRemoval>(scope, item_id(1))
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    fixture.host.clock.store(
        removal_deadline(item_id(1)).unix_millis() as u64,
        Ordering::SeqCst,
    );
    let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
    engine
        .register(RecordRemoval::new(DeliveryHostRole::Machine))
        .unwrap();
    assert_eq!(engine.drive_due().await.unwrap().completed.len(), 1);
    assert!(projected(&fixture).await.is_empty());
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS)
                        .status(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .state,
                    OutboxStatusState::Unknown
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn relay_settlement_projects_unknown_item_refusal_once() {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    for _ in 0..2 {
        fixture
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS)
                        .relay_settle::<RecordRemoval>(
                            Submitter::Machine(peer(1)),
                            item_id(1),
                            submit_input().digest,
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_eq!(
            projected(&fixture).await,
            vec![(
                item_id(1).as_uuid().to_string(),
                serde_json::json!({"kind":"refused","value":{"reason":{"kind":"processingFailed","value":{"kind":"custodyUnresolvable"}}}})
            )]
        );
    }
}

#[tokio::test]
async fn recovered_terminal_projection_survives_a_stale_nonterminal_submit_answer() {
    let fixture = Fixture::new().await;
    prepare(&fixture).await;
    handoff(&fixture).await;
    transition(
        &fixture,
        Transition::Publication(Terminal::Delivered),
        false,
    )
    .await
    .unwrap();
    // A publication can win the race with the submit reply while custody is
    // already recovered. The delayed nonterminal reply must not re-arm hooks.
    handoff_after_terminal(&fixture).await;
    transition(
        &fixture,
        Transition::Publication(Terminal::Delivered),
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        projected(&fixture).await,
        vec![(
            item_id(1).as_uuid().to_string(),
            Terminal::Delivered.expected()
        )]
    );
}

async fn handoff_after_terminal(fixture: &Fixture) {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &OUTCOME_BINDINGS)
                    .mark_submitted(
                        &submit_input(),
                        SubmitResult::Accepted {
                            status: DeliveryStatus::Sent,
                            version: StatusVersion::new(1),
                            cancel: None,
                        },
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
}
