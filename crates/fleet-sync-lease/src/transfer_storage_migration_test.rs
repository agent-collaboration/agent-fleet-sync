#![allow(clippy::panic_in_result_fn)]
use crate::{registry_records::*, registry_storage::RegistryStorage, turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{migrate::Migrator, query::query, row::Row};
use sqlx_turso::Turso;

const DATABASE: &str = "019eef00-0000-7000-8000-000000000041";
const ATTEMPT: &str = "019eef00-0000-7000-8000-000000000009";
const RENEWAL: &str = r#"{"request":{"database":"019eef00-0000-7000-8000-000000000041","attempt":"019eef00-0000-7000-8000-000000000008","epoch":18446744073709551615},"holder":"019eef00-0000-7000-8000-000000000001","answer":{"kind":"renewed","value":{"recorded":{"epoch":18446744073709551615,"expiresAt":3000},"current":{"database":"019eef00-0000-7000-8000-000000000041","epoch":18446744073709551615,"holder":"019eef00-0000-7000-8000-000000000001","state":"held","expiresAt":3000,"remainingMs":18446744073709551615},"transferInProgress":"019eef00-0000-7000-8000-000000000009","replayed":false}}}"#;
const REQUEST: &str = r#"{"kind":"takeover","value":{"attempt":"019eef00-0000-7000-8000-000000000009","database":"019eef00-0000-7000-8000-000000000041","preview":{"token":"019eef00-0000-7000-8000-000000000007","fence":18446744073709551615,"acked":{"epoch":18446744073709551615,"sequence":18446744073709551615},"holder":"019eef00-0000-7000-8000-000000000001","lastSeen":900},"to":"019eef00-0000-7000-8000-000000000002","reason":"preserve original reason"}}"#;
const OUTCOME: &str = r#"{"kind":"refused","value":{"kind":"schemaTooNew","value":{"store":{"version":18446744073709551615,"fingerprint":"frozen legacy schema"},"supported":18446744073709551615}}}"#;
const HISTORY: &str = r#"[{"phase":{"phase":"requested"},"enteredAt":1000},{"phase":{"phase":"fencing"},"enteredAt":1100},{"phase":{"phase":"ended"},"enteredAt":1200}]"#;
const LIFECYCLE_INTENT: &str = r#"{"kind":"register","request":{"attempt":"019eef00-0000-7000-8000-000000000006","database":"019eef00-0000-7000-8000-000000000041","writer":"019eef00-0000-7000-8000-000000000001","origin":{"kind":"seeded","value":{"position":{"epoch":18446744073709551615,"sequence":18446744073709551615}}}}}"#;
const LIFECYCLE_OUTCOME: &str = r#"{"kind":"registration","answer":{"kind":"registered","value":{"lease":{"database":"019eef00-0000-7000-8000-000000000041","epoch":18446744073709551615,"holder":"019eef00-0000-7000-8000-000000000001","state":"held","expiresAt":3000,"remainingMs":18446744073709551615},"replayed":false}}}"#;

#[tokio::test]
async fn frozen_legacy_workflow_receipts_and_history_migrate_and_reopen_with_every_field()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host.clone(), false).await?;
    store.transaction(|scope|Box::pin(async move {
        let mut old=Migrator::with_migrations(LEASE_MIGRATOR.sql_migrations().filter(|m|m.version<=4).cloned().collect::<Vec<_>>());
        old.dangerous_set_table_name("_fleet_sync_registry_migrations");
        old.run_direct(None,scope.executor(),false).await.map_err(storage)?;
        query::<Turso>("INSERT INTO _fleet_sync_renewal_attempts(database,attempt,record) VALUES(?1,'019eef00-0000-7000-8000-000000000008',?2)").bind(DATABASE).bind(RENEWAL).execute(scope.executor()).await?;
        query::<Turso>("INSERT INTO _fleet_sync_transfer_admissions(attempt,operation,request,requested_by,admitted_at,follow_up,target_schema) VALUES(?1,?2,?3,?4,'1000',?5,?6)").bind(ATTEMPT).bind("\"019eef00-0000-7000-8000-000000000010\"").bind(REQUEST).bind("\"actor:tests\"").bind("\"019eef00-0000-7000-8000-000000000011\"").bind("{\"maxVersion\":18446744073709551615}").execute(scope.executor()).await?;
        query::<Turso>("INSERT INTO _fleet_sync_transfers(attempt,database,intent,outcome,phase,phase_history) VALUES(?1,?2,'18446744073709551615',?3,'{\"phase\":\"ended\"}',?4)").bind(ATTEMPT).bind(DATABASE).bind(OUTCOME).bind(HISTORY).execute(scope.executor()).await?;
        query::<Turso>("INSERT INTO _fleet_sync_lifecycle_attempts(attempt,database,intent,outcome) VALUES('019eef00-0000-7000-8000-000000000006',?1,?2,?3)").bind(DATABASE).bind(LIFECYCLE_INTENT).bind(LIFECYCLE_OUTCOME).execute(scope.executor()).await?;
        let all=LEASE_MIGRATOR.with_history_table("_fleet_sync_registry_migrations");
        all.run_direct(None,scope.executor(),false).await.map_err(storage)
    })).await?;
    drop(store);
    let store = open_store(&directory.path().join("registry.db"), host, true).await?;
    store
        .transaction(|scope| {
            Box::pin(async move {
                let expected: RenewalAttempt = serde_json::from_str(RENEWAL).map_err(storage)?;
                let mut registry = RegistryStorage(scope);
                assert_eq!(
                    registry
                        .renewal(expected.request.database, expected.request.attempt)
                        .await
                        .map_err(storage)?,
                    Some(expected)
                );
                let request: HolderChangeRequest =
                    serde_json::from_str(REQUEST).map_err(storage)?;
                let id = crate::change_checks::attempt(&request);
                let admission = registry
                    .admission(id)
                    .await
                    .map_err(storage)?
                    .ok_or_else(|| storage("missing admission"))?;
                let target = SchemaSupport {
                    max_version: u64::MAX,
                };
                assert_eq!(admission.request, request);
                assert_eq!(
                    admission.requested_by,
                    ActorRef::new("actor:tests").map_err(storage)?
                );
                assert_eq!(admission.admitted_at, SyncTime::from_unix_millis(1000));
                assert_eq!(admission.target_schema, Some(target));
                assert_eq!(
                    admission.operation.as_uuid().to_string(),
                    "019eef00-0000-7000-8000-000000000010"
                );
                assert_eq!(
                    admission
                        .follow_up
                        .map(|operation| operation.as_uuid().to_string())
                        .as_deref(),
                    Some("019eef00-0000-7000-8000-000000000011")
                );
                let expected = TransferRecord {
                    intent: TransferIntent {
                        request,
                        new_epoch: Some(Epoch::new(u64::MAX).map_err(storage)?),
                        target_schema: Some(target),
                    },
                    outcome: Some(serde_json::from_str(OUTCOME).map_err(storage)?),
                    phase: HolderChangePhase::Ended,
                    phase_history: serde_json::from_str(HISTORY).map_err(storage)?,
                    cleanup_failed: None,
                };
                assert_eq!(
                    registry.transfer(id).await.map_err(storage)?,
                    Some(expected)
                );
                let times =
                    query::<Turso>("SELECT created_at,updated_at FROM _fleet_sync_transfers")
                        .fetch_one(registry.0.executor())
                        .await?;
                assert_eq!(times.try_get::<i64, _>("created_at")?, 1000);
                assert_eq!(times.try_get::<i64, _>("updated_at")?, 1200);
                let expected = LifecycleAttempt {
                    intent: serde_json::from_str(LIFECYCLE_INTENT).map_err(storage)?,
                    outcome: Some(serde_json::from_str(LIFECYCLE_OUTCOME).map_err(storage)?),
                };
                assert_eq!(
                    registry
                        .lifecycle(expected.intent.attempt())
                        .await
                        .map_err(storage)?,
                    Some(expected)
                );
                for table in [
                    "_fleet_sync_renewal_attempts",
                    "_fleet_sync_transfers",
                    "_fleet_sync_transfer_admissions",
                    "_fleet_sync_lifecycle_attempts",
                ] {
                    for row in query::<Turso>("SELECT name FROM pragma_table_info(?1)")
                        .bind(table)
                        .fetch_all(registry.0.executor())
                        .await?
                    {
                        let column = row.try_get::<String, _>("name")?;
                        assert!(
                            ![
                                "record",
                                "request",
                                "intent",
                                "outcome",
                                "phase_history",
                                "cleanup_failed",
                                "target_schema"
                            ]
                            .contains(&column.as_str()),
                            "serialized library field remains: {table}.{column}"
                        );
                    }
                }
                Ok(())
            })
        })
        .await?;
    Ok(())
}
