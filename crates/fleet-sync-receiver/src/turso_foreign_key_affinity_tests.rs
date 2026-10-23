//! Mixed-affinity FK rules must survive engine-FK-off seed, replay and install.
#![allow(clippy::panic_in_result_fn)]
use crate::turso_foreign_key_tests::{NoHook, apply, export, incoming, relation_seed_pages, rows};
use crate::turso_rule_fixture::{uuid_key, writer};
use crate::turso_seed_tests::{TursoScope, database, position, setup};
use crate::*;
use fleet_sync_protocol::*;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_turso::{Turso, TursoConnectOptions};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct AffinityCase {
    name: &'static str,
    parent_type: &'static str,
    child_type: &'static str,
    parent: SqlValue,
    child: SqlValue,
    valid: bool,
}
fn cases() -> Vec<AffinityCase> {
    // Literal outcomes follow SQLite FK parent-affinity and parent-collation rules.
    // In particular NUMERIC affinity is not CAST: nonnumeric text stays text.
    vec![
        AffinityCase {
            name: "text-integer-valid",
            parent_type: "TEXT",
            child_type: "INTEGER",
            parent: SqlValue::Text("1".into()),
            child: SqlValue::Integer(1),
            valid: true,
        },
        AffinityCase {
            name: "text-integer-orphan",
            parent_type: "TEXT",
            child_type: "INTEGER",
            parent: SqlValue::Text("01".into()),
            child: SqlValue::Integer(1),
            valid: false,
        },
        AffinityCase {
            name: "numeric-text-valid",
            parent_type: "NUMERIC",
            child_type: "TEXT",
            parent: SqlValue::Integer(123),
            child: SqlValue::Text("00123".into()),
            valid: true,
        },
        AffinityCase {
            name: "numeric-text-orphan",
            parent_type: "NUMERIC",
            child_type: "TEXT",
            parent: SqlValue::Integer(0),
            child: SqlValue::Text("abc".into()),
            valid: false,
        },
        AffinityCase {
            name: "blob-integer-orphan",
            parent_type: "BLOB",
            child_type: "INTEGER",
            parent: SqlValue::Text("1".into()),
            child: SqlValue::Integer(1),
            valid: false,
        },
        AffinityCase {
            name: "blob-integer-valid",
            parent_type: "BLOB",
            child_type: "INTEGER",
            parent: SqlValue::Integer(1),
            child: SqlValue::Integer(1),
            valid: true,
        },
        AffinityCase {
            name: "parent-collation-valid",
            parent_type: "TEXT COLLATE NOCASE",
            child_type: "TEXT COLLATE BINARY",
            parent: SqlValue::Text("abc".into()),
            child: SqlValue::Text("ABC".into()),
            valid: true,
        },
    ]
}
impl AffinityCase {
    fn statements(&self) -> Vec<String> {
        vec![
            format!(
                "CREATE TABLE z_parent(id TEXT NOT NULL PRIMARY KEY, code {} UNIQUE)",
                self.parent_type
            ),
            format!(
                "CREATE TABLE a_child(id TEXT NOT NULL PRIMARY KEY, parent {} REFERENCES z_parent(code))",
                self.child_type
            ),
        ]
    }
    fn pages(&self, seed: SeedId) -> TestResult<(Vec<SeedPage>, SeedComplete)> {
        let statements = self.statements();
        relation_seed_pages(
            seed,
            &statements[0],
            &statements[1],
            vec![vec![uuid_key(1), self.child.clone()]],
            vec![vec![uuid_key(2), self.parent.clone()]],
        )
    }
    fn insert(&self) -> TestResult<Piece> {
        rows(
            2,
            vec![
                RowChange::upsert(
                    "a_child",
                    vec![uuid_key(1)],
                    vec![uuid_key(1), self.child.clone()],
                )?,
                RowChange::upsert(
                    "z_parent",
                    vec![uuid_key(2)],
                    vec![uuid_key(2), self.parent.clone()],
                )?,
            ],
        )
    }
}
fn attempt() -> TestResult<SeedAttemptId> {
    Ok(SeedAttemptId::new(
        "019f0000-0000-7000-8000-000000000022".parse()?,
    )?)
}
#[tokio::test]
async fn mixed_affinity_seed_uses_parent_comparison_before_publication() -> TestResult {
    // Export all scenarios before assertions, so the DO runs share these actual
    // protocol inputs even when the native regression is red.
    for case in cases() {
        let (pages, done) = case.pages(SeedId { generation: 1 })?;
        export(
            &format!("affinity-seed-{}.json", case.name),
            &serde_json::json!({
                "create": CreateCopy { database: database()?, epoch: Epoch::new(1)?, holder: None, origin: CopyOrigin::AwaitingSeed { expected_position: None } },
                "attempt": attempt()?, "pages": pages, "done": done, "valid": case.valid,
            }),
        )?;
    }
    for case in cases() {
        let mut connection = setup(None).await?;
        sqlx_core::query::query::<Turso>("PRAGMA foreign_keys=OFF")
            .execute(&mut connection)
            .await?;
        let authority = SeedAuthority::Admin;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let seed = seed_begin(
            &mut TursoScope(&mut transaction),
            database()?,
            attempt()?,
            &authority,
        )
        .await?;
        let (pages, done) = case.pages(seed)?;
        for page in pages {
            seed_page(&mut TursoScope(&mut transaction), &page, &authority).await?;
        }
        transaction.commit().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = seed_complete(
            &mut TursoScope(&mut transaction),
            &done,
            &NoHook,
            &authority,
        )
        .await;
        if case.valid {
            assert!(
                matches!(result?, SeedDecision::Published { .. }),
                "{}",
                case.name
            );
            transaction.commit().await?;
            assert_eq!(counts(&mut connection).await?, (1, 1, 0), "{}", case.name);
        } else {
            let refused = matches!(result, Err(ReceiverError::SeedRefused { answer: SeedAnswer::IntegrityViolation { table, references } }) if table.as_str()=="a_child" && references.as_str()=="z_parent");
            transaction.rollback().await?;
            assert!(refused, "{}: orphan published", case.name);
            let schemas: i64 = sqlx_core::query_scalar::query_scalar::<Turso, i64>(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('a_child','z_parent')",
            )
            .fetch_one(&mut connection)
            .await?;
            assert_eq!(schemas, 0, "{}: refused seed schema leaked", case.name);
        }
        connection.close().await?;
    }
    Ok(())
}
async fn counts(connection: &mut sqlx_turso::TursoConnection) -> TestResult<(i64, i64, i64)> {
    let child = sqlx_core::query_scalar::query_scalar::<Turso, i64>("SELECT COUNT(*) FROM a_child")
        .fetch_one(&mut *connection)
        .await?;
    let parent =
        sqlx_core::query_scalar::query_scalar::<Turso, i64>("SELECT COUNT(*) FROM z_parent")
            .fetch_one(&mut *connection)
            .await?;
    let receipts = sqlx_core::query_scalar::query_scalar::<Turso, i64>(
        "SELECT COUNT(*) FROM _fleet_sync_receipts",
    )
    .fetch_one(connection)
    .await?;
    Ok((child, parent, receipts))
}
#[tokio::test]
async fn mixed_affinity_apply_uses_parent_comparison_before_receipts() -> TestResult {
    for case in cases() {
        let schema = incoming(
            1,
            ReplicatedTransaction::SchemaStep {
                commit_id: CommitSequence::new(1),
                migration_version: 1,
                statements: case.statements(),
            },
        )?;
        let create = CreateCopy {
            database: database()?,
            epoch: Epoch::new(1)?,
            holder: Some(writer()?),
            origin: CopyOrigin::Empty,
        };
        export(
            &format!("affinity-apply-{}.json", case.name),
            &serde_json::json!({"create":create,"sender":writer()?,"schema":schema,"insert":case.insert()?,"valid":case.valid}),
        )?;
    }
    for case in cases() {
        let mut connection = TursoConnectOptions::new()
            .foreign_keys(false)
            .connect()
            .await?;
        RECEIVER_MIGRATOR
            .run_direct(None, &mut connection, false)
            .await?;
        create_copy(
            &mut TursoScope(&mut connection),
            &CreateCopy {
                database: database()?,
                epoch: Epoch::new(1)?,
                holder: Some(writer()?),
                origin: CopyOrigin::Empty,
            },
        )
        .await?;
        let schema = incoming(
            1,
            ReplicatedTransaction::SchemaStep {
                commit_id: CommitSequence::new(1),
                migration_version: 1,
                statements: case.statements(),
            },
        )?;
        assert!(matches!(
            apply(&mut connection, &schema).await??,
            ApplyDecision::Applied { .. }
        ));
        let before = copy_status(&mut TursoScope(&mut connection)).await?;
        let result = apply(&mut connection, &case.insert()?).await?;
        if case.valid {
            assert!(
                matches!(result?, ApplyDecision::Applied { .. }),
                "{}",
                case.name
            );
            assert_eq!(counts(&mut connection).await?, (1, 1, 2), "{}", case.name);
        } else {
            assert!(
                matches!(result, Err(ApplyFailure::Refused(PieceRefusal::IntegrityViolation {table,references})) if table.as_str()=="a_child" && references.as_str()=="z_parent"),
                "{}: orphan acknowledged",
                case.name
            );
            assert_eq!(copy_status(&mut TursoScope(&mut connection)).await?, before);
            assert_eq!(counts(&mut connection).await?, (0, 0, 1), "{}", case.name);
        }
        connection.close().await?;
    }
    Ok(())
}
#[tokio::test]
async fn mixed_affinity_snapshot_uses_parent_comparison_before_verified() -> TestResult {
    for case in cases() {
        let (pages, done) = case.pages(SeedId { generation: 1 })?;
        let install = SnapshotInstall {
            install_id: InstallId::new("019f0000-0000-7000-8000-000000000051".parse()?)?,
            database: database()?,
            position: position()?,
            schema: done.schema.clone(),
        };
        let source = CopyStatus {
            database: database()?,
            role: CopyRole::Authoritative,
            fence: Epoch::new(1)?,
            holder: Some(writer()?),
            start: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
            acked: position()?,
            schema: done.schema.clone(),
        };
        let manifest = SnapshotManifest {
            database: database()?,
            position: position()?,
            schema: done.schema,
            tables: done
                .table_digests
                .into_iter()
                .map(|(table, digest)| TableManifest {
                    table,
                    digest,
                    row_count: 1,
                })
                .collect(),
        };
        let mut connection = TursoConnectOptions::new()
            .foreign_keys(false)
            .connect()
            .await?;
        RECEIVER_MIGRATOR
            .run_direct(None, &mut connection, false)
            .await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        SnapshotInstallRules(&mut TursoScope(&mut transaction))
            .prepare(&install, &source)
            .await?;
        transaction.commit().await?;
        let mut cursor = None;
        for page in pages {
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            SnapshotInstallRules(&mut TursoScope(&mut transaction))
                .stage_page(&install, page.page, cursor.as_ref(), &page.contents)
                .await?;
            cursor = page.contents.next;
            transaction.commit().await?;
        }
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = SnapshotInstallRules(&mut TursoScope(&mut transaction))
            .verify(&install, &manifest)
            .await;
        if case.valid {
            result?;
            SnapshotInstallRules(&mut TursoScope(&mut transaction))
                .mark_ready(&install, &NoHook)
                .await?;
            transaction.commit().await?;
        } else {
            let refused = matches!(result, Err(SnapshotInstallError::Rules(ReceiverError::InvalidInput { field })) if field=="foreign key a_child references z_parent");
            transaction.rollback().await?;
            assert!(refused, "{}: orphan verified", case.name);
        }
        let state = SnapshotInstallRules(&mut TursoScope(&mut connection))
            .state()
            .await?
            .ok_or("install marker")?;
        assert_eq!(
            (state.verified, state.ready),
            (case.valid, case.valid),
            "{}",
            case.name
        );
        connection.close().await?;
    }
    Ok(())
}
