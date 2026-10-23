//! Receiver integrity, independently of the native engine's FK switch.
#![allow(clippy::panic_in_result_fn)]
use crate::turso_rule_fixture::uuid_key;
use crate::turso_seed_tests::{TursoScope, database, position, setup};
use crate::*;
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_turso::Turso;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
pub(super) struct NoHook;
impl<S: TransactionScope> ReceiverRules<S> for NoHook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(&self, _: &mut S, _: &AppliedPiece<'_, ()>) -> Result<Vec<()>, HookError> {
        Ok(Vec::new())
    }
}
pub(super) fn seeded_pages(
    seed: SeedId,
    include_parent: bool,
) -> TestResult<(Vec<SeedPage>, SeedComplete)> {
    let parent_sql = "CREATE TABLE z_parent(id TEXT NOT NULL PRIMARY KEY)";
    let child_sql =
        "CREATE TABLE a_child(id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES z_parent(id))";
    relation_seed_pages(
        seed,
        parent_sql,
        child_sql,
        vec![vec![uuid_key(1), uuid_key(2)]],
        if include_parent {
            vec![vec![uuid_key(2)]]
        } else {
            Vec::new()
        },
    )
}
pub(super) fn relation_seed_pages(
    seed: SeedId,
    parent_sql: &str,
    child_sql: &str,
    child: Vec<Vec<SqlValue>>,
    parent: Vec<Vec<SqlValue>>,
) -> TestResult<(Vec<SeedPage>, SeedComplete)> {
    let statements = vec![parent_sql.into(), child_sql.into()];
    let schema = SchemaIdentity::new(
        1,
        schema_fingerprint(&[
            SchemaEntry {
                kind: SchemaObjectKind::Table,
                name: "z_parent".into(),
                sql: parent_sql.into(),
            },
            SchemaEntry {
                kind: SchemaObjectKind::Table,
                name: "a_child".into(),
                sql: child_sql.into(),
            },
        ])?,
    )?;
    let mut pages = Vec::new();
    let mut digests = Vec::new();
    for (index, (table, rows)) in [("a_child", child), ("z_parent", parent)]
        .into_iter()
        .enumerate()
    {
        let mut digest = TableDigestBuilder::new();
        for row in &rows {
            digest.push_row(row)?;
        }
        digests.push((TableName::new(table)?, digest.finish()));
        let contents = SnapshotPage {
            database: database()?,
            position: position()?,
            schema: schema.clone(),
            schema_steps: statements.clone(),
            table: table.into(),
            rows,
            next: if index == 0 {
                Some(SnapshotCursor {
                    position: position()?,
                    table: "z_parent".into(),
                    after_key: Vec::new(),
                })
            } else {
                None
            },
        };
        pages.push(SeedPage {
            seed,
            page: u32::try_from(index)?,
            digest: PageDigest::of(&contents)?,
            contents,
        });
    }
    Ok((
        pages,
        SeedComplete {
            database: database()?,
            seed,
            position: position()?,
            pages: 2,
            schema,
            table_digests: digests,
        },
    ))
}
#[tokio::test]
async fn orphan_seed_refuses_before_publication_with_native_engine_fks_disabled() -> TestResult {
    let mut connection = setup(None).await?;
    sqlx_core::query::query::<Turso>("PRAGMA foreign_keys=OFF")
        .execute(&mut connection)
        .await?;
    let authority = SeedAuthority::Admin;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    let attempt = SeedAttemptId::new("019f0000-0000-7000-8000-000000000022".parse()?)?;
    let seed = seed_begin(&mut scope, database()?, attempt, &authority).await?;
    let (pages, done) = seeded_pages(seed, false)?;
    for page in &pages {
        assert_eq!(
            seed_page(&mut scope, page, &authority).await?,
            SeedPageAnswer::Staged
        );
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
    let refused = matches!(&result, Err(ReceiverError::SeedRefused {answer: SeedAnswer::IntegrityViolation {table, references}}) if table.as_str() == "a_child" && references.as_str() == "z_parent");
    transaction.rollback().await?;
    assert!(
        refused,
        "receiver published an orphan seed while native engine FK enforcement was disabled"
    );
    Ok(())
}

pub(super) fn export(name: &str, value: &impl serde::Serialize) -> TestResult {
    if let Some(root) = std::env::var_os("FK_FIXTURE_ROOT") {
        let root = std::path::PathBuf::from(root);
        std::fs::create_dir_all(&root)?;
        std::fs::write(root.join(name), serde_json::to_vec_pretty(value)?)?;
    }
    Ok(())
}
#[tokio::test]
async fn child_page_before_parent_publishes_with_exact_rows() -> TestResult {
    for include_parent in [false, true] {
        let mut connection = setup(None).await?;
        sqlx_core::query::query::<Turso>("PRAGMA foreign_keys=OFF")
            .execute(&mut connection)
            .await?;
        let authority = SeedAuthority::Admin;
        let attempt = SeedAttemptId::new("019f0000-0000-7000-8000-000000000022".parse()?)?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut scope = TursoScope(&mut transaction);
        let seed = seed_begin(&mut scope, database()?, attempt, &authority).await?;
        let (pages, done) = seeded_pages(seed, include_parent)?;
        export(
            if include_parent {
                "seed-valid.json"
            } else {
                "seed-orphan.json"
            },
            &serde_json::json!({
                "create": CreateCopy {database: database()?, epoch: Epoch::new(1)?, holder: None, origin: CopyOrigin::AwaitingSeed {expected_position: None}},
                "attempt": attempt, "pages": pages, "done": done,
            }),
        )?;
        for page in &pages {
            assert_eq!(
                seed_page(&mut scope, page, &authority).await?,
                SeedPageAnswer::Staged
            );
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
        if include_parent {
            assert!(
                matches!(result?,SeedDecision::Published {position: at,..} if at == position()?)
            );
            transaction.commit().await?;
            let child: i64 =
                sqlx_core::query_scalar::query_scalar::<Turso, i64>("SELECT COUNT(*) FROM a_child")
                    .fetch_one(&mut connection)
                    .await?;
            let parent: i64 = sqlx_core::query_scalar::query_scalar::<Turso, i64>(
                "SELECT COUNT(*) FROM z_parent",
            )
            .fetch_one(&mut connection)
            .await?;
            assert_eq!((child, parent), (1, 1));
        } else {
            assert!(matches!(
                result,
                Err(ReceiverError::SeedRefused {
                    answer: SeedAnswer::IntegrityViolation { .. }
                })
            ));
            transaction.rollback().await?;
            let count: i64 = sqlx_core::query_scalar::query_scalar::<Turso, i64>(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name='a_child'",
            )
            .fetch_one(&mut connection)
            .await?;
            assert_eq!(
                count, 0,
                "refused publication must roll back rows and schema"
            );
        }
        connection.close().await?;
    }
    Ok(())
}
fn at(sequence: u64) -> TestResult<ReplicationPosition> {
    Ok(ReplicationPosition::new(
        Epoch::new(1)?,
        CommitSequence::new(sequence),
    ))
}
pub(super) fn incoming(sequence: u64, transaction: ReplicatedTransaction) -> TestResult<Piece> {
    Ok(Piece::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: at(sequence - 1)?,
        through: at(sequence)?,
        transactions: vec![transaction],
        part: None,
        annotation: None,
    })?)
}
pub(super) fn rows(sequence: u64, changes: Vec<RowChange>) -> TestResult<Piece> {
    incoming(
        sequence,
        ReplicatedTransaction::Rows {
            commit_id: CommitSequence::new(sequence),
            rows: changes,
        },
    )
}
fn changes(include_parent: bool) -> TestResult<Vec<RowChange>> {
    let mut rows = vec![RowChange::upsert(
        "a_child",
        vec![uuid_key(1)],
        vec![uuid_key(1), uuid_key(2)],
    )?];
    if include_parent {
        rows.push(RowChange::upsert(
            "z_parent",
            vec![uuid_key(2)],
            vec![uuid_key(2)],
        )?);
    }
    Ok(rows)
}
pub(super) async fn apply(
    connection: &mut sqlx_turso::TursoConnection,
    piece: &Piece,
) -> TestResult<Result<ApplyDecision<()>, ApplyFailure>> {
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let result = apply_piece(
        &mut TursoScope(&mut transaction),
        piece,
        &crate::turso_rule_fixture::writer()?,
        &NoHook,
        SyncTime::from_unix_millis(0),
    )
    .await;
    if result.is_ok() {
        transaction.commit().await?;
    } else {
        transaction.rollback().await?;
    }
    Ok(result)
}
#[tokio::test]
async fn complete_and_split_pieces_validate_final_relationships_before_receipts() -> TestResult {
    for action in ["NO ACTION", "RESTRICT"] {
        for split in [false, true] {
            let mut connection = sqlx_turso::TursoConnectOptions::new()
                .foreign_keys(false)
                .connect()
                .await?;
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            RECEIVER_MIGRATOR
                .run_direct(None, &mut *transaction, false)
                .await?;
            let create = CreateCopy {
                database: database()?,
                epoch: Epoch::new(1)?,
                holder: Some(crate::turso_rule_fixture::writer()?),
                origin: CopyOrigin::Empty,
            };
            create_copy(&mut TursoScope(&mut transaction), &create).await?;
            transaction.commit().await?;
            let statements = vec![
                "CREATE TABLE z_parent(id TEXT NOT NULL PRIMARY KEY)".into(),
                format!(
                    "CREATE TABLE a_child(id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES z_parent(id) ON DELETE {action})"
                ),
            ];
            let schema = incoming(
                1,
                ReplicatedTransaction::SchemaStep {
                    commit_id: CommitSequence::new(1),
                    migration_version: 1,
                    statements,
                },
            )?;
            assert!(matches!(
                apply(&mut connection, &schema).await??,
                ApplyDecision::Applied { .. }
            ));
            let insert = rows(2, changes(true)?)?;
            let mut inserts = vec![insert.clone()];
            if split {
                let whole = insert.transactions.first().ok_or("transaction")?;
                let ReplicatedTransaction::Rows { rows: changes, .. } = whole else {
                    return Err("row transaction".into());
                };
                inserts = changes
                    .iter()
                    .enumerate()
                    .map(|(index, row)| {
                        let mut contents = PieceContents {
                            database: insert.database,
                            epoch: insert.epoch,
                            continues_from: insert.continues_from,
                            through: insert.through,
                            transactions: Vec::new(),
                            part: None,
                            annotation: None,
                        };
                        contents.transactions = vec![ReplicatedTransaction::Rows {
                            commit_id: CommitSequence::new(2),
                            rows: vec![row.clone()],
                        }];
                        contents.part = Some(TransactionPart::new(
                            TransactionKey {
                                epoch: Epoch::new(1)?,
                                commit_id: CommitSequence::new(2),
                            },
                            u32::try_from(index)?,
                            2,
                            PieceDigest::of_transaction(whole)?,
                        )?);
                        Ok(Piece::new(contents)?)
                    })
                    .collect::<TestResult<Vec<_>>>()?;
            }
            for (index, piece) in inserts.iter().enumerate() {
                let answer = apply(&mut connection, piece).await??;
                if index + 1 < inserts.len() {
                    assert!(matches!(answer, ApplyDecision::PartStaged { .. }));
                } else {
                    assert!(matches!(answer, ApplyDecision::Applied { .. }));
                }
            }
            let before = copy_status(&mut TursoScope(&mut connection)).await?;
            let orphan = rows(
                3,
                vec![RowChange::upsert(
                    "a_child",
                    vec![uuid_key(1)],
                    vec![uuid_key(1), uuid_key(99)],
                )?],
            )?;
            assert!(
                matches!(apply(&mut connection,&orphan).await?,Err(ApplyFailure::Refused(PieceRefusal::IntegrityViolation {table,references})) if table.as_str()=="a_child" && references.as_str()=="z_parent")
            );
            assert_eq!(copy_status(&mut TursoScope(&mut connection)).await?, before);
            let delete = rows(
                3,
                vec![
                    RowChange::Delete {
                        table: "z_parent".into(),
                        key: vec![uuid_key(2)],
                    },
                    RowChange::Delete {
                        table: "a_child".into(),
                        key: vec![uuid_key(1)],
                    },
                ],
            )?;
            assert!(matches!(
                apply(&mut connection, &delete).await??,
                ApplyDecision::Applied { .. }
            ));
            export(
                &format!("apply-{}-{}.json", action.replace(' ', "-"), split),
                &serde_json::json!({"create":create,"sender":crate::turso_rule_fixture::writer()?,"schema":schema,"insert":inserts,"orphan":orphan,"delete":delete}),
            )?;
            connection.close().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn self_reference_and_nullable_composite_fks_use_final_match_simple_semantics() -> TestResult
{
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .foreign_keys(false)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    RECEIVER_MIGRATOR
        .run_direct(None, &mut *transaction, false)
        .await?;
    let create = CreateCopy {
        database: database()?,
        epoch: Epoch::new(1)?,
        holder: Some(crate::turso_rule_fixture::writer()?),
        origin: CopyOrigin::Empty,
    };
    create_copy(&mut TursoScope(&mut transaction), &create).await?;
    transaction.commit().await?;
    let schema=incoming(1,ReplicatedTransaction::SchemaStep {commit_id:CommitSequence::new(1),migration_version:1,statements:vec![
        "CREATE TABLE z_composite(first TEXT NOT NULL, second TEXT NOT NULL, PRIMARY KEY(first,second))".into(),
        "CREATE TABLE a_composite(id TEXT NOT NULL PRIMARY KEY, first TEXT, second TEXT, FOREIGN KEY(first,second) REFERENCES z_composite)".into(),
        "CREATE TABLE self_nodes(id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES self_nodes(id))".into(),
    ]})?;
    assert!(matches!(
        apply(&mut connection, &schema).await??,
        ApplyDecision::Applied { .. }
    ));
    let insert = rows(
        2,
        vec![
            RowChange::upsert(
                "a_composite",
                vec![uuid_key(3)],
                vec![uuid_key(3), uuid_key(4), uuid_key(5)],
            )?,
            RowChange::upsert(
                "a_composite",
                vec![uuid_key(6)],
                vec![uuid_key(6), uuid_key(99), SqlValue::Null],
            )?,
            RowChange::upsert(
                "self_nodes",
                vec![uuid_key(1)],
                vec![uuid_key(1), uuid_key(2)],
            )?,
            RowChange::upsert(
                "self_nodes",
                vec![uuid_key(2)],
                vec![uuid_key(2), SqlValue::Null],
            )?,
            RowChange::upsert(
                "z_composite",
                vec![uuid_key(4), uuid_key(5)],
                vec![uuid_key(4), uuid_key(5)],
            )?,
        ],
    )?;
    assert!(matches!(
        apply(&mut connection, &insert).await??,
        ApplyDecision::Applied { .. }
    ));
    let before = copy_status(&mut TursoScope(&mut connection)).await?;
    let orphan = rows(
        3,
        vec![RowChange::upsert(
            "a_composite",
            vec![uuid_key(6)],
            vec![uuid_key(6), uuid_key(99), uuid_key(98)],
        )?],
    )?;
    assert!(
        matches!(apply(&mut connection,&orphan).await?,Err(ApplyFailure::Refused(PieceRefusal::IntegrityViolation {table,references})) if table.as_str()=="a_composite" && references.as_str()=="z_composite")
    );
    assert_eq!(copy_status(&mut TursoScope(&mut connection)).await?, before);
    export(
        "composite-self.json",
        &serde_json::json!({"create":create,"sender":crate::turso_rule_fixture::writer()?,"schema":schema,"insert":insert,"orphan":orphan}),
    )?;
    connection.close().await?;
    Ok(())
}
