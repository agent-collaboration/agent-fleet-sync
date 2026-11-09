//! Retained-record inspection over records the real receiver wrote, then one
//! stored value damaged per case, on the owned Turso driver.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_rule_fixture::{self as fixture, TestResult, TursoScope},
    *,
};
use fleet_sync_protocol::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use uuid::Uuid;

async fn inspect(
    connection: &mut TursoConnection,
) -> Result<RetainedRecords, RetainedRecordRefusal> {
    inspect_retained_records::<_, ()>(&mut TursoScope(connection)).await
}

async fn damage(connection: &mut TursoConnection, sql: &str) -> TestResult {
    let changed = sqlx_core::raw_sql::raw_sql(sqlx_core::sql_str::AssertSqlSafe(sql.to_owned()))
        .execute(&mut *connection)
        .await?
        .rows_affected();
    assert_eq!(changed, 1, "the damage must rewrite exactly one row: {sql}");
    Ok(())
}

async fn refused(
    connection: &mut TursoConnection,
    family: RetainedFamily,
) -> TestResult<RetainedRecordRefusal> {
    match inspect(connection).await {
        Err(refusal) if refusal.family == family => Ok(refusal),
        answer => Err(format!("expected a {family:?} refusal, got {answer:?}").into()),
    }
}

/// Every receiver record, so a test can show inspection changed nothing.
async fn receiver_rows(connection: &mut TursoConnection) -> TestResult<String> {
    let mut rows = Vec::new();
    for table in [
        "_fleet_sync_copy",
        "_fleet_sync_creation",
        "_fleet_sync_fence_receipts",
        "_fleet_sync_seed",
        "_fleet_sync_seed_pages",
        "_fleet_sync_seed_attempts",
        "_fleet_sync_install",
        "_fleet_sync_install_pages",
        "_fleet_sync_staged_parts",
    ] {
        let columns = sqlx_core::query::query::<Turso>(
            "SELECT CAST(name AS TEXT) AS name FROM pragma_table_info(?1) ORDER BY cid",
        )
        .bind(table)
        .fetch_all(&mut *connection)
        .await?
        .iter()
        .map(|column| column.try_get::<String, _>("name"))
        .collect::<Result<Vec<_>, _>>()?;
        let quoted = columns
            .iter()
            .map(|column| format!("quote(\"{column}\")"))
            .collect::<Vec<_>>()
            .join("||','||");
        let sql = format!(
            "SELECT group_concat(line, '|') AS dump FROM (SELECT {quoted} AS line FROM {table} ORDER BY rowid)"
        );
        let dump: Option<String> =
            sqlx_core::query::query::<Turso>(sqlx_core::sql_str::AssertSqlSafe(sql))
                .fetch_one(&mut *connection)
                .await?
                .try_get("dump")?;
        rows.push(format!("{table}={}", dump.unwrap_or_default()));
    }
    Ok(rows.join("\n"))
}

fn fence_request(
    transfer: u128,
    new_epoch: u64,
    acked: ReplicationPosition,
    holder: u128,
) -> TestResult<FenceRequest> {
    Ok(FenceRequest {
        database: fixture::database()?,
        transfer_id: TransferId::new(Uuid::from_u128(
            0x019eef00000070008000000000000000 | transfer,
        ))?,
        new_epoch: Epoch::new(new_epoch)?,
        expect: FenceExpectation {
            epoch: Epoch::new(new_epoch - 1)?,
            acked,
        },
        target_schema: Some(SchemaSupport { max_version: 1 }),
        holder: PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000000 | holder))?,
    })
}

/// An authoritative Copy with its creation request and two fence receipts:
/// A ends epoch 1 and grants 2, then B ends 2 and grants 3.
async fn fenced_copy() -> TestResult<(TursoConnection, FenceRequest, FenceRequest)> {
    let mut connection = fixture::connection().await?;
    let first = fence_request(0x701, 2, fixture::position(0)?, 0x702)?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        fence(&mut TursoScope(&mut transaction), &first).await?,
        FenceResult::Fenced { .. }
    ));
    let status = fixture::ready_status(copy_status(&mut TursoScope(&mut transaction)).await?)?;
    let second = fence_request(0x703, 3, status.acked, 0x704)?;
    assert!(matches!(
        fence(&mut TursoScope(&mut transaction), &second).await?,
        FenceResult::Fenced { .. }
    ));
    transaction.commit().await?;
    Ok((connection, first, second))
}

#[tokio::test]
async fn creation_and_fence_receipts_decode_and_a_damaged_one_is_refused() -> TestResult {
    let (mut connection, first, second) = fenced_copy().await?;
    let first_row = serde_json::to_string(&first.transfer_id)?;
    let second_row = serde_json::to_string(&second.transfer_id)?;
    let before = receiver_rows(&mut connection).await?;
    assert_eq!(
        inspect(&mut connection).await?,
        RetainedRecords {
            creation: 1,
            fence_receipts: 2,
            ..RetainedRecords::default()
        }
    );
    assert_eq!(
        receiver_rows(&mut connection).await?,
        before,
        "inspection must not write"
    );

    // Each damages row A only. Aliased: A holds B's request, which B's own
    // address would accept, so only reading A at its own address refuses it.
    // Result: A's result no longer a successful fence. Transfer: A filed under
    // another transfer, so the refusal names it at that address. Epoch: A
    // filed under another ended epoch.
    let other_transfer = serde_json::to_string(&TransferId::new(Uuid::from_u128(
        0x019eef000000700080000000000007ff,
    ))?)?;
    for (name, set, refused_row) in [
        (
            "aliased",
            format!(
                "request = (SELECT request FROM _fleet_sync_fence_receipts WHERE transfer = '{second_row}')"
            ),
            &first_row,
        ),
        (
            "result",
            r#"result = '{"kind":"fenceNotAdvanced","currentEpoch":1}'"#.to_owned(),
            &first_row,
        ),
        (
            "transfer",
            format!("transfer = '{other_transfer}'"),
            &other_transfer,
        ),
        ("epoch", "old_epoch = '7'".to_owned(), &first_row),
    ] {
        let (mut connection, ..) = fenced_copy().await?;
        damage(
            &mut connection,
            &format!("UPDATE _fleet_sync_fence_receipts SET {set} WHERE transfer = '{first_row}'"),
        )
        .await?;
        let refusal = refused(&mut connection, RetainedFamily::FenceReceipts)
            .await
            .map_err(|error| format!("{name}: {error}"))?;
        assert_eq!(&refusal.row, refused_row, "{name}");
    }

    let (mut connection, ..) = fenced_copy().await?;
    damage(
        &mut connection,
        "UPDATE _fleet_sync_creation SET request = '{}'",
    )
    .await?;
    refused(&mut connection, RetainedFamily::Creation).await?;
    Ok(())
}

/// A seeding Copy with one seed generation, its staged page and its attempt.
async fn seeded_copy() -> TestResult<(TursoConnection, SeedAttemptId)> {
    let mut connection = crate::turso_seed_tests::setup(None).await?;
    let attempt = SeedAttemptId::new(Uuid::from_u128(0x019eef00000070008000000000000711))?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let seed = seed_begin(
        &mut TursoScope(&mut transaction),
        crate::turso_seed_tests::database()?,
        attempt,
        &SeedAuthority::Admin,
    )
    .await?;
    let (page, _) = crate::turso_seed_tests::snapshot(seed)?;
    assert_eq!(
        seed_page(
            &mut TursoScope(&mut transaction),
            &page,
            &SeedAuthority::Admin
        )
        .await?,
        SeedPageAnswer::Staged
    );
    transaction.commit().await?;
    Ok((connection, attempt))
}

#[tokio::test]
async fn seed_record_pages_and_attempts_decode_and_a_damaged_one_is_refused() -> TestResult {
    let (mut connection, attempt) = seeded_copy().await?;
    let before = receiver_rows(&mut connection).await?;
    assert_eq!(
        inspect(&mut connection).await?,
        RetainedRecords {
            creation: 1,
            seed: 1,
            seed_pages: 1,
            seed_attempts: 1,
            ..RetainedRecords::default()
        }
    );
    assert_eq!(receiver_rows(&mut connection).await?, before);

    damage(&mut connection, "UPDATE _fleet_sync_seed SET record = '{}'").await?;
    refused(&mut connection, RetainedFamily::Seed).await?;

    let (mut connection, _) = seeded_copy().await?;
    damage(
        &mut connection,
        "UPDATE _fleet_sync_seed_pages SET digest = 'sha256:0000000000000000000000000000000000000000000000000000000000000000'",
    )
    .await?;
    let refusal = refused(&mut connection, RetainedFamily::SeedPages).await?;
    assert_eq!(refusal.row, "generation 1");

    let (mut connection, _) = seeded_copy().await?;
    damage(
        &mut connection,
        r#"UPDATE _fleet_sync_seed_attempts SET record = '{"generation":"one"}'"#,
    )
    .await?;
    let refusal = refused(&mut connection, RetainedFamily::SeedAttempts).await?;
    assert_eq!(refusal.row, attempt.to_string());

    let (mut connection, _) = seeded_copy().await?;
    damage(
        &mut connection,
        "UPDATE _fleet_sync_seed_attempts SET attempt = 'not-a-seed-attempt'",
    )
    .await?;
    let refusal = refused(&mut connection, RetainedFamily::SeedAttempts).await?;
    assert_eq!(refusal.row, "not-a-seed-attempt");
    Ok(())
}

/// A follower Copy mid-install: its marker, one verified page receipt, and the
/// carried fence set a snapshot install reads.
async fn installing_copy() -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new()
        .foreign_keys(false)
        .connect()
        .await?;
    RECEIVER_MIGRATOR
        .run_direct(None, &mut connection, false)
        .await?;
    let (install, status, page, manifest) = crate::turso_snapshot_install_tests::source()?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = TursoScope(&mut transaction);
    let mut rules = SnapshotInstallRules(&mut scope);
    rules.prepare(&install, &status).await?;
    rules.stage_page(&install, 0, None, &page).await?;
    rules.verify(&install, &manifest).await?;
    transaction.commit().await?;
    Ok(connection)
}

#[tokio::test]
async fn install_marker_and_page_receipts_decode_and_a_damaged_one_is_refused() -> TestResult {
    let mut connection = installing_copy().await?;
    let before = receiver_rows(&mut connection).await?;
    assert_eq!(
        inspect(&mut connection).await?,
        RetainedRecords {
            install: 1,
            install_pages: 1,
            ..RetainedRecords::default()
        }
    );
    assert_eq!(receiver_rows(&mut connection).await?, before);

    damage(
        &mut connection,
        "UPDATE _fleet_sync_install SET record = '{}'",
    )
    .await?;
    refused(&mut connection, RetainedFamily::Install).await?;

    let mut connection = installing_copy().await?;
    damage(
        &mut connection,
        "UPDATE _fleet_sync_install_pages SET record = json_set(record, '$.page', 1)",
    )
    .await?;
    let refusal = refused(&mut connection, RetainedFamily::InstallPages).await?;
    let (install, ..) = crate::turso_snapshot_install_tests::source()?;
    assert_eq!(refusal.row, install.install_id.to_string());
    Ok(())
}

fn row(identity: i64) -> TestResult<RowChange> {
    Ok(RowChange::upsert(
        "records",
        vec![fixture::uuid_key(identity)],
        vec![
            fixture::uuid_key(identity),
            SqlValue::Text(format!("row {identity}")),
        ],
    )?)
}

/// Part `index` of a three-part transaction committed at `commit`.
fn part(commit: u64, index: u32, identity: i64) -> TestResult<Piece> {
    let whole = ReplicatedTransaction::Rows {
        commit_id: fixture::position(commit)?.sequence,
        rows: vec![row(1)?, row(2)?, row(3)?],
    };
    Ok(Piece::new(PieceContents {
        database: fixture::database()?,
        epoch: Epoch::new(1)?,
        continues_from: fixture::position(0)?,
        through: fixture::position(commit)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: whole.commit_id(),
            rows: vec![row(identity)?],
        }],
        part: Some(TransactionPart::new(
            TransactionKey {
                epoch: Epoch::new(1)?,
                commit_id: whole.commit_id(),
            },
            index,
            3,
            PieceDigest::of_transaction(&whole)?,
        )?),
        annotation: None,
    })?)
}

/// A following Copy with two of three parts staged through apply.
async fn staged_copy() -> TestResult<(TursoConnection, String)> {
    let mut connection = fixture::connection().await?;
    for index in 0..2 {
        let piece = part(1, index, i64::from(index) + 1)?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        assert!(matches!(
            apply_piece(
                &mut TursoScope(&mut transaction),
                &piece,
                &fixture::writer()?,
                &fixture::Hook { refuse: false },
                SyncTime::from_unix_millis(100)
            )
            .await?,
            ApplyDecision::PartStaged { .. }
        ));
        transaction.commit().await?;
    }
    let key = TransactionKey {
        epoch: Epoch::new(1)?,
        commit_id: fixture::position(1)?.sequence,
    };
    Ok((connection, serde_json::to_string(&key)?))
}

#[tokio::test]
async fn staged_parts_decode_as_apply_reads_them_and_each_damage_is_refused() -> TestResult {
    let (mut connection, key) = staged_copy().await?;
    let before = receiver_rows(&mut connection).await?;
    assert_eq!(
        inspect(&mut connection).await?,
        RetainedRecords {
            creation: 1,
            staged_parts: 2,
            ..RetainedRecords::default()
        }
    );
    assert_eq!(receiver_rows(&mut connection).await?, before);

    // A transaction's other part, freshly hashed, so only the transaction disagrees.
    let other_transaction = serde_json::to_string(&part(2, 0, 1)?)?;
    let cases = [
        (
            "malformed payload",
            "UPDATE _fleet_sync_staged_parts SET payload = '{}' WHERE part_index = 0".to_owned(),
        ),
        (
            "index mismatch",
            "UPDATE _fleet_sync_staged_parts SET part_index = 2 WHERE part_index = 1".to_owned(),
        ),
        (
            "transaction mismatch",
            format!(
                "UPDATE _fleet_sync_staged_parts SET payload = '{}' WHERE part_index = 0",
                other_transaction.replace('\'', "''")
            ),
        ),
        (
            "digest mismatch",
            "UPDATE _fleet_sync_staged_parts SET payload = replace(payload, 'row 1', 'row 9') WHERE part_index = 0".to_owned(),
        ),
    ];
    for (name, sql) in cases {
        let (mut connection, _) = staged_copy().await?;
        damage(&mut connection, &sql).await?;
        let refusal = refused(&mut connection, RetainedFamily::StagedParts)
            .await
            .map_err(|error| format!("{name}: {error}"))?;
        assert_eq!(refusal.row, key, "{name}");
        assert!(refusal.detail.contains("staged"), "{name}: {refusal}");
    }
    Ok(())
}

#[tokio::test]
async fn a_retired_copy_retains_no_receiver_records() -> TestResult {
    let (mut connection, _) = staged_copy().await?;
    let CopyStatusAnswer::Status(status) = copy_status(&mut TursoScope(&mut connection)).await?
    else {
        return Err("copy is not following".into());
    };
    retire_copy(
        &mut TursoScope(&mut connection),
        &RetireCopy {
            database: status.database,
            expect_fence: status.fence,
            expect_acked: status.acked,
        },
        SyncTime::from_unix_millis(42),
    )
    .await?;
    assert_eq!(inspect(&mut connection).await?, RetainedRecords::default());
    Ok(())
}
