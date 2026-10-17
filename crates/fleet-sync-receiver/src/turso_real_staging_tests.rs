//! Real values survive both JSON staging paths, file reopen, and canonical digest verification.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use fleet_sync_protocol::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};

const REAL_BITS: [u64; 4] = [
    3_485_510_186_621_062_260,
    f64::INFINITY.to_bits(),
    f64::NEG_INFINITY.to_bits(),
    f64::MIN_POSITIVE.to_bits(),
];

async fn assert_values(connection: &mut TursoConnection) -> TestResult {
    let rows = sqlx_core::query::query::<Turso>(
        "SELECT value,typeof(value) AS kind FROM real_rows ORDER BY id",
    )
    .fetch_all(connection)
    .await?;
    assert_eq!(rows.len(), REAL_BITS.len());
    for (row, expected) in rows.iter().zip(REAL_BITS) {
        assert_eq!(row.try_get::<String, _>("kind")?, "real");
        assert_eq!(row.try_get::<f64, _>("value")?.to_bits(), expected);
    }
    Ok(())
}

#[tokio::test]
async fn split_piece_and_seed_json_keep_sensitive_finite_bits_and_infinities_after_reopen()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.db");
    let mut source = connection_with_options(
        TursoConnectOptions::new()
            .filename(&source_path)
            .create_if_missing(true),
    )
    .await?;
    let schema = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::SchemaStep {
            commit_id: position(1)?.sequence,
            migration_version: 2,
            statements: vec![
                "CREATE TABLE real_rows(id TEXT NOT NULL PRIMARY KEY,value REAL)".into(),
            ],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = source.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &schema,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(1),
    )
    .await?;
    transaction.commit().await?;
    let rows = REAL_BITS
        .iter()
        .enumerate()
        .map(|(index, bits)| {
            let id = i64::try_from(index).map_err(|error| error.to_string())?;
            RowChange::upsert(
                "real_rows",
                vec![crate::turso_rule_fixture::uuid_key(id)],
                vec![
                    crate::turso_rule_fixture::uuid_key(id),
                    SqlValue::Real(f64::from_bits(*bits)),
                ],
            )
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, String>>()?;
    let whole = ReplicatedTransaction::Rows {
        commit_id: position(2)?.sequence,
        rows: rows.clone(),
    };
    let digest = PieceDigest::of_transaction(&whole)?;
    let key = TransactionKey {
        epoch: Epoch::new(1)?,
        commit_id: position(2)?.sequence,
    };
    let mut parts = Vec::new();
    for (index, rows) in rows.chunks(2).enumerate() {
        parts.push(Piece::<()>::new(PieceContents {
            database: database()?,
            epoch: Epoch::new(1)?,
            continues_from: position(1)?,
            through: position(2)?,
            transactions: vec![ReplicatedTransaction::Rows {
                commit_id: position(2)?.sequence,
                rows: rows.to_vec(),
            }],
            part: Some(TransactionPart::new(
                key,
                u32::try_from(index)?,
                2,
                digest.clone(),
            )?),
            annotation: None,
        })?);
    }
    let mut transaction = source.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            parts.first().ok_or("first part")?,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(2)
        )
        .await?,
        ApplyDecision::PartStaged { .. }
    ));
    transaction.commit().await?;
    drop(source);
    let mut source = TursoConnectOptions::new()
        .filename(&source_path)
        .connect()
        .await?;
    let mut transaction = source.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        apply_piece(
            &mut TursoScope(&mut transaction),
            parts.last().ok_or("last part")?,
            &writer()?,
            &Hook { refuse: false },
            SyncTime::from_unix_millis(3)
        )
        .await?,
        ApplyDecision::Applied { .. }
    ));
    transaction.commit().await?;
    assert_values(&mut source).await?;
    let mut transaction = source.begin_with("BEGIN IMMEDIATE").await?;
    let SnapshotManifestAnswer::Manifest(manifest) =
        snapshot_manifest(&mut TursoScope(&mut transaction), position(2)?).await?
    else {
        return Err("missing manifest".into());
    };
    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let SnapshotRead::Page(page) =
            snapshot_page(&mut TursoScope(&mut transaction), cursor).await?
        else {
            return Err("moved snapshot".into());
        };
        cursor = page.next.clone();
        pages.push(*page);
        if cursor.is_none() {
            break;
        }
    }
    transaction.commit().await?;
    let target_path = directory.path().join("target.db");
    let mut target = TursoConnectOptions::new()
        .filename(&target_path)
        .create_if_missing(true)
        .connect()
        .await?;
    let mut transaction = target.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_core::raw_sql::raw_sql(concat!(
        include_str!("../migrations/0001_copy_rules.sql"),
        "\n",
        include_str!("../migrations/0002_replicated_schema.sql"),
        "\n",
        include_str!("../migrations/0003_snapshot_install.sql")
    ))
    .execute(&mut *transaction)
    .await?;
    initialize_hook(&mut TursoScope(&mut transaction), &Hook { refuse: false }).await?;
    create_copy(
        &mut TursoScope(&mut transaction),
        &CreateCopy {
            database: database()?,
            epoch: Epoch::new(1)?,
            holder: None,
            origin: CopyOrigin::AwaitingSeed {
                expected_position: Some(position(2)?),
            },
        },
    )
    .await?;
    let seed = seed_begin(
        &mut TursoScope(&mut transaction),
        database()?,
        uuid::Uuid::from_u128(1500),
        &crate::SeedAuthority::Admin,
    )
    .await?;
    for (index, contents) in pages.iter().enumerate() {
        let page = SeedPage {
            seed,
            page: u32::try_from(index)?,
            contents: contents.clone(),
            digest: PageDigest::of(contents)?,
        };
        assert_eq!(
            seed_page(
                &mut TursoScope(&mut transaction),
                &page,
                &crate::SeedAuthority::Admin
            )
            .await?,
            SeedPageAnswer::Staged
        );
    }
    transaction.commit().await?;
    drop(target);
    let mut target = TursoConnectOptions::new()
        .filename(&target_path)
        .connect()
        .await?;
    let complete = SeedComplete {
        database: database()?,
        seed,
        position: position(2)?,
        pages: u32::try_from(pages.len())?,
        schema: manifest.schema,
        table_digests: manifest
            .tables
            .into_iter()
            .map(|table| (table.table, table.digest))
            .collect(),
    };
    let mut transaction = target.begin_with("BEGIN IMMEDIATE").await?;
    assert!(
        matches!(seed_complete(&mut TursoScope(&mut transaction), &complete, &Hook { refuse: false }, &crate::SeedAuthority::Admin).await?, SeedDecision::Published { position: actual, follow_ups } if actual == position(2)? && follow_ups.is_empty())
    );
    transaction.commit().await?;
    assert_values(&mut target).await?;
    Ok(())
}
