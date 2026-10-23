//! Complete native copy-page units, deterministic continuation and row digests.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use fleet_sync_protocol::*;
use sqlx_core::connection::Connection;

fn assert_complete_units_fit(page: &SnapshotPage, limit: usize) -> TestResult {
    let frame = FrameId::new(durable_steps::Uuid::from_u128(1))?;
    let seed = SeedPage {
        seed: SeedId {
            generation: u64::MAX,
        },
        page: u32::MAX,
        contents: page.clone(),
        digest: PageDigest::of(page)?,
    };
    let request = ClientFrame::<NoConsumer>::Request {
        frame,
        body: OperationRequest::Replication(ReplicationRequest::SeedPage(Box::new(seed.clone()))),
    };
    let answer = ServerFrame::<NoConsumer>::Answer {
        frame,
        body: OperationAnswer::Replication(ReplicationAnswer::SnapshotPage(SnapshotRead::Page(
            Box::new(page.clone()),
        ))),
    };
    for (kind, encoded) in [
        ("page", serde_json::to_vec(page)?),
        ("seed", serde_json::to_vec(&seed)?),
        ("request frame", serde_json::to_vec(&request)?),
        ("answer frame", serde_json::to_vec(&answer)?),
    ] {
        assert!(
            encoded.len() <= limit,
            "{kind} encoded to {} bytes",
            encoded.len()
        );
    }
    Ok(())
}

#[tokio::test]
async fn byte_short_copy_pages_follow_last_emitted_key_and_replay_identically() -> TestResult {
    let mut connection = connection().await?;
    let expected = (0..48)
        .map(|index| {
            vec![
                uuid_key(index),
                SqlValue::Text(format!(
                    "row {index}:{}{}",
                    "\"".repeat(24 * 1024),
                    "\\".repeat(8 * 1024)
                )),
            ]
        })
        .collect::<Vec<_>>();
    let rows = expected
        .iter()
        .map(|row| -> TestResult<RowChange> {
            Ok(RowChange::upsert(
                "records",
                vec![row.first().ok_or("missing key")?.clone()],
                row.clone(),
            )?)
        })
        .collect::<TestResult<Vec<_>>>()?;
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: position(1)?.sequence,
            rows,
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(1),
    )
    .await?;
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut cursor = None;
    let mut received = Vec::new();
    let mut byte_short = false;
    let mut pages = 0;
    let started = std::time::Instant::now();
    loop {
        let replay_cursor = cursor.clone();
        let SnapshotRead::Page(page) =
            snapshot_page(&mut TursoScope(&mut transaction), cursor).await?
        else {
            return Err("copy moved inside an owned transaction".into());
        };
        assert_complete_units_fit(&page, 1024 * 1024)?;
        // Exercise the existing staging storage owner, then count its actual
        // persisted TEXT fields and integer plus conservative record framing.
        let seed = SeedPage {
            seed: SeedId {
                generation: u64::MAX,
            },
            page: u32::try_from(pages)?,
            digest: PageDigest::of(&page)?,
            contents: (*page).clone(),
        };
        crate::seed_storage::SeedStorage(&mut TursoScope(&mut transaction))
            .save_page(&seed)
            .await?;
        use sqlx_core::row::Row;
        let staged = sqlx_core::query::query::<sqlx_turso::Turso>(
            "SELECT length(CAST(payload AS BLOB))+length(CAST(seed AS BLOB))+length(CAST(digest AS BLOB))+8 AS bytes FROM _fleet_sync_seed_pages WHERE page=?")
            .bind(i64::from(seed.page)).fetch_one(&mut *transaction).await?.try_get::<i64, _>("bytes")?;
        assert!(
            staged + 128 <= 1024 * 1024,
            "actual staged seed fields exceed target"
        );
        let SnapshotRead::Page(replay) =
            snapshot_page(&mut TursoScope(&mut transaction), replay_cursor).await?
        else {
            return Err("copy replay moved inside an owned transaction".into());
        };
        assert_eq!(serde_json::to_vec(&replay)?, serde_json::to_vec(&page)?);
        if let Some(next) = &page.next {
            assert_eq!(next.table, "records");
            assert!(!page.rows.is_empty());
            assert_eq!(
                next.after_key,
                vec![
                    page.rows
                        .last()
                        .and_then(|row| row.first())
                        .ok_or("missing emitted key")?
                        .clone()
                ]
            );
            byte_short |= page.rows.len() < 256;
        }
        received.extend(page.rows.clone());
        cursor = page.next.clone();
        pages += 1;
        if cursor.is_none() {
            break;
        }
        assert!(pages < 64, "copy page producer did not make progress");
    }
    let SnapshotManifestAnswer::Manifest(manifest) =
        snapshot_manifest(&mut TursoScope(&mut transaction), position(1)?).await?
    else {
        return Err("copy manifest unavailable".into());
    };
    let actual = manifest.tables.first().ok_or("copy manifest empty")?;
    let mut digest = TableDigestBuilder::new();
    for row in &expected {
        digest.push_row(row)?;
    }
    assert!(byte_short, "copy did not split below its row-count limit");
    assert_eq!(received, expected);
    assert_eq!(actual.row_count, 48);
    assert_eq!(actual.digest, digest.finish());
    eprintln!(
        "copy byte paging: {pages} pages,48 rows, {:?} including replay and manifest",
        started.elapsed()
    );
    transaction.rollback().await?;
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Annotation {
    label: String,
}
struct AnnotatedProtocol;
impl ConsumerProtocol for AnnotatedProtocol {
    type Delivery = NoDelivery;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = ();
    type Annotation = Annotation;
}
struct AnnotationRules;
impl ReceiverRules<TursoScope<'_>> for AnnotationRules {
    type Protocol = AnnotatedProtocol;
    type FollowUp = ();
    async fn on_applied(
        &self,
        scope: &mut TursoScope<'_>,
        applied: &AppliedPiece<'_, Annotation>,
    ) -> Result<Vec<()>, HookError> {
        let annotation = applied.annotation.ok_or_else(|| {
            HookError::Storage(StoreError::Corrupt {
                field: "missing final annotation".into(),
            })
        })?;
        sqlx_core::query::query::<sqlx_turso::Turso>("INSERT INTO hook_records(value) VALUES(?)")
            .bind(&annotation.label)
            .execute(&mut *scope.0)
            .await
            .map_err(crate::sql_transport::sql_error)?;
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn empty_final_part_retains_non_clone_annotation_and_atomic_apply() -> TestResult {
    use sqlx_core::row::Row;
    let mut connection = connection().await?;
    let whole = ReplicatedTransaction::Rows {
        commit_id: position(1)?.sequence,
        rows: vec![RowChange::upsert(
            "records",
            vec![uuid_key(9)],
            vec![uuid_key(9), SqlValue::Text("kept row".into())],
        )?],
    };
    let digest = PieceDigest::of_transaction(&whole)?;
    let key = TransactionKey {
        epoch: Epoch::new(1)?,
        commit_id: position(1)?.sequence,
    };
    let first = Piece::<Annotation>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![whole],
        part: Some(TransactionPart::new(key, 0, 2, digest.clone())?),
        annotation: None,
    })?;
    let final_part = Piece::<Annotation>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: position(1)?.sequence,
            rows: Vec::new(),
        }],
        part: Some(TransactionPart::new(key, 1, 2, digest)?),
        annotation: Some(Annotation {
            label: "one whole annotation".into(),
        }),
    })?;
    let final_bytes = serde_json::to_vec(&final_part)?;
    let final_part: Piece<Annotation> = serde_json::from_slice(&final_bytes)?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(
        matches!(apply_piece(&mut TursoScope(&mut transaction), &first, &writer()?, &AnnotationRules, SyncTime::from_unix_millis(1)).await?,
        ApplyDecision::PartStaged { index: 0, acked, .. } if acked == position(0)?)
    );
    let before: i64 =
        sqlx_core::query::query::<sqlx_turso::Turso>("SELECT count(*) AS rows FROM records")
            .fetch_one(&mut *transaction)
            .await?
            .try_get("rows")?;
    assert_eq!(
        before, 0,
        "non-final part exposed a row before atomic apply"
    );
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(
        matches!(apply_piece(&mut TursoScope(&mut transaction), &final_part, &writer()?, &AnnotationRules, SyncTime::from_unix_millis(2)).await?,
        ApplyDecision::Applied { through, .. } if through == position(1)?)
    );
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let replay: Piece<Annotation> = serde_json::from_slice(&final_bytes)?;
    assert!(
        matches!(apply_piece(&mut TursoScope(&mut transaction), &replay, &writer()?, &AnnotationRules, SyncTime::from_unix_millis(3)).await?,
        ApplyDecision::AlreadyApplied { through } if through == position(1)?)
    );
    let body: String = sqlx_core::query::query::<sqlx_turso::Turso>("SELECT body FROM records")
        .fetch_one(&mut *transaction)
        .await?
        .try_get("body")?;
    assert_eq!(body, "kept row");
    let annotations =
        sqlx_core::query::query::<sqlx_turso::Turso>("SELECT value FROM hook_records")
            .fetch_all(&mut *transaction)
            .await?;
    assert_eq!(
        annotations.len(),
        1,
        "replay repeated the annotation effect"
    );
    assert_eq!(
        annotations
            .first()
            .ok_or("missing annotation")?
            .try_get::<String, _>("value")?,
        "one whole annotation"
    );
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn oversize_copy_row_is_refused_before_a_page_is_emitted() -> TestResult {
    let mut connection = connection().await?;
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: position(1)?.sequence,
            rows: vec![RowChange::upsert(
                "records",
                vec![uuid_key(7)],
                vec![uuid_key(7), SqlValue::Text("x".repeat(2 * 1024 * 1024))],
            )?],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(1),
    )
    .await?;
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let result = snapshot_page(&mut TursoScope(&mut transaction), None).await;
    let SqlValue::Text(expected_key) = uuid_key(7) else {
        return Err("invalid fixture key".into());
    };
    assert!(
        matches!(result, Err(ReceiverError::UnitTooLarge(unit_budget::UnitTooLarge::Row {
        table, key, encoded_size
    })) if table == "records" && key == [expected_key] && encoded_size > 1536 * 1024),
        "an oversize copy row was emitted or lost its actual table/key context"
    );
    transaction.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn first_copy_row_above_target_still_progresses_below_the_hard_cap() -> TestResult {
    let mut connection = connection().await?;
    let payload = "x".repeat(1100 * 1024);
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: position(1)?.sequence,
            rows: vec![
                RowChange::upsert(
                    "records",
                    vec![uuid_key(7)],
                    vec![uuid_key(7), SqlValue::Text(payload.clone())],
                )?,
                RowChange::upsert(
                    "records",
                    vec![uuid_key(8)],
                    vec![uuid_key(8), SqlValue::Text("next row".into())],
                )?,
            ],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &incoming,
        &writer()?,
        &Hook { refuse: false },
        SyncTime::from_unix_millis(1),
    )
    .await?;
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let SnapshotRead::Page(page) = snapshot_page(&mut TursoScope(&mut transaction), None).await?
    else {
        return Err("copy moved".into());
    };
    assert_eq!(page.rows, [vec![uuid_key(7), SqlValue::Text(payload)]]);
    assert!(serde_json::to_vec(&page)?.len() > 1024 * 1024);
    assert_complete_units_fit(&page, 1536 * 1024)?;
    let next = page.next.ok_or("large first copy row did not continue")?;
    assert_eq!(next.table, "records");
    assert_eq!(next.after_key, [uuid_key(7)]);
    let SnapshotRead::Page(last) =
        snapshot_page(&mut TursoScope(&mut transaction), Some(next)).await?
    else {
        return Err("copy moved".into());
    };
    assert_eq!(
        last.rows,
        [vec![uuid_key(8), SqlValue::Text("next row".into())]]
    );
    assert!(last.next.is_none());
    assert_complete_units_fit(&last, 1024 * 1024)?;
    transaction.rollback().await?;
    Ok(())
}
