#![allow(clippy::panic_in_result_fn)]
use crate::{turso_seed_tests::*, *};
use fleet_sync_protocol::{unit_budget::*, *};
use sqlx_core::{connection::Connection, query::query, row::Row};
use sqlx_turso::Turso;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn actual_encoded_seed_units_accept_the_cap_and_refuse_overflow_before_staging() -> TestResult
{
    for offset in [0_usize, 1] {
        let mut connection = setup(None).await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let seed = seed_begin(
            &mut TursoScope(&mut transaction),
            database()?,
            SeedAttemptId::new("019eef00-0000-7000-8000-000000000731".parse()?)?,
            &SeedAuthority::Admin,
        )
        .await?;
        let (mut page, _) = snapshot(seed)?;
        let key = SqlValue::Text("019eef00-0000-7000-8000-000000000001".into());
        page.contents.rows = vec![vec![key.clone(), SqlValue::Text(String::new())]];
        let overhead = measure_snapshot_unit(page.contents.clone())?.encoded_size;
        page.contents.rows = vec![vec![
            key,
            SqlValue::Text("x".repeat(MAX_UNIT_BYTES + offset - overhead)),
        ]];
        page.digest = PageDigest::of(&page.contents)?;
        assert_eq!(
            measure_snapshot_unit(page.contents.clone())?.encoded_size,
            MAX_UNIT_BYTES + offset,
            "real typed encodings must establish the exact fixture boundary"
        );
        let result = seed_page(
            &mut TursoScope(&mut transaction),
            &page,
            &SeedAuthority::Admin,
        )
        .await;
        if offset == 0 {
            assert_eq!(result?, SeedPageAnswer::Staged);
        } else {
            let error = match result {
                Err(error @ ReceiverError::InvalidInput { .. }) => error,
                other => return Err(format!("oversize seed was not refused: {other:?}").into()),
            };
            assert!(
                unavailable_receiver(&error).is_none(),
                "oversize is not transient"
            );
            let classified = classify_copy_store_error(receiver_rollback(error));
            assert!(!matches!(
                classified,
                CopyTransactionFailure::Unavailable(_)
            ));
        }
        let row = query::<Turso>("SELECT COUNT(*) AS count FROM _fleet_sync_seed_pages")
            .fetch_one(&mut *transaction)
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, i64::from(offset == 0));
        let row =
            query::<Turso>("SELECT COUNT(*) AS count FROM sqlite_master WHERE name='records'")
                .fetch_one(&mut *transaction)
                .await?;
        assert_eq!(
            row.try_get::<i64, _>("count")?,
            0,
            "staging never publishes live rows"
        );
        transaction.commit().await?;
        connection.close().await?;
    }
    Ok(())
}
