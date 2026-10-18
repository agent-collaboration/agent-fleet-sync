//! Seed authority is authenticated host input and precedes every replay and mutation.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_seed_tests::{NoHook, TursoScope, database, position, setup_with_holder, snapshot},
    *,
};
use fleet_sync_protocol::*;
use sqlx_core::{connection::Connection, row::Row};
use sqlx_turso::{Turso, TursoConnection};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
fn writer(id: u128) -> TestResult<PeerId> {
    Ok(PeerId::new(Uuid::from_u128(
        (id) | 0x019eef00000070008000000000000000,
    ))?)
}
fn attempt() -> TestResult<SeedAttemptId> {
    Ok(SeedAttemptId::new(Uuid::from_u128(
        0x019eef000000700080000000000008fc,
    ))?)
}
async fn state(connection: &mut TursoConnection) -> TestResult<Vec<String>> {
    let rows = sqlx_core::query::query::<Turso>("SELECT 'copy:'||record AS value FROM _fleet_sync_copy UNION ALL SELECT 'seed:'||record FROM _fleet_sync_seed UNION ALL SELECT 'attempt:'||attempt||':'||record FROM _fleet_sync_seed_attempts UNION ALL SELECT 'page:'||seed||':'||CAST(page AS TEXT)||':'||digest||':'||payload FROM _fleet_sync_seed_pages UNION ALL SELECT 'creation:'||request FROM _fleet_sync_creation UNION ALL SELECT 'schema:'||type||':'||name||':'||coalesce(sql,'') FROM sqlite_schema ORDER BY value").fetch_all(connection).await?;
    Ok(rows
        .iter()
        .map(|row| row.try_get("value"))
        .collect::<Result<_, _>>()?)
}
async fn stage(
    connection: &mut TursoConnection,
    authority: &SeedAuthority,
) -> TestResult<(SeedPage, SeedComplete)> {
    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    let seed = seed_begin(&mut TursoScope(&mut tx), database()?, attempt()?, authority).await?;
    let (page, done) = snapshot(seed)?;
    assert_eq!(
        seed_page(&mut TursoScope(&mut tx), &page, authority).await?,
        SeedPageAnswer::Staged
    );
    tx.commit().await?;
    Ok((page, done))
}
async fn refuse_all(
    connection: &mut TursoConnection,
    authority: &SeedAuthority,
    page: &SeedPage,
    done: &SeedComplete,
    epoch: u64,
) -> TestResult {
    let expected = Some(ReceiverError::NotWriter {
        current: Some(Epoch::new(epoch)?),
    });
    let before = state(connection).await?;
    for offered in [
        attempt()?,
        SeedAttemptId::new(Uuid::from_u128(0x019eef000000700080000000000008fd))?,
    ] {
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        assert_eq!(
            seed_begin(&mut TursoScope(&mut tx), database()?, offered, authority)
                .await
                .err(),
            expected
        );
        tx.commit().await?;
        assert_eq!(state(connection).await?, before);
    }
    for index in [0, 1] {
        let mut offered = page.clone();
        offered.page = index;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        assert_eq!(
            seed_page(&mut TursoScope(&mut tx), &offered, authority)
                .await
                .err(),
            expected
        );
        tx.commit().await?;
        assert_eq!(state(connection).await?, before);
    }
    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        seed_complete(&mut TursoScope(&mut tx), done, &NoHook, authority)
            .await
            .err(),
        expected
    );
    tx.commit().await?;
    assert_eq!(state(connection).await?, before);
    Ok(())
}
async fn publish(
    connection: &mut TursoConnection,
    authority: &SeedAuthority,
    done: &SeedComplete,
) -> TestResult {
    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(
        matches!(seed_complete(&mut TursoScope(&mut tx), done, &NoHook, authority).await?, SeedDecision::Published { position: actual, follow_ups } if actual==position()? && follow_ups.is_empty())
    );
    tx.commit().await?;
    let row =
        sqlx_core::query::query::<Turso>("SELECT CAST(count(*) AS TEXT) AS rows FROM records")
            .fetch_one(&mut *connection)
            .await?;
    assert_eq!(row.try_get::<String, _>("rows")?, "2");
    Ok(())
}

#[tokio::test]
async fn writer_seed_refuses_another_peer_and_admin_before_every_mutation_or_replay() -> TestResult
{
    let authority = SeedAuthority::Writer(writer(1)?);
    let mut connection = setup_with_holder(Some(position()?), Some(writer(1)?)).await?;
    let (page, done) = stage(&mut connection, &authority).await?;
    for wrong in [SeedAuthority::Writer(writer(2)?), SeedAuthority::Admin] {
        refuse_all(&mut connection, &wrong, &page, &done, 1).await?;
    }
    publish(&mut connection, &authority, &done).await?;
    for wrong in [SeedAuthority::Writer(writer(2)?), SeedAuthority::Admin] {
        refuse_all(&mut connection, &wrong, &page, &done, 1).await?;
    }
    Ok(())
}

#[tokio::test]
async fn imported_copy_admits_only_admin_and_refuses_writer_staging_and_completed_replays()
-> TestResult {
    let mut connection = setup_with_holder(None, None).await?;
    let (page, done) = stage(&mut connection, &SeedAuthority::Admin).await?;
    refuse_all(
        &mut connection,
        &SeedAuthority::Writer(writer(1)?),
        &page,
        &done,
        1,
    )
    .await?;
    publish(&mut connection, &SeedAuthority::Admin, &done).await?;
    refuse_all(
        &mut connection,
        &SeedAuthority::Writer(writer(1)?),
        &page,
        &done,
        1,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn seed_authority_comes_from_immutable_creation_after_the_current_holder_changes()
-> TestResult {
    let original = SeedAuthority::Writer(writer(1)?);
    let mut connection = setup_with_holder(Some(position()?), Some(writer(1)?)).await?;
    let (page, done) = stage(&mut connection, &original).await?;
    publish(&mut connection, &original, &done).await?;
    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        fence(
            &mut TursoScope(&mut tx),
            &FenceRequest {
                database: database()?,
                transfer_id: TransferId::new(Uuid::from_u128(0x019eef000000700080000000000008fe))?,
                new_epoch: Epoch::new(2)?,
                expect: FenceExpectation {
                    epoch: Epoch::new(1)?,
                    acked: position()?
                },
                target_schema_max: 10,
                holder: writer(2)?
            }
        )
        .await?,
        FenceResult::Fenced { .. }
    ));
    tx.commit().await?;
    refuse_all(
        &mut connection,
        &SeedAuthority::Writer(writer(2)?),
        &page,
        &done,
        2,
    )
    .await?;
    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        seed_begin(&mut TursoScope(&mut tx), database()?, attempt()?, &original).await?,
        page.seed
    );
    assert!(
        matches!(seed_complete(&mut TursoScope(&mut tx), &done, &NoHook, &original).await?, SeedDecision::Answer(SeedAnswer::Published { position: actual }) if actual==position()?)
    );
    tx.commit().await?;
    Ok(())
}
