#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::TursoScope, turso_snapshot_install_tests::source, *};
use sqlx_core::{connection::ConnectOptions, query::query, row::Row};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use uuid::Uuid;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn staging_file() -> TestResult<(tempfile::TempDir, TursoConnection)> {
    let directory = tempfile::tempdir()?;
    let mut connection = TursoConnectOptions::new()
        .filename(directory.path().join("copy.db"))
        .create_if_missing(true)
        .foreign_keys(false)
        .connect()
        .await?;
    RECEIVER_MIGRATOR
        .run_direct(None, &mut connection, false)
        .await?;
    Ok((directory, connection))
}

fn replace_install_id(record: &str, container: Option<&str>, spelling: &str) -> TestResult<String> {
    let mut value: serde_json::Value = serde_json::from_str(record)?;
    let target = match container {
        Some(container) => value
            .get_mut(container)
            .ok_or("missing install container")?,
        None => &mut value,
    };
    *target
        .get_mut("install_id")
        .ok_or("missing install identity")? = serde_json::json!(spelling);
    Ok(serde_json::to_string(&value)?)
}

#[tokio::test]
async fn direct_receiver_install_rejects_invalid_identity_before_any_marker_or_page() -> TestResult
{
    let (_directory, mut connection) = staging_file().await?;
    let (install, _, _, _) = source()?;
    let encoded = serde_json::to_string(&install)?;
    let canonical = install.install_id.to_string();
    for spelling in [
        canonical.replacen("7000", "4000", 1),
        Uuid::nil().to_string(),
        canonical.to_uppercase(),
        canonical.replace('-', ""),
    ] {
        let invalid = replace_install_id(&encoded, None, &spelling)?;
        assert!(
            serde_json::from_str::<SnapshotInstall>(&invalid).is_err(),
            "accepted {spelling}"
        );
    }
    let rows = query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_copy)+(SELECT count(*) FROM _fleet_sync_install)+(SELECT count(*) FROM _fleet_sync_install_pages) AS count")
        .fetch_one(&mut connection).await?.try_get::<i64, _>("count")?;
    assert_eq!(rows, 0);
    assert_eq!(serde_json::from_str::<SnapshotInstall>(&encoded)?, install);
    Ok(())
}

#[tokio::test]
async fn receiver_reads_refuse_noncanonical_install_markers_and_page_receipts() -> TestResult {
    for compact in [false, true] {
        let (_directory, mut connection) = staging_file().await?;
        let (install, source, page, _) = source()?;
        let canonical = install.install_id.to_string();
        let spelling = if compact {
            canonical.replace('-', "")
        } else {
            canonical.to_uppercase()
        };
        SnapshotInstallRules(&mut TursoScope(&mut connection))
            .prepare(&install, &source)
            .await?;
        let copy: String = query::<Turso>("SELECT record FROM _fleet_sync_copy")
            .fetch_one(&mut connection)
            .await?
            .try_get("record")?;
        let corrupt = replace_install_id(&copy, Some("copy"), &spelling)?;
        query::<Turso>("UPDATE _fleet_sync_copy SET record=?")
            .bind(corrupt)
            .execute(&mut connection)
            .await?;
        assert!(copy_status(&mut TursoScope(&mut connection)).await.is_err());
        query::<Turso>("UPDATE _fleet_sync_copy SET record=?")
            .bind(copy)
            .execute(&mut connection)
            .await?;
        let marker: String = query::<Turso>("SELECT record FROM _fleet_sync_install")
            .fetch_one(&mut connection)
            .await?
            .try_get("record")?;
        let corrupt = replace_install_id(&marker, Some("install"), &spelling)?;
        query::<Turso>("UPDATE _fleet_sync_install SET record=?")
            .bind(corrupt)
            .execute(&mut connection)
            .await?;
        assert!(
            SnapshotInstallRules(&mut TursoScope(&mut connection))
                .state()
                .await
                .is_err()
        );
        query::<Turso>("UPDATE _fleet_sync_install SET record=?")
            .bind(marker)
            .execute(&mut connection)
            .await?;
        let receipt = SnapshotInstallRules(&mut TursoScope(&mut connection))
            .stage_page(&install, 0, None, &page)
            .await?;
        let encoded = serde_json::to_string(&receipt)?;
        let corrupt = replace_install_id(&encoded, None, &spelling)?;
        query::<Turso>("UPDATE _fleet_sync_install_pages SET record=?")
            .bind(corrupt)
            .execute(&mut connection)
            .await?;
        assert!(
            SnapshotInstallRules(&mut TursoScope(&mut connection))
                .page_receipt(&install, 0)
                .await
                .is_err()
        );
        query::<Turso>("UPDATE _fleet_sync_install_pages SET record=?, install_id=?")
            .bind(encoded)
            .bind(spelling)
            .execute(&mut connection)
            .await?;
        assert!(
            SnapshotInstallRules(&mut TursoScope(&mut connection))
                .page_receipt(&install, 0)
                .await
                .is_err()
        );
    }
    Ok(())
}
