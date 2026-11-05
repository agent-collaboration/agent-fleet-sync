//! Every public migration path must fit beneath its host's retained caller frames.
use durable_steps::turso_store::migrate_journal;
use fleet_sync_delivery::DELIVERY_MIGRATOR;
use fleet_sync_lease::LEASE_MIGRATOR;
use fleet_sync_session::SESSION_MIGRATOR;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    migrate::{Migration, Migrator},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::{hint::black_box, process::Command};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn corpus(engine: &str) -> Vec<Migration> {
    match engine {
        "delivery" => DELIVERY_MIGRATOR.iter().cloned().collect(),
        "lease" => LEASE_MIGRATOR.sql_migrations().cloned().collect(),
        "session" => SESSION_MIGRATOR.sql_migrations().cloned().collect(),
        "journal" => sqlx::migrate!("../durable-steps/migrations")
            .iter()
            .cloned()
            .collect(),
        _ => panic!("unknown migration engine"),
    }
}

// These are the last migrations in the released v0.4.0 corpora, independently
// checked against that tag. Current public prefix SQL remains byte-identical.
fn released_version(engine: &str) -> i64 {
    match engine {
        "delivery" => 3,
        "lease" => 4,
        "session" => 202610180001,
        "journal" => 1,
        _ => panic!("unknown migration engine"),
    }
}

fn history_table(engine: &str) -> &'static str {
    if engine == "journal" {
        "_fleet_sync_migrations"
    } else {
        "_sqlx_migrations"
    }
}

async fn migrate_public(engine: &str, connection: &mut TursoConnection) -> Result {
    match engine {
        "delivery" => {
            DELIVERY_MIGRATOR
                .run_direct(None, connection, false)
                .await?
        }
        "lease" => LEASE_MIGRATOR.run_direct(None, connection, false).await?,
        "session" => SESSION_MIGRATOR.run_direct(None, connection, false).await?,
        "journal" => migrate_journal(connection).await?,
        _ => panic!("unknown migration engine"),
    }
    Ok(())
}

async fn receipt_version(engine: &str, connection: &mut TursoConnection) -> Result<i64> {
    let sql = format!(
        "SELECT max(version) AS version FROM {}",
        history_table(engine)
    );
    Ok(query::<Turso>(sqlx_core::sql_str::AssertSqlSafe(sql))
        .fetch_one(connection)
        .await?
        .try_get("version")?)
}

async fn assert_no_scratch_columns(connection: &mut TursoConnection) -> Result {
    let tables = query::<Turso>("SELECT name FROM sqlite_master WHERE type='table'")
        .fetch_all(&mut *connection)
        .await?;
    for table in tables {
        let name: String = table.try_get("name")?;
        let columns = query::<Turso>(sqlx_core::sql_str::AssertSqlSafe(format!(
            "PRAGMA table_info({name})"
        )))
        .fetch_all(&mut *connection)
        .await?;
        for column in columns {
            let name: String = column.try_get("name")?;
            assert!(
                !name.starts_with("_uuid_"),
                "scratch column survived migration"
            );
        }
    }
    Ok(())
}

#[inline(never)]
fn retain_caller_frames(depth: usize, action: &mut dyn FnMut() -> Result) -> Result {
    let retained = [0_u8; 8192];
    black_box(&retained);
    let result = if depth == 0 {
        action()
    } else {
        retain_caller_frames(depth - 1, action)
    };
    black_box(&retained);
    result
}

fn run_child(phase: &str) -> Result {
    let (engine, layout) = phase.split_once(':').ok_or("invalid child phase")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let directory = tempfile::tempdir()?;
    let mut connection = runtime.block_on(
        TursoConnectOptions::new()
            .filename(directory.path().join("migration.sqlite"))
            .create_if_missing(true)
            .foreign_keys(true)
            .connect(),
    )?;
    if layout == "v0.4.0" {
        let mut prefix = Migrator::with_migrations(
            corpus(engine)
                .into_iter()
                .filter(|migration| migration.version <= released_version(engine))
                .collect(),
        );
        prefix.dangerous_set_table_name(history_table(engine));
        runtime.block_on(async {
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            prefix.run_direct(None, &mut *transaction, false).await?;
            transaction.commit().await
        })?;
        assert_eq!(
            runtime.block_on(receipt_version(engine, &mut connection))?,
            released_version(engine)
        );
    } else {
        assert_eq!(layout, "fresh");
    }
    if engine == "delivery" && layout == "v0.4.0" {
        runtime.block_on(super::legacy_delivery::seed(&mut connection))?;
    }
    // Use the ordinary default thread stack, retaining at least one MiB for
    // the caller. Do not raise a stack limit to fit the parser.
    retain_caller_frames(128, &mut || {
        runtime.block_on(async {
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            migrate_public(engine, &mut transaction).await?;
            transaction.commit().await?;
            Ok(())
        })
    })?;
    assert_eq!(
        runtime.block_on(receipt_version(engine, &mut connection))?,
        corpus(engine)
            .last()
            .ok_or("empty migration corpus")?
            .version
    );
    runtime.block_on(assert_no_scratch_columns(&mut connection))?;
    if engine == "delivery" && layout == "v0.4.0" {
        runtime.block_on(super::legacy_delivery::assert_preserved(&mut connection))?;
    }
    println!("PUBLIC_MIGRATOR_STACK_OK:{phase}");
    Ok(())
}

#[test]
fn migration_child() -> Result {
    let Ok(phase) = std::env::var("FLEET_PUBLIC_MIGRATION_CHILD") else {
        return Ok(());
    };
    std::thread::spawn(move || run_child(&phase).map_err(|error| error.to_string()))
        .join()
        .map_err(|_| "migration thread panicked")??;
    Ok(())
}

fn assert_public_migrator(phase: &str) -> Result {
    // current_exe was signed by the Cargo runner before this parent test ran.
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", "migration_stack::migration_child", "--nocapture"])
        .env("FLEET_PUBLIC_MIGRATION_CHILD", phase)
        .env_remove("RUST_MIN_STACK")
        .output()?;
    assert!(
        child.status.success(),
        "{phase} failed below retained caller frames: {}\n{}",
        child.status,
        String::from_utf8_lossy(&child.stderr)
    );
    assert!(
        String::from_utf8_lossy(&child.stdout)
            .contains(&format!("PUBLIC_MIGRATOR_STACK_OK:{phase}")),
        "child did not execute the selected public migrator"
    );
    Ok(())
}

#[test]
fn delivery_fresh() -> Result {
    assert_public_migrator("delivery:fresh")
}
#[test]
fn delivery_from_v0_4_0() -> Result {
    assert_public_migrator("delivery:v0.4.0")
}
#[test]
fn lease_fresh() -> Result {
    assert_public_migrator("lease:fresh")
}
#[test]
fn lease_from_v0_4_0() -> Result {
    assert_public_migrator("lease:v0.4.0")
}
#[test]
fn session_fresh() -> Result {
    assert_public_migrator("session:fresh")
}
#[test]
fn session_from_v0_4_0() -> Result {
    assert_public_migrator("session:v0.4.0")
}
#[test]
fn journal_fresh() -> Result {
    assert_public_migrator("journal:fresh")
}
#[test]
fn journal_from_v0_4_0() -> Result {
    assert_public_migrator("journal:v0.4.0")
}
