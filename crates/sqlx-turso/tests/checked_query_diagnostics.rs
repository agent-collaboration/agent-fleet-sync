//! Compile-time proof that checked queries are described by the Turso engine
//!
//! trybuild compiles each case as its own crate. Those compilations must describe queries
//! online against `turso::memory:`, so the environment differs from this test process's. Rather
//! than mutate the environment (`set_var` is unsafe and the workspace forbids unsafe code), the
//! test re-runs exactly itself in a child process whose environment is set at spawn.
#![allow(clippy::panic_in_result_fn)]

use std::{
    process::Command,
    time::{Duration, Instant},
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const CHILD_MARKER: &str = "SQLX_TURSO_DIAGNOSTICS_CHILD";
const THIS_TEST: &str = "checked_queries_compile_or_fail_as_the_engine_describes";
/// trybuild builds the cases' dependency graph once; allow for a cold cache
const CHILD_DEADLINE: Duration = Duration::from_secs(1_200);

#[test]
fn checked_queries_compile_or_fail_as_the_engine_describes() -> TestResult {
    if std::env::var_os(CHILD_MARKER).is_some() {
        let cases = trybuild::TestCases::new();
        cases.pass("tests/checked_query_diagnostics/pass/*.rs");
        cases.compile_fail("tests/checked_query_diagnostics/fail/*.rs");
        // The cases run and report when `cases` drops.
        return Ok(());
    }

    let private_metadata = tempfile::tempdir()?;
    let mut child = Command::new(std::env::current_exe()?)
        .args([THIS_TEST, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_MARKER, "1")
        .env("DATABASE_URL", "turso::memory:")
        .env("SQLX_OFFLINE", "false")
        .env("SQLX_OFFLINE_DIR", private_metadata.path())
        .spawn()?;

    let deadline = Instant::now() + CHILD_DEADLINE;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let _reaped = child.wait()?;
            return Err(format!("trybuild child did not finish within {CHILD_DEADLINE:?}").into());
        }
        std::thread::sleep(Duration::from_millis(200));
    };

    assert!(status.success(), "trybuild cases failed: {status}");
    Ok(())
}
