//! The driver's own build links the Turso engine at exactly 0.8.1, and nothing from SQLite or
//! libSQL
//!
//! `turso = "=0.8.1"` pins only the top crate; its eight internal crates are caret requirements
//! (0.8.2 exists), so the lockfile alone holds them. This test fails if any drifts.
//!
//! Scope: a build of the Turso packages by themselves. A combined workspace build unifies
//! features with crates that use SQLx's SQLite driver, so it is not covered by this check.
#![allow(clippy::panic_in_result_fn)]

use std::{collections::BTreeSet, path::PathBuf, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const FORBIDDEN_PACKAGES: [&str; 2] = ["libsqlite3-sys", "sqlx-sqlite"];
const PINNED_TURSO_VERSION: &str = "v0.8.1";
const TURSO_PACKAGES: [&str; 9] = [
    "turso",
    "turso_core",
    "turso_ext",
    "turso_macros",
    "turso_parser",
    "turso_sdk_kit",
    "turso_sdk_kit_macros",
    "turso_sync_engine",
    "turso_sync_sdk_kit",
];

#[test]
fn the_driver_build_contains_no_sqlite_or_libsql_engine() -> TestResult {
    // Arrange
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

    // Act
    let output = Command::new("cargo")
        .args([
            "tree",
            "--locked",
            "--offline",
            "--package",
            "sqlx-turso",
            "--all-features",
            "--edges",
            "normal,build,dev",
            "--prefix",
            "none",
            "--format",
            "{p}",
        ])
        .current_dir(&workspace_root)
        .output()?;
    let tree = String::from_utf8(output.stdout)?;
    let resolved: BTreeSet<(&str, &str)> = tree
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            Some((fields.next()?, fields.next()?))
        })
        .collect();
    let packages: BTreeSet<&str> = resolved.iter().map(|(name, _)| *name).collect();
    let turso_versions: BTreeSet<(&str, &str)> = resolved
        .iter()
        .copied()
        .filter(|(name, _)| name.starts_with("turso"))
        .collect();
    let expected_turso: BTreeSet<(&str, &str)> = TURSO_PACKAGES
        .iter()
        .map(|name| (*name, PINNED_TURSO_VERSION))
        .collect();

    // Assert
    assert!(
        output.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        turso_versions, expected_turso,
        "every Turso package must resolve to {PINNED_TURSO_VERSION}"
    );
    let forbidden: Vec<&str> = packages
        .iter()
        .copied()
        .filter(|name| FORBIDDEN_PACKAGES.contains(name) || name.starts_with("libsql"))
        .collect();
    assert!(
        forbidden.is_empty(),
        "non-Turso engines in the graph: {forbidden:?}"
    );
    Ok(())
}
