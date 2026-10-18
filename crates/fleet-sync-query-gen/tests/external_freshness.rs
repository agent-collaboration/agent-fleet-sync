#![cfg(feature = "native")]
#![allow(clippy::panic_in_result_fn)]
use fleet_sync_query_gen::{CorpusMetadata, QueryManifest, QueryResult, QuerySpec};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};
#[path = "support/metadata_integrity_cases.rs"]
mod metadata_integrity_cases;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn library_root() -> TestResult<PathBuf> {
    Ok(Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or("library workspace missing")?
        .canonicalize()?)
}

fn copied_library(source: &Path, destination: &Path) -> TestResult {
    // Verification also runs from immutable source archives without .git.
    for relative in [
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        "README.md",
        ".cargo",
        "crates",
        "tests",
        "licenses",
    ] {
        copy_source_entry(source, destination, Path::new(relative))?;
    }
    Ok(())
}

fn copy_source_entry(source: &Path, destination: &Path, relative: &Path) -> TestResult {
    let original = source.join(relative);
    let target = destination.join(relative);
    if original.is_dir() {
        let name = relative.file_name().and_then(|name| name.to_str());
        if matches!(
            name,
            Some("target" | "node_modules" | ".git" | ".wrangler" | "__pycache__")
        ) || [
            "tests/workerd/build",
            "tests/workerd/workerd-state",
            "tests/durable-steps/build",
            "tests/normal-migrations-worker/build",
            "tests/normal-migrations-worker/evidence",
            "crates/sqlx-durable-object/tests/worker/build",
        ]
        .iter()
        .any(|artifact| relative == Path::new(artifact))
        {
            return Ok(());
        }
        fs::create_dir_all(&target)?;
        for entry in fs::read_dir(original)? {
            copy_source_entry(source, destination, &relative.join(entry?.file_name()))?;
        }
    } else if original.is_file() {
        fs::create_dir_all(target.parent().ok_or("copied file parent missing")?)?;
        fs::copy(original, target)?;
    }
    Ok(())
}

fn write_dependency_manifest(consumer: &Path, library: &Path) -> TestResult {
    let package_path = |package: &str| -> TestResult<String> {
        Ok(serde_json::to_string(
            &library.join("crates").join(package),
        )?)
    };
    fs::write(
        consumer.join("Cargo.toml"),
        format!(
            "[package]\nname = \"external-freshness-consumer\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n\n[dependencies]\ndurable-steps = {{ path = {}, default-features = false }}\nsqlx-core = {{ version = \"=0.9.0\", default-features = false }}\n\n[build-dependencies]\nfleet-sync-query-gen = {{ path = {}, default-features = false }}\n\n[profile.dev]\ndebug = 0\nincremental = false\n",
            package_path("durable-steps")?,
            package_path("fleet-sync-query-gen")?,
        ),
    )?;
    Ok(())
}

fn external_consumer(consumer: &Path, library: &Path) -> TestResult<PathBuf> {
    for directory in ["src", "sql", "migrations"] {
        fs::create_dir_all(consumer.join(directory))?;
    }
    write_dependency_manifest(consumer, library)?;
    fs::write(
        consumer.join("src/lib.rs"),
        "#[allow(dead_code)]\nmod checked_queries;\n",
    )?;
    fs::write(
        consumer.join("build.rs"),
        "fn main() {\n    if let Err(error) = fleet_sync_query_gen::verify_manifest(\"checked-queries.json\") {\n        eprintln!(\"{error}\");\n        std::process::exit(1);\n    }\n}\n",
    )?;
    fs::write(
        consumer.join("migrations/1_records.sql"),
        "CREATE TABLE records(id TEXT PRIMARY KEY, amount INTEGER NOT NULL);\n",
    )?;
    fs::write(
        consumer.join("sql/read.sql"),
        "SELECT id, amount FROM records\n",
    )?;
    let queries = ["first_read", "second_read"]
        .into_iter()
        .map(|name| QuerySpec {
            name: name.into(),
            sql: "sql/read.sql".into(),
            row: Some("RecordRow".into()),
            result: QueryResult::Many,
            parameters: Vec::new(),
        })
        .collect();
    let manifest = consumer.join("checked-queries.json");
    fs::write(
        &manifest,
        serde_json::to_vec_pretty(&QueryManifest {
            migrations: vec!["migrations".into()],
            output: "src/checked_queries.rs".into(),
            metadata: "checked-query-metadata.json".into(),
            queries,
        })?,
    )?;
    Ok(manifest)
}

fn check_consumer(consumer: &Path, target: &Path, locked: bool) -> TestResult<Output> {
    check_consumer_target(consumer, target, locked, None)
}

fn check_consumer_target(
    consumer: &Path,
    target: &Path,
    locked: bool,
    target_triple: Option<&str>,
) -> TestResult<Output> {
    let mut command = Command::new(env!("CARGO"));
    command
        .args(["check", "--offline", "-vv", "--manifest-path"])
        .arg(consumer.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(target)
        .current_dir(library_root()?);
    if locked {
        command.arg("--locked");
    }
    if let Some(target_triple) = target_triple {
        command.args(["--target", target_triple]);
    }
    Ok(command.output()?)
}

fn output_text(output: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn watched_paths(output: &Output, consumer: &Path) -> Vec<PathBuf> {
    output_text(output)
        .lines()
        .filter_map(|line| line.split_once("cargo:rerun-if-changed="))
        .map(|(_, path)| {
            let path = PathBuf::from(path.trim());
            if path.is_absolute() {
                path
            } else {
                consumer.join(path)
            }
        })
        .collect()
}

fn assert_mutation_refused(
    consumer: &Path,
    target: &Path,
    path: &Path,
    suffix: &[u8],
    logical_key: &str,
) -> TestResult {
    let original = fs::read(path)?;
    let mut changed = original.clone();
    changed.extend_from_slice(suffix);
    fs::write(path, changed)?;
    let result = check_consumer(consumer, target, true);
    // Restore before assertions, even if Cargo or the assertion failed.
    fs::write(path, original)?;
    let output = result?;
    let text = output_text(&output);
    println!(
        "OBS mutation {logical_key}: exit={:?}",
        output.status.code()
    );
    assert!(!output.status.success(), "accepted changed {logical_key}");
    assert!(
        text.contains(&format!("stale checked query input {logical_key};")),
        "mutation failed outside freshness for {logical_key}: {text}"
    );
    Ok(())
}

#[test]
fn generated_external_consumer_survives_independent_library_and_consumer_relocation() -> TestResult
{
    let temporary = tempfile::tempdir()?;
    let library_a = library_root()?;
    let consumer_a = temporary.path().join("consumer-a");
    let library_b = temporary
        .path()
        .join("independently-moved/deeper/library-b");
    let consumer_b = temporary
        .path()
        .join("consumer-only/moved-again/consumer-b");
    let target = temporary.path().join("cargo-target");
    let manifest = external_consumer(&consumer_a, &library_a)?;
    let generated = Command::new(env!("CARGO_BIN_EXE_fleet-sync-query-gen"))
        .arg("--manifest")
        .arg(&manifest)
        .current_dir(&library_a)
        .output()?;
    assert!(generated.status.success(), "{}", output_text(&generated));
    let generated_source = fs::read(consumer_a.join("src/checked_queries.rs"))?;
    let metadata_bytes = fs::read(consumer_a.join("checked-query-metadata.json"))?;
    let initial = check_consumer(&consumer_a, &target, false)?;
    assert!(
        initial.status.success(),
        "initial real external build: {}",
        output_text(&initial)
    );
    println!("OBS generated CLI artifacts and built external consumer A with shared SQL: exit=0");

    // First relocate only the library, compiling the actual dependency at B.
    copied_library(&library_a, &library_b)?;
    write_dependency_manifest(&consumer_a, &library_b)?;
    let generator_moved = check_consumer(&consumer_a, &target, true)?;
    println!(
        "OBS library-only relocation: exit={:?}",
        generator_moved.status.code()
    );
    // Then relocate only the consumer, keeping the generator's B path fixed.
    fs::create_dir_all(consumer_b.parent().ok_or("consumer parent missing")?)?;
    fs::rename(&consumer_a, &consumer_b)?;
    let consumer_moved = check_consumer(&consumer_b, &target, true)?;
    println!(
        "OBS consumer-only relocation: exit={:?}",
        consumer_moved.status.code()
    );
    assert_eq!(
        fs::read(consumer_b.join("src/checked_queries.rs"))?,
        generated_source
    );
    assert_eq!(
        fs::read(consumer_b.join("checked-query-metadata.json"))?,
        metadata_bytes
    );
    assert!(
        generator_moved.status.success(),
        "library-only relocation: {}",
        output_text(&generator_moved)
    );
    assert!(
        consumer_moved.status.success(),
        "consumer-only relocation: {}",
        output_text(&consumer_moved)
    );

    let watches = watched_paths(&consumer_moved, &consumer_b);
    assert!(!watches.is_empty(), "no Cargo watches observed");
    for watched in &watches {
        assert!(
            watched.exists(),
            "Cargo watched a logical/nonexistent path: {watched:?}"
        );
    }
    for required in [
        library_b.join("Cargo.lock"),
        library_b.join("rust-toolchain.toml"),
        library_b.join("crates/fleet-sync-query-gen/src/lib.rs"),
        library_b.join("crates/sqlx-turso-core/src/lib.rs"),
        consumer_b.join("sql/read.sql"),
        consumer_b.join("src/checked_queries.rs"),
    ] {
        let required = required.canonicalize()?;
        assert!(
            watches
                .iter()
                .any(|path| path.canonicalize().ok().as_ref() == Some(&required)),
            "missing real Cargo watch: {required:?}"
        );
    }
    println!("OBS Cargo watches resolve to actual relocated inputs and output");

    for (path, suffix, key) in [
        (
            consumer_b.join("sql/read.sql"),
            "\n-- changed query\n",
            "sql/read.sql",
        ),
        (
            consumer_b.join("migrations/1_records.sql"),
            "\n-- changed migration\n",
            "migrations/1_records.sql",
        ),
        (
            consumer_b.join("src/checked_queries.rs"),
            "\n// changed output\n",
            "src/checked_queries.rs",
        ),
        (
            library_b.join("crates/fleet-sync-query-gen/src/lib.rs"),
            "\n// changed generator\n",
            "generator:fleet-sync-query-gen/src/lib.rs",
        ),
        (
            library_b.join("crates/sqlx-turso-core/src/lib.rs"),
            "\n// changed driver\n",
            "generator:sqlx-turso-core/src/lib.rs",
        ),
        (
            library_b.join("Cargo.lock"),
            "\n# changed generator lock\n",
            "generator:workspace/Cargo.lock",
        ),
        (
            library_b.join("rust-toolchain.toml"),
            "\n# changed generator toolchain\n",
            "generator:workspace/rust-toolchain.toml",
        ),
    ] {
        assert_mutation_refused(&consumer_b, &target, &path, suffix.as_bytes(), key)?;
    }
    let recovered = check_consumer(&consumer_b, &target, true)?;
    assert!(
        recovered.status.success(),
        "restored input recovery: {}",
        output_text(&recovered)
    );
    println!("OBS seven independent input/output mutations refused; restored build: exit=0");

    let metadata_path = consumer_b.join("checked-query-metadata.json");
    let mut legacy: CorpusMetadata = serde_json::from_slice(&metadata_bytes)?;
    let mut generator_keys = 0;
    legacy.sources = legacy
        .sources
        .into_iter()
        .map(|(key, digest)| {
            let Some(generator) = key.strip_prefix("generator:") else {
                return Ok((key, digest));
            };
            let (package, relative) = generator.split_once('/').ok_or("bad generator key")?;
            let relative = if package == "workspace" {
                relative.to_owned()
            } else {
                format!("crates/{package}/{relative}")
            };
            let old_key = format!("../../../independently-moved/deeper/library-b/{relative}");
            assert_eq!(
                consumer_b.join(&old_key).canonicalize()?,
                library_b.join(relative).canonicalize()?
            );
            generator_keys += 1;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((old_key, digest))
        })
        .collect::<TestResult<_>>()?;
    assert!(generator_keys > 4, "no actual generator keys converted");
    fs::write(&metadata_path, serde_json::to_vec_pretty(&legacy)?)?;
    let legacy_result = check_consumer(&consumer_b, &target, true);
    fs::write(&metadata_path, metadata_bytes)?;
    let legacy_result = legacy_result?;
    assert!(
        !legacy_result.status.success(),
        "legacy key fallback accepted"
    );
    assert!(output_text(&legacy_result).contains("stale checked query input"));
    println!("OBS legacy generator key refused without fallback");
    Ok(())
}
