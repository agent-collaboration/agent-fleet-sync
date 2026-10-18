//! Build the emitted portable consumer through its actual public build.rs gate.
use super::{TestResult, check_consumer_target, external_consumer, library_root, output_text};
use serde_json::Value;
use std::{fs, process::Command};

fn description_field_mut<'a>(metadata: &'a mut Value, pointer: &str) -> TestResult<&'a mut Value> {
    metadata
        .pointer_mut(pointer)
        .ok_or_else(|| format!("generated metadata has no {pointer}").into())
}

fn metadata_mutants(original: &Value) -> TestResult<Vec<(&'static str, Value)>> {
    let mut cases = Vec::new();
    for (name, field, replacement) in [
        ("rust_type", "rust_type", Value::from("Option<i64>")),
        ("sql_type", "sql_type", Value::from("INTEGER")),
        ("column_name", "name", Value::from("renamed_column")),
        ("nullable", "nullable", Value::from(false)),
        ("type_source", "type_source", Value::from("cast")),
    ] {
        let mut mutated = original.clone();
        *description_field_mut(&mut mutated, &format!("/queries/0/columns/0/{field}"))? =
            replacement;
        cases.push((name, mutated));
    }
    let mut query_name = original.clone();
    *description_field_mut(&mut query_name, "/queries/0/name")? = Value::from("renamed_query");
    cases.push(("query_name", query_name));
    let mut bind_slots = original.clone();
    *description_field_mut(&mut bind_slots, "/queries/0/bind_slots")? = Value::from(17);
    cases.push(("bind_slots", bind_slots));
    let mut no_queries = original.clone();
    *description_field_mut(&mut no_queries, "/queries")? = serde_json::json!([]);
    cases.push(("drop_queries", no_queries));
    let mut deleted_query = original.clone();
    description_field_mut(&mut deleted_query, "/queries")?
        .as_array_mut()
        .ok_or("missing generated query vector")?
        .remove(0);
    cases.push(("delete_query", deleted_query));
    let mut reordered_queries = original.clone();
    description_field_mut(&mut reordered_queries, "/queries")?
        .as_array_mut()
        .ok_or("missing generated query vector")?
        .swap(0, 1);
    cases.push(("reorder_queries", reordered_queries));
    let mut deleted_column = original.clone();
    description_field_mut(&mut deleted_column, "/queries/0/columns")?
        .as_array_mut()
        .ok_or("missing generated column vector")?
        .remove(0);
    cases.push(("delete_column", deleted_column));
    let mut reordered_columns = original.clone();
    description_field_mut(&mut reordered_columns, "/queries/0/columns")?
        .as_array_mut()
        .ok_or("missing generated column vector")?
        .swap(0, 1);
    cases.push(("reorder_columns", reordered_columns));
    let mut no_digest = original.clone();
    no_digest
        .as_object_mut()
        .ok_or("missing generated metadata object")?
        .remove("queries_digest");
    cases.push(("missing_digest", no_digest));
    let mut wrong_digest = original.clone();
    wrong_digest
        .as_object_mut()
        .ok_or("missing generated metadata object")?
        .insert("queries_digest".into(), Value::from("0".repeat(64)));
    cases.push(("wrong_digest", wrong_digest));
    Ok(cases)
}

#[test]
fn public_build_gate_binds_complete_ordered_descriptions_on_native_and_wasm() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let consumer = temporary.path().join("consumer");
    let target = temporary.path().join("cargo-target");
    let manifest = external_consumer(&consumer, &library_root()?)?;
    let generated = Command::new(env!("CARGO_BIN_EXE_fleet-sync-query-gen"))
        .args(["--manifest"])
        .arg(&manifest)
        .current_dir(library_root()?)
        .output()?;
    assert!(generated.status.success(), "{}", output_text(&generated));
    let metadata_path = consumer.join("checked-query-metadata.json");
    let original_bytes = fs::read(&metadata_path)?;
    let original: Value = serde_json::from_slice(&original_bytes)?;
    let output_path = consumer.join("src/checked_queries.rs");
    let output_bytes = fs::read(&output_path)?;
    let sql_path = consumer.join("sql/read.sql");
    let sql_bytes = fs::read(&sql_path)?;
    let mut violations = Vec::new();

    for (target_name, target_triple) in [("native", None), ("wasm", Some("wasm32-unknown-unknown"))]
    {
        let baseline = check_consumer_target(&consumer, &target, false, target_triple)?;
        assert!(
            baseline.status.success(),
            "{target_name} baseline: {}",
            output_text(&baseline)
        );
        println!("OBS {target_name} generated portable consumer baseline: exit=0");

        // Raw file formatting and object-key order are not query descriptions.
        let whitespace = [b"\n  ".as_slice(), &original_bytes, b"\n\n"].concat();
        let reordered_keys = serde_json::to_vec_pretty(&original)?;
        assert_ne!(
            reordered_keys, original_bytes,
            "key-order control changed no bytes"
        );
        assert_eq!(serde_json::from_slice::<Value>(&reordered_keys)?, original);
        for (name, bytes) in [
            ("whitespace", whitespace),
            ("object_key_order", reordered_keys),
        ] {
            fs::write(&metadata_path, bytes)?;
            let result = check_consumer_target(&consumer, &target, true, target_triple);
            fs::write(&metadata_path, &original_bytes)?;
            let result = result?;
            println!(
                "OBS {target_name} positive {name}: exit={:?}",
                result.status.code()
            );
            if !result.status.success() {
                violations.push(format!("{target_name} {name}: {}", output_text(&result)));
            }
        }

        for (name, mutated) in metadata_mutants(&original)? {
            // Only metadata changes. Neither generated Rust nor SQL is edited.
            if name != "missing_digest" {
                assert_ne!(mutated, original, "vacuous metadata mutation {name}");
            }
            fs::write(&metadata_path, serde_json::to_vec_pretty(&mutated)?)?;
            assert_eq!(fs::read(&output_path)?, output_bytes);
            assert_eq!(fs::read(&sql_path)?, sql_bytes);
            let result = check_consumer_target(&consumer, &target, true, target_triple);
            fs::write(&metadata_path, &original_bytes)?;
            let result = result?;
            let text = output_text(&result);
            println!(
                "OBS {target_name} metadata-only {name}: exit={:?}",
                result.status.code()
            );
            if result.status.success() {
                violations.push(format!("{target_name} accepted metadata-only {name}"));
            } else if name == "missing_digest" {
                if !text.contains("missing field `queries_digest`") {
                    violations.push(format!(
                        "{target_name} missing digest failed outside the required field: {text}"
                    ));
                }
            } else if !text.contains("stale checked query input checked-query-metadata.json;") {
                violations.push(format!(
                    "{target_name} {name} failed outside metadata freshness: {text}"
                ));
            }
        }

        for (path, original, suffix, key) in [
            (&sql_path, &sql_bytes, "\n-- changed SQL\n", "sql/read.sql"),
            (
                &output_path,
                &output_bytes,
                "\n// changed output\n",
                "src/checked_queries.rs",
            ),
        ] {
            let mut changed = original.to_vec();
            changed.extend_from_slice(suffix.as_bytes());
            fs::write(path, changed)?;
            let result = check_consumer_target(&consumer, &target, true, target_triple);
            fs::write(path, original)?;
            let result = result?;
            let text = output_text(&result);
            println!(
                "OBS {target_name} existing source/output {key}: exit={:?}",
                result.status.code()
            );
            if result.status.success()
                || !text.contains(&format!("stale checked query input {key};"))
            {
                violations.push(format!("{target_name} existing control {key}: {text}"));
            }
        }
        let recovered = check_consumer_target(&consumer, &target, true, target_triple)?;
        assert!(
            recovered.status.success(),
            "{target_name} restored build: {}",
            output_text(&recovered)
        );
        println!("OBS {target_name} exact restored consumer: exit=0");
    }
    assert!(
        violations.is_empty(),
        "public build gate violations:\n{}",
        violations.join("\n")
    );
    Ok(())
}
