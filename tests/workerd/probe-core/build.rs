#![forbid(unsafe_code)]
use sha2::{Digest, Sha256};
use std::{error::Error, fs};
fn main() -> Result<(), Box<dyn Error>> {
    let metadata: serde_json::Value =
        serde_json::from_str(&fs::read_to_string("../query-metadata.json")?)?;
    println!("cargo:rerun-if-changed=../query-metadata.json");
    for (path, field) in [
        ("../schema.sql", "schema_sha256"),
        ("../queries/source.sql", "query_sha256"),
        ("src/checked_source.rs", "generated_sha256"),
    ] {
        println!("cargo:rerun-if-changed={path}");
        let digest = format!("{:x}", Sha256::digest(fs::read(path)?));
        if metadata.get(field).and_then(serde_json::Value::as_str) != Some(digest.as_str()) {
            return Err(
                format!("stale checked query: {path}; regenerate with probe-query-gen").into(),
            );
        }
    }
    Ok(())
}
