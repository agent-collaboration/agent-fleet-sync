use crate::{
    GenerateError,
    source_inputs::{SourceInputs, digest_bytes, digest_file},
    source_manifest::read_manifest,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, path::Path};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnMetadata {
    pub name: String,
    pub sql_type: String,
    pub rust_type: String,
    pub nullable: Option<bool>,
    pub type_source: ColumnTypeSource,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ColumnTypeSource {
    Driver,
    Typeof,
    Cast,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryMetadata {
    pub name: String,
    pub bind_slots: usize,
    pub columns: Vec<ColumnMetadata>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorpusMetadata {
    pub producer: String,
    pub sources: BTreeMap<String, String>,
    pub queries_digest: String,
    pub queries: Vec<QueryMetadata>,
}

pub(crate) fn digest_query_descriptions(
    queries: &[QueryMetadata],
) -> Result<String, GenerateError> {
    Ok(digest_bytes(&serde_json::to_vec(queries)?))
}

/// Called by a consumer's build.rs. It checks SQL, schema, generator, drivers,
/// toolchain, lockfile and generated output without a runtime or regeneration bypass.
pub fn verify_manifest(path: impl AsRef<Path>) -> Result<(), GenerateError> {
    let path = path.as_ref();
    let root = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .canonicalize()?;
    let manifest = read_manifest(path)?;
    println!("cargo:rerun-if-changed={}", path.display());
    println!(
        "cargo:rerun-if-changed={}",
        root.join(&manifest.metadata).display()
    );
    let metadata: CorpusMetadata =
        serde_json::from_slice(&fs::read(root.join(&manifest.metadata))?)?;
    let mut inputs = SourceInputs::collect(path, &manifest)?;
    inputs.add_output(&manifest.output, digest_file(&root.join(&manifest.output))?)?;
    inputs.emit_watches();
    let current = inputs.into_hashes();
    for key in current.keys().chain(metadata.sources.keys()) {
        if current.get(key) != metadata.sources.get(key) {
            return Err(GenerateError::Stale(key.clone()));
        }
    }
    if digest_query_descriptions(&metadata.queries)? != metadata.queries_digest {
        return Err(GenerateError::Stale(manifest.metadata));
    }
    Ok(())
}
