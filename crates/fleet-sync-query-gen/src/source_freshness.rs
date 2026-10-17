use crate::{GenerateError, QueryManifest, source_manifest::read_manifest};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

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
    pub queries: Vec<QueryMetadata>,
}

pub(crate) fn digest_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(crate) fn digest_file(path: &Path) -> Result<String, GenerateError> {
    Ok(digest_bytes(&fs::read(path)?))
}

/// Called by a consumer's build.rs. It checks SQL, schema, generator, drivers,
/// toolchain, lockfile and generated output without a runtime or regeneration bypass.
pub fn verify_manifest(path: impl AsRef<Path>) -> Result<(), GenerateError> {
    let path = path.as_ref();
    let root = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let manifest = read_manifest(path)?;
    println!("cargo:rerun-if-changed={}", path.display());
    println!(
        "cargo:rerun-if-changed={}",
        root.join(&manifest.metadata).display()
    );
    let metadata: CorpusMetadata =
        serde_json::from_slice(&fs::read(root.join(&manifest.metadata))?)?;
    let mut current = input_hashes(path, &manifest)?;
    current.insert(
        manifest.output.clone(),
        digest_file(&root.join(&manifest.output))?,
    );
    for relative in current.keys().chain(metadata.sources.keys()) {
        println!("cargo:rerun-if-changed={}", root.join(relative).display());
        if current.get(relative) != metadata.sources.get(relative) {
            return Err(GenerateError::Stale(relative.clone()));
        }
    }
    Ok(())
}

pub(crate) fn input_hashes(
    path: &Path,
    manifest: &QueryManifest,
) -> Result<BTreeMap<String, String>, GenerateError> {
    let root = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .canonicalize()?;
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| GenerateError::Manifest("generator workspace root missing".into()))?;
    let mut paths = vec![
        path.canonicalize()?,
        workspace.join("Cargo.lock"),
        workspace.join("rust-toolchain.toml"),
    ];
    for directory in &manifest.migrations {
        println!("cargo:rerun-if-changed={}", root.join(directory).display());
        collect_files(&root.join(directory), &mut paths)?;
    }
    for query in &manifest.queries {
        paths.push(root.join(&query.sql));
    }
    for package in [
        "fleet-sync-query-gen",
        "sqlx-turso",
        "sqlx-turso-core",
        "sqlx-durable-object-core",
    ] {
        let package = workspace.join("crates").join(package);
        println!("cargo:rerun-if-changed={}", package.join("src").display());
        paths.push(package.join("Cargo.toml"));
        collect_files(&package.join("src"), &mut paths)?;
    }
    let mut hashes = BTreeMap::new();
    for path in paths {
        hashes.insert(
            relative_path(&root, &path.canonicalize()?)?,
            digest_file(&path)?,
        );
    }
    Ok(hashes)
}

fn relative_path(root: &Path, target: &Path) -> Result<String, GenerateError> {
    let root_components: Vec<_> = root.components().collect();
    let target_components: Vec<_> = target.components().collect();
    let shared = root_components
        .iter()
        .zip(&target_components)
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = PathBuf::new();
    for _ in root_components.iter().skip(shared) {
        relative.push("..");
    }
    for component in target_components.iter().skip(shared) {
        relative.push(component.as_os_str());
    }
    relative
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| GenerateError::Manifest("query source path is not UTF-8".into()))
}

fn collect_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<(), GenerateError> {
    let mut entries = fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_files(&path, paths)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "rs" || extension == "sql")
            || path.file_name().is_some_and(|name| name == "Cargo.toml")
        {
            paths.push(path);
        }
    }
    Ok(())
}
