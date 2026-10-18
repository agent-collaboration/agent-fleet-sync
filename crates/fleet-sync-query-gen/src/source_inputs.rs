use crate::{GenerateError, QueryManifest};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputOrigin {
    Consumer,
    GeneratorPackage(&'static str),
    GeneratorWorkspace,
    GeneratedOutput,
}

struct SourceInput {
    path: PathBuf,
    origin: InputOrigin,
    digest: String,
}

/// Metadata keys identify inputs; physical paths identify reads and Cargo watches.
pub(crate) struct SourceInputs {
    root: PathBuf,
    inputs: BTreeMap<String, SourceInput>,
    watches: BTreeSet<PathBuf>,
}

impl SourceInputs {
    fn new(root: &Path) -> Result<Self, GenerateError> {
        Ok(Self {
            root: root.canonicalize()?,
            inputs: BTreeMap::new(),
            watches: BTreeSet::new(),
        })
    }

    pub(crate) fn collect(path: &Path, manifest: &QueryManifest) -> Result<Self, GenerateError> {
        let root = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut inputs = Self::new(root)?;
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| GenerateError::Manifest("generator workspace root missing".into()))?
            .canonicalize()?;
        let mut consumer_paths = vec![path.canonicalize()?];
        for directory in &manifest.migrations {
            let directory = inputs.root.join(directory).canonicalize()?;
            inputs.watches.insert(directory.clone());
            collect_files(&directory, &mut consumer_paths)?;
        }
        for query in &manifest.queries {
            consumer_paths.push(inputs.root.join(&query.sql));
        }
        for path in consumer_paths {
            let path = path.canonicalize()?;
            let key = relative_path(&inputs.root, &path)?;
            inputs.insert_file(key, path, InputOrigin::Consumer)?;
        }
        for name in ["Cargo.lock", "rust-toolchain.toml"] {
            inputs.insert_file(
                format!("generator:workspace/{name}"),
                workspace.join(name),
                InputOrigin::GeneratorWorkspace,
            )?;
        }
        for package in [
            "fleet-sync-query-gen",
            "sqlx-turso",
            "sqlx-turso-core",
            "sqlx-durable-object-core",
        ] {
            let package_root = workspace.join("crates").join(package).canonicalize()?;
            let source = package_root.join("src").canonicalize()?;
            inputs.watches.insert(source.clone());
            let mut paths = vec![package_root.join("Cargo.toml")];
            collect_files(&source, &mut paths)?;
            for path in paths {
                let path = path.canonicalize()?;
                let key = format!(
                    "generator:{package}/{}",
                    relative_path(&package_root, &path)?
                );
                inputs.insert_file(key, path, InputOrigin::GeneratorPackage(package))?;
            }
        }
        Ok(inputs)
    }

    fn insert_file(
        &mut self,
        key: String,
        path: PathBuf,
        origin: InputOrigin,
    ) -> Result<(), GenerateError> {
        let path = path.canonicalize()?;
        let digest = digest_file(&path)?;
        self.insert(key, path, origin, digest)
    }

    fn insert(
        &mut self,
        key: String,
        path: PathBuf,
        origin: InputOrigin,
        digest: String,
    ) -> Result<(), GenerateError> {
        if let Some(existing) = self.inputs.get(&key) {
            if existing.path == path && existing.origin == origin {
                return Ok(());
            }
            return Err(GenerateError::Manifest(format!(
                "different checked-query sources share logical key {key}"
            )));
        }
        self.watches.insert(path.clone());
        self.inputs.insert(
            key,
            SourceInput {
                path,
                origin,
                digest,
            },
        );
        Ok(())
    }

    pub(crate) fn add_output(
        &mut self,
        relative: &str,
        digest: String,
    ) -> Result<(), GenerateError> {
        let path = self.root.join(relative);
        let path = match path.canonicalize() {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => path,
            Err(error) => return Err(error.into()),
        };
        self.insert(
            normalized_path(Path::new(relative))?,
            path,
            InputOrigin::GeneratedOutput,
            digest,
        )
    }

    pub(crate) fn emit_watches(&self) {
        for path in &self.watches {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }

    pub(crate) fn into_hashes(self) -> BTreeMap<String, String> {
        self.inputs
            .into_iter()
            .map(|(key, input)| (key, input.digest))
            .collect()
    }
}

pub(crate) fn digest_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(crate) fn digest_file(path: &Path) -> Result<String, GenerateError> {
    Ok(digest_bytes(&fs::read(path)?))
}

fn normalized_path(path: &Path) -> Result<String, GenerateError> {
    path.components()
        .map(|component| {
            component
                .as_os_str()
                .to_str()
                .ok_or_else(|| GenerateError::Manifest("query source path is not UTF-8".into()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("/"))
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
    normalized_path(&relative)
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

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;

    #[test]
    fn repeated_canonical_input_coalesces() -> Result<(), GenerateError> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("query.sql");
        fs::write(&path, b"same bytes")?;
        let mut inputs = SourceInputs::new(directory.path())?;
        inputs.insert_file("query.sql".into(), path, InputOrigin::Consumer)?;
        inputs.insert_file(
            "query.sql".into(),
            directory.path().join(".").join("query.sql"),
            InputOrigin::Consumer,
        )?;
        assert_eq!(inputs.into_hashes().len(), 1);
        Ok(())
    }

    #[test]
    fn identical_bytes_do_not_merge_distinct_physical_inputs() -> Result<(), GenerateError> {
        let directory = tempfile::tempdir()?;
        let first = directory.path().join("first.sql");
        let second = directory.path().join("second.sql");
        fs::write(&first, b"same bytes")?;
        fs::write(&second, b"same bytes")?;
        let mut inputs = SourceInputs::new(directory.path())?;
        inputs.insert_file("same-key".into(), first, InputOrigin::Consumer)?;
        assert!(matches!(
            inputs.insert_file("same-key".into(), second, InputOrigin::Consumer),
            Err(GenerateError::Manifest(_))
        ));
        Ok(())
    }

    #[test]
    fn one_physical_input_under_different_origins_does_not_coalesce() -> Result<(), GenerateError> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("input");
        fs::write(&path, b"same bytes")?;
        let mut inputs = SourceInputs::new(directory.path())?;
        inputs.insert_file("same-key".into(), path.clone(), InputOrigin::Consumer)?;
        assert!(matches!(
            inputs.insert_file("same-key".into(), path, InputOrigin::GeneratorWorkspace),
            Err(GenerateError::Manifest(_))
        ));
        Ok(())
    }
}
