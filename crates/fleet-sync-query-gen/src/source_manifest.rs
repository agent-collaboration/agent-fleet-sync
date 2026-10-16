use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Component, Path},
};

#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid query manifest: {0}")]
    Manifest(String),
    #[error("stale checked query input {0}; regenerate the manifest")]
    Stale(String),
    #[error("SQL description failed: {0}")]
    Sql(String),
    #[error("query {query} has unsupported type {sql_type} for column {column}")]
    UnsupportedType {
        query: String,
        column: String,
        sql_type: String,
    },
    #[error("query {query} binds {actual} slots; its manifest declares {expected}")]
    BindCount {
        query: String,
        expected: usize,
        actual: usize,
    },
    #[error("generated query formatting failed: {0}")]
    Formatter(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryManifest {
    pub migrations: Vec<String>,
    pub output: String,
    pub metadata: String,
    pub queries: Vec<QuerySpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuerySpec {
    pub name: String,
    pub sql: String,
    pub row: Option<String>,
    pub result: QueryResult,
    pub parameters: Vec<ParameterSpec>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryResult {
    Many,
    Optional,
    One,
    Execute,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterSpec {
    pub name: String,
    pub kind: ParameterKind,
    pub nullable: bool,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParameterKind {
    Text,
    Integer,
    Real,
    Blob,
}
impl ParameterKind {
    pub const fn rust_type(self) -> &'static str {
        match self {
            Self::Text => "String",
            Self::Integer => "i64",
            Self::Real => "f64",
            Self::Blob => "Vec<u8>",
        }
    }
}
impl ParameterSpec {
    pub fn rust_type(&self) -> String {
        let base = self.kind.rust_type();
        if self.nullable {
            format!("Option<{base}>")
        } else {
            base.into()
        }
    }
}

pub(crate) fn read_manifest(path: &Path) -> Result<QueryManifest, GenerateError> {
    let manifest: QueryManifest = serde_json::from_slice(&fs::read(path)?)?;
    if manifest.migrations.is_empty() || manifest.queries.is_empty() {
        return Err(GenerateError::Manifest(
            "migrations and queries must not be empty".into(),
        ));
    }
    for path in manifest
        .migrations
        .iter()
        .chain([&manifest.output, &manifest.metadata])
    {
        validate_relative_path(path)?;
    }
    let mut names = std::collections::BTreeSet::new();
    for query in &manifest.queries {
        validate_identifier(&query.name)?;
        if !names.insert(&query.name) {
            return Err(GenerateError::Manifest(format!(
                "duplicate query {}",
                query.name
            )));
        }
        validate_relative_path(&query.sql)?;
        match (&query.row, query.result) {
            (Some(row), QueryResult::Many | QueryResult::One | QueryResult::Optional) => {
                validate_identifier(row)?
            }
            (None, QueryResult::Execute) => {}
            _ => {
                return Err(GenerateError::Manifest(format!(
                    "{} has an inconsistent row/result shape",
                    query.name
                )));
            }
        }
        let mut parameters = std::collections::BTreeSet::new();
        for parameter in &query.parameters {
            validate_identifier(&parameter.name)?;
            if !parameters.insert(&parameter.name) {
                return Err(GenerateError::Manifest(format!(
                    "duplicate parameter {}",
                    parameter.name
                )));
            }
        }
    }
    Ok(manifest)
}
pub(crate) fn validate_identifier(value: &str) -> Result<(), GenerateError> {
    let mut chars = value.chars();
    if !chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(GenerateError::Manifest(format!(
            "invalid Rust identifier {value}"
        )));
    }
    Ok(())
}
fn validate_relative_path(value: &str) -> Result<(), GenerateError> {
    if value.is_empty()
        || Path::new(value)
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(GenerateError::Manifest(format!(
            "expected a package-relative path: {value}"
        )));
    }
    Ok(())
}
