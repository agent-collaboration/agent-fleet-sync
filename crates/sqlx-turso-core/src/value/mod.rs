//! Owned and borrowed Turso values and their storage-class access
//!
//! Codecs live beside this module: [`primitive_codecs`] for numbers, text and bytes, and
//! `chrono_codecs` for dates and times.

#[cfg(feature = "chrono")]
mod chrono_codecs;
mod primitive_codecs;

use std::{borrow::Cow, fmt};

use sqlx_core::{
    error::BoxDynError,
    type_info::TypeInfo,
    value::{Value, ValueRef},
};

use crate::{Turso, TursoAdapterError, TursoTypeInfo};

/// SQLite storage class of a value returned by the Turso engine
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TursoStorageClass {
    /// SQL NULL
    Null,
    /// 64-bit signed integer
    Integer,
    /// 64-bit IEEE floating point
    Real,
    /// UTF-8 text
    Text,
    /// Raw bytes
    Blob,
}

impl fmt::Display for TursoStorageClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Null => "NULL",
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
        })
    }
}

/// Owned Turso value
#[derive(Clone, Debug, Default)]
pub struct TursoValue {
    type_info: TursoTypeInfo,
    kind: TursoValueKind,
}

#[derive(Clone, Debug, Default)]
enum TursoValueKind {
    #[default]
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl TursoValueKind {
    fn storage_class(&self) -> TursoStorageClass {
        match self {
            Self::Null => TursoStorageClass::Null,
            Self::Integer(_) => TursoStorageClass::Integer,
            Self::Real(_) => TursoStorageClass::Real,
            Self::Text(_) => TursoStorageClass::Text,
            Self::Blob(_) => TursoStorageClass::Blob,
        }
    }
}

impl TursoValue {
    /// Creates a NULL value
    pub fn null() -> Self {
        Self {
            type_info: TursoTypeInfo::NULL,
            kind: TursoValueKind::Null,
        }
    }

    /// Returns the storage class of this value
    pub fn storage_class(&self) -> TursoStorageClass {
        self.kind.storage_class()
    }

    pub(crate) fn from_turso(value: turso::Value) -> Self {
        match value {
            turso::Value::Null => Self::null(),
            turso::Value::Integer(value) => Self::integer(value),
            turso::Value::Real(value) => Self::real(value),
            turso::Value::Text(value) => Self::text(value),
            turso::Value::Blob(value) => Self::blob(value),
        }
    }

    pub(crate) fn with_type_info(mut self, type_info: TursoTypeInfo) -> Self {
        if !type_info.is_null() {
            self.type_info = type_info;
        }

        self
    }

    pub(crate) fn integer(value: i64) -> Self {
        Self {
            type_info: TursoTypeInfo::new("INTEGER"),
            kind: TursoValueKind::Integer(value),
        }
    }

    pub(crate) fn real(value: f64) -> Self {
        Self {
            type_info: TursoTypeInfo::new("REAL"),
            kind: TursoValueKind::Real(value),
        }
    }

    pub(crate) fn text(value: impl Into<String>) -> Self {
        Self {
            type_info: TursoTypeInfo::new("TEXT"),
            kind: TursoValueKind::Text(value.into()),
        }
    }

    pub(crate) fn blob(value: impl Into<Vec<u8>>) -> Self {
        Self {
            type_info: TursoTypeInfo::new("BLOB"),
            kind: TursoValueKind::Blob(value.into()),
        }
    }

    pub(crate) fn into_turso(self) -> turso::Value {
        match self.kind {
            TursoValueKind::Null => turso::Value::Null,
            TursoValueKind::Integer(value) => turso::Value::Integer(value),
            TursoValueKind::Real(value) => turso::Value::Real(value),
            TursoValueKind::Text(value) => turso::Value::Text(value),
            TursoValueKind::Blob(value) => turso::Value::Blob(value),
        }
    }
}

impl Value for TursoValue {
    type Database = Turso;

    fn as_ref(&self) -> TursoValueRef<'_> {
        TursoValueRef::new(self)
    }

    fn type_info(&self) -> Cow<'_, TursoTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    fn is_null(&self) -> bool {
        matches!(self.kind, TursoValueKind::Null)
    }
}

/// Borrowed Turso value
#[derive(Clone, Copy, Debug)]
pub struct TursoValueRef<'r> {
    value: &'r TursoValue,
}

impl<'r> TursoValueRef<'r> {
    pub(crate) fn new(value: &'r TursoValue) -> Self {
        Self { value }
    }

    fn mismatch(&self, expected: &'static str) -> BoxDynError {
        Box::new(TursoAdapterError::StorageClassMismatch {
            expected,
            actual: self.value.storage_class(),
        })
    }

    fn integer(&self) -> Result<i64, BoxDynError> {
        match &self.value.kind {
            TursoValueKind::Integer(value) => Ok(*value),
            _ => Err(self.mismatch("an integer")),
        }
    }

    fn real(&self) -> Result<f64, BoxDynError> {
        match &self.value.kind {
            TursoValueKind::Real(value) => Ok(*value),
            // SQLite numeric affinity reads an INTEGER as REAL the same lossy way.
            TursoValueKind::Integer(value) => Ok(*value as f64),
            _ => Err(self.mismatch("a float")),
        }
    }

    fn text(&self) -> Result<&'r str, BoxDynError> {
        match &self.value.kind {
            TursoValueKind::Text(value) => Ok(value),
            _ => Err(self.mismatch("text")),
        }
    }

    fn blob(&self) -> Result<&'r [u8], BoxDynError> {
        match &self.value.kind {
            TursoValueKind::Blob(value) => Ok(value),
            _ => Err(self.mismatch("bytes")),
        }
    }

    #[cfg(feature = "chrono")]
    fn temporal(&self) -> Result<TursoTemporalValue<'r>, BoxDynError> {
        match &self.value.kind {
            TursoValueKind::Text(value) => Ok(TursoTemporalValue::Text(value)),
            TursoValueKind::Integer(value) => Ok(TursoTemporalValue::Integer(*value)),
            TursoValueKind::Real(value) => Ok(TursoTemporalValue::Real(*value)),
            TursoValueKind::Null | TursoValueKind::Blob(_) => Err(self.mismatch("a date or time")),
        }
    }
}

/// The storage classes a SQLite date or time can be stored as
#[cfg(feature = "chrono")]
enum TursoTemporalValue<'r> {
    Text(&'r str),
    Integer(i64),
    Real(f64),
}

impl<'r> ValueRef<'r> for TursoValueRef<'r> {
    type Database = Turso;

    fn to_owned(&self) -> TursoValue {
        self.value.clone()
    }

    fn type_info(&self) -> Cow<'_, TursoTypeInfo> {
        self.value.type_info()
    }

    fn is_null(&self) -> bool {
        self.value.is_null()
    }
}
