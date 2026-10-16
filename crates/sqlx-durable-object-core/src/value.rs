use std::fmt::{self, Display, Formatter};

use sqlx_core::error::BoxDynError;

/// The storage class of a single value, which is all these backends report.
///
/// There is no declared column type to go on: D1 and Durable Object storage
/// hand back JavaScript values, not SQLite's column metadata. So the type is
/// read off each value as it arrives, and a column's type is the type of
/// whatever is in it.
///
/// Non-exhaustive, like [`Value`]: a new storage class must not break a
/// `match` outside this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TypeInfo {
    /// A `NULL` value, or a column with no non-`NULL` value to type it by.
    Null,
    /// A whole number within ±(2^53 − 1).
    Integer,
    /// Any other number.
    Real,
    /// A string.
    Text,
    /// Bytes.
    Blob,
}

impl Display for TypeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad(sqlx_core::type_info::TypeInfo::name(self))
    }
}

impl sqlx_core::type_info::TypeInfo for TypeInfo {
    fn is_null(&self) -> bool {
        matches!(self, TypeInfo::Null)
    }

    fn name(&self) -> &str {
        match self {
            TypeInfo::Null => "NULL",
            TypeInfo::Integer => "INTEGER",
            TypeInfo::Real => "REAL",
            TypeInfo::Text => "TEXT",
            TypeInfo::Blob => "BLOB",
        }
    }
}

/// One SQLite value on the Rust side of the JavaScript boundary, in either
/// direction: a parameter a query binds, or a value a result row holds.
///
/// One type for both, as sqlx's `Any` driver has, because both directions
/// are the same thing here -- plain values that cross as JavaScript ones.
/// sqlx's built-in drivers split them only because what they read is a C
/// handle or wire bytes, decoded lazily.
///
/// Owned rather than borrowed: sqlx 0.9's `Arguments` has no lifetime, and the
/// backends return a whole result set as JavaScript values, leaving nothing on
/// the Rust side for a row to borrow from.
///
/// Non-exhaustive so that a backend growing a representation -- a `BigInt`,
/// say -- is not a breaking change for `Encode` impls outside this crate.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Value {
    /// SQL `NULL`: JavaScript `null`.
    Null,
    /// A whole number, within ±(2^53 − 1) because it crosses as a JavaScript
    /// number and not a `BigInt`. A result only holds integers in that range;
    /// binding a wider one is an encode error rather than a rounded value.
    Integer(i64),
    /// Any other number, stored as `REAL`.
    Real(f64),
    /// A JavaScript string, stored as `TEXT`.
    Text(String),
    /// An `ArrayBuffer`, stored as `BLOB`.
    Blob(Vec<u8>),
}

/// The largest integer a JavaScript number holds exactly: 2^53 − 1.
/// `Number.MAX_SAFE_INTEGER`, in other words.
pub const MAX_SAFE_INTEGER: i64 = (1 << 53) - 1;

/// `value`, if a JavaScript number can carry it without rounding.
///
/// Both backends take and return JavaScript numbers, never `BigInt`, so an
/// integer outside ±(2^53 − 1) would come back as a different integer. That
/// is an error here rather than a silently wrong row.
///
/// # Errors
///
/// When `value` is outside ±(2^53 − 1).
pub fn safe_integer(value: i64) -> Result<i64, BoxDynError> {
    if (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&value) {
        Ok(value)
    } else {
        Err(format!(
            "{value} is outside ±(2^53 − 1), the integers a JavaScript number holds exactly: \
             it would be stored as a different integer"
        )
        .into())
    }
}

impl Value {
    /// A JavaScript number, as the storage class it most likely came from.
    ///
    /// JavaScript has one number type, so `3` and `3.0` arrive the same. A
    /// whole number in the safe range is taken as an `Integer` -- which is
    /// why `f64` decodes from `Integer` as well as `Real`. Anything else,
    /// including whole numbers too large to be exact, is a `Real`.
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        reason = "the range check makes both casts exact"
    )]
    pub fn from_number(value: f64) -> Self {
        if value.fract() == 0.0 && value.abs() <= MAX_SAFE_INTEGER as f64 {
            Value::Integer(value as i64)
        } else {
            Value::Real(value)
        }
    }

    /// The value's storage class.
    #[must_use]
    pub fn type_info(&self) -> TypeInfo {
        match self {
            Value::Null => TypeInfo::Null,
            Value::Integer(_) => TypeInfo::Integer,
            Value::Real(_) => TypeInfo::Real,
            Value::Text(_) => TypeInfo::Text,
            Value::Blob(_) => TypeInfo::Blob,
        }
    }

    /// Whether the value is `NULL`.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    // The accessors `Decode` implementations use. Each accepts exactly the
    // storage classes the matching `Type::compatible` does, so `try_get` and
    // `try_get_unchecked` agree about what decodes.

    /// An `INTEGER`.
    ///
    /// # Errors
    ///
    /// For any other storage class.
    pub fn int(&self) -> Result<i64, BoxDynError> {
        match *self {
            Value::Integer(value) => Ok(value),
            ref other => Err(mismatch("an INTEGER", other)),
        }
    }

    /// A `REAL`, or an `INTEGER` as one.
    ///
    /// # Errors
    ///
    /// For any other storage class.
    #[allow(
        clippy::cast_precision_loss,
        reason = "an INTEGER here is within ±(2^53 − 1), so it converts exactly"
    )]
    pub fn real(&self) -> Result<f64, BoxDynError> {
        match *self {
            Value::Real(value) => Ok(value),
            Value::Integer(value) => Ok(value as f64),
            ref other => Err(mismatch("a REAL", other)),
        }
    }

    /// `TEXT`.
    ///
    /// # Errors
    ///
    /// For any other storage class.
    pub fn text(&self) -> Result<&str, BoxDynError> {
        match self {
            Value::Text(value) => Ok(value),
            other => Err(mismatch("TEXT", other)),
        }
    }

    /// A `BLOB`, or `TEXT` as its bytes.
    ///
    /// # Errors
    ///
    /// For any other storage class.
    pub fn blob(&self) -> Result<&[u8], BoxDynError> {
        match self {
            Value::Blob(value) => Ok(value),
            // As sqlx-sqlite does: SQLite hands TEXT to `sqlite3_column_blob`
            // as its bytes.
            Value::Text(value) => Ok(value.as_bytes()),
            other => Err(mismatch("a BLOB", other)),
        }
    }
}

fn mismatch(expected: &str, found: &Value) -> BoxDynError {
    format!("expected {expected}, found {}", found.type_info()).into()
}

/// The [`Value`] behind a driver's `ValueRef`, which is what the `Decode`
/// impls [`impl_types!`](crate::impl_types) writes read from.
pub trait AsValue<'r> {
    /// The value this reference points at.
    fn as_value(&self) -> &'r Value;
}

#[cfg(test)]
mod tests {
    use super::{MAX_SAFE_INTEGER, Value, safe_integer};

    #[test]
    fn whole_numbers_in_the_safe_range_are_integers() {
        assert_eq!(Value::from_number(3.0), Value::Integer(3));
        assert_eq!(Value::from_number(-0.0), Value::Integer(0));
        assert_eq!(
            Value::from_number(9_007_199_254_740_991.0),
            Value::Integer(MAX_SAFE_INTEGER)
        );
    }

    #[test]
    fn everything_else_is_real() {
        assert_eq!(Value::from_number(1.5), Value::Real(1.5));
        // 2^53: whole, but no longer exact -- 2^53 + 1 would read the same.
        assert_eq!(
            Value::from_number(9_007_199_254_740_992.0),
            Value::Real(9_007_199_254_740_992.0)
        );
        assert!(matches!(Value::from_number(f64::INFINITY), Value::Real(_)));
        assert!(matches!(Value::from_number(f64::NAN), Value::Real(_)));
    }

    #[test]
    fn safe_integer_accepts_exactly_the_safe_range() {
        assert!(safe_integer(MAX_SAFE_INTEGER).is_ok());
        assert!(safe_integer(-MAX_SAFE_INTEGER).is_ok());
        assert!(safe_integer(MAX_SAFE_INTEGER + 1).is_err());
        assert!(safe_integer(-MAX_SAFE_INTEGER - 1).is_err());
        assert!(safe_integer(i64::MIN).is_err());
    }

    #[test]
    fn accessors_accept_what_their_types_are_compatible_with() {
        assert_eq!(Value::Integer(2).int().unwrap(), 2);
        assert!(Value::Real(2.0).int().is_err());
        assert!((Value::Integer(2).real().unwrap() - 2.0).abs() < f64::EPSILON);
        assert_eq!(Value::Text("ab".into()).blob().unwrap(), b"ab");
        assert!(Value::Blob(b"ab".to_vec()).text().is_err());
        assert!(Value::Null.int().is_err());
    }
}
