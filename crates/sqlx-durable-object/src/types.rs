//! `Encode`, `Decode` and `Type` for the Rust types Durable Object storage
//! can store.
//!
//! Durable Object storage is SQLite behind a JavaScript API, like D1, and maps
//! types exactly as `sqlx-cloudflare-d1` does -- the two drivers share one
//! definition. That mapping follows sqlx-sqlite wherever JavaScript does not
//! force a difference, so code moving from `Sqlite` to `Do` behaves the same.
//! Where it does force one, it fails loudly:
//!
//! | Rust | Stored as | Notes |
//! |---|---|---|
//! | `bool` | `INTEGER` 0/1 | any non-zero integer decodes as `true` |
//! | `i8`, `i16`, `i32`, `u8`, `u16`, `u32` | `INTEGER` | decoding a value that does not fit is an error |
//! | `i64` | `INTEGER` | only within ±(2^53 − 1), in both directions |
//! | `f32`, `f64` | `REAL` | also decode from `INTEGER` |
//! | `&str`, `String`, `Box<str>`, `Arc<str>`, `Cow<str>` | `TEXT` | |
//! | `&[u8]`, `Vec<u8>`, `Box<[u8]>`, `Arc<[u8]>` | `BLOB` | also decode from `TEXT`, as its bytes |
//!
//! `u64` is deliberately missing: there is nothing it could be stored as
//! without losing most of its range.
//!
//! # Integers and JavaScript numbers
//!
//! `sql.exec` takes and returns JavaScript numbers, never `BigInt`, and a
//! number is exact only up to 2^53 − 1. So binding an `i64` outside
//! ±(2^53 − 1) is an error, not a value rounded on the way in, and a stored
//! integer beyond that range comes back as a `REAL` that will not decode as
//! `i64`.
//!
//! For the same reason, a whole number stored as `REAL` -- `3.0` -- arrives
//! indistinguishable from the integer `3`, and is typed `INTEGER`. That is why
//! the float types decode from `INTEGER` too.

sqlx_durable_object_core::impl_types!(crate::Do);

// Through `Row::try_get`, as a consumer decodes: that runs `Type::compatible`
// before `Decode`, and the two have to agree.
#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;

    use sqlx_core::arguments::Arguments;
    use sqlx_core::decode::Decode;
    use sqlx_core::row::Row;
    use sqlx_core::types::Type;

    use sqlx_durable_object_core::Value;

    use crate::{Do, DoArgumentValue, DoArguments, DoRow};

    fn get<T>(data: Value) -> Result<T, sqlx_core::error::Error>
    where
        T: for<'r> Decode<'r, Do> + Type<Do>,
    {
        let rows = DoRow::from_result(vec!["v".into()], vec![vec![data]]).unwrap();
        rows[0].try_get("v")
    }

    fn encoded<'t, T: sqlx_core::encode::Encode<'t, Do> + Type<Do>>(value: T) -> DoArgumentValue {
        let mut arguments = DoArguments::default();
        arguments.add(value).unwrap();
        arguments.values[0].clone()
    }

    #[test]
    fn a_narrow_integer_that_does_not_fit_is_an_error() {
        assert_eq!(get::<i32>(Value::Integer(5)).unwrap(), 5);
        assert!(get::<i8>(Value::Integer(300)).is_err());
        assert!(get::<u8>(Value::Integer(256)).is_err());
        assert!(get::<u32>(Value::Integer(-1)).is_err());
        assert_eq!(get::<u32>(Value::Integer(4_294_967_295)).unwrap(), u32::MAX);
    }

    #[test]
    fn any_non_zero_integer_is_true() {
        assert!(!get::<bool>(Value::Integer(0)).unwrap());
        assert!(get::<bool>(Value::Integer(1)).unwrap());
        assert!(get::<bool>(Value::Integer(2)).unwrap());
        assert!(get::<bool>(Value::Integer(-1)).unwrap());
        assert!(get::<bool>(Value::Text("true".into())).is_err());
    }

    #[test]
    fn floats_decode_from_integers_too() {
        assert!((get::<f32>(Value::Integer(3)).unwrap() - 3.0).abs() < f32::EPSILON);
        assert!((get::<f64>(Value::Real(0.25)).unwrap() - 0.25).abs() < f64::EPSILON);
        assert!(get::<f64>(Value::Text("1.5".into())).is_err());
    }

    #[test]
    fn bytes_decode_from_text_but_text_not_from_bytes() {
        assert_eq!(get::<Vec<u8>>(Value::Text("ab".into())).unwrap(), b"ab");
        assert_eq!(get::<Vec<u8>>(Value::Blob(vec![0, 255])).unwrap(), [0, 255]);
        assert!(get::<String>(Value::Blob(b"ab".to_vec())).is_err());
    }

    #[test]
    fn nothing_converts_between_numbers_and_text() {
        assert!(get::<String>(Value::Integer(1)).is_err());
        assert!(get::<i64>(Value::Text("1".into())).is_err());
        assert!(get::<i64>(Value::Real(1.5)).is_err());
    }

    #[test]
    fn null_decodes_only_into_an_option() {
        assert_eq!(get::<Option<String>>(Value::Null).unwrap(), None);
        assert!(get::<String>(Value::Null).is_err());
        assert!(get::<i64>(Value::Null).is_err());
    }

    #[test]
    fn every_text_and_byte_container_encodes_the_same() {
        let text = DoArgumentValue::Text("x".into());
        assert_eq!(encoded("x"), text);
        assert_eq!(encoded(String::from("x")), text);
        assert_eq!(encoded(Box::<str>::from("x")), text);
        assert_eq!(encoded(Arc::<str>::from("x")), text);
        assert_eq!(encoded(Cow::Borrowed("x")), text);

        let blob = DoArgumentValue::Blob(vec![1, 2]);
        assert_eq!(encoded(&[1_u8, 2][..]), blob);
        assert_eq!(encoded(vec![1_u8, 2]), blob);
        assert_eq!(encoded(Box::<[u8]>::from([1_u8, 2])), blob);
        assert_eq!(encoded(Arc::<[u8]>::from([1_u8, 2])), blob);
    }

    #[test]
    fn narrow_numbers_encode_widened() {
        assert_eq!(encoded(-7_i8), DoArgumentValue::Integer(-7));
        assert_eq!(encoded(u32::MAX), DoArgumentValue::Integer(4_294_967_295));
        assert_eq!(encoded(false), DoArgumentValue::Integer(0));
        assert_eq!(encoded(0.5_f32), DoArgumentValue::Real(0.5));
    }
}
