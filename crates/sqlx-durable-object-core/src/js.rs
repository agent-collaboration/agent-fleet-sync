//! The conversions between JavaScript values and this crate's.
//!
//! Each driver's own `js` module talks to its binding; these are the
//! conversions both need on either side of that call -- one point in each
//! direction, so the two backends cannot drift apart on what a value means.

use js_sys::{Array, ArrayBuffer, Uint8Array};
use sqlx_core::error::Error;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

use crate::{DatabaseError, Value, safe_integer};

/// `values` as a JavaScript array, in placeholder order.
///
/// # Errors
///
/// When an integer is outside ±(2^53 − 1).
pub fn to_js_array(values: &[Value]) -> Result<Array, Error> {
    values.iter().map(to_js).collect()
}

/// A bound parameter as the JavaScript value the binding expects.
///
/// # Errors
///
/// When an integer is outside ±(2^53 − 1).
pub fn to_js(value: &Value) -> Result<JsValue, Error> {
    Ok(match value {
        Value::Null => JsValue::NULL,
        // Checked again here, not only in `Encode for i64`: a third-party
        // `Encode` impl can push any `Integer` it likes.
        Value::Integer(value) => {
            #[allow(
                clippy::cast_precision_loss,
                reason = "`safe_integer` has just checked the conversion is exact"
            )]
            let value = safe_integer(*value).map_err(Error::Encode)? as f64;
            JsValue::from_f64(value)
        }
        Value::Real(value) => JsValue::from_f64(*value),
        Value::Text(value) => JsValue::from_str(value),
        // An `ArrayBuffer`, which both backends store as BLOB. A fresh
        // `Uint8Array` owns a buffer of exactly its length.
        Value::Blob(value) => Uint8Array::from(value.as_slice()).buffer().into(),
    })
}

/// A JavaScript value from a result row, as a [`Value`].
///
/// # Errors
///
/// When the value is of a JavaScript type neither backend returns.
pub fn from_js(value: &JsValue) -> Result<Value, Error> {
    Ok(if value.is_null() || value.is_undefined() {
        Value::Null
    } else if let Some(number) = value.as_f64() {
        Value::from_number(number)
    } else if let Some(text) = value.as_string() {
        Value::Text(text)
    } else if let Some(boolean) = value.as_bool() {
        Value::Integer(i64::from(boolean))
    } else if let Some(buffer) = value.dyn_ref::<ArrayBuffer>() {
        Value::Blob(Uint8Array::new(buffer).to_vec())
    } else if let Some(bytes) = value.dyn_ref::<Uint8Array>() {
        Value::Blob(bytes.to_vec())
    } else if let Some(numbers) = value.dyn_ref::<Array>() {
        // D1 has returned BLOBs as plain arrays of byte values.
        Value::Blob(bytes_from_numbers(numbers)?)
    } else {
        return Err(Error::Decode(
            format!("a value of an unexpected JavaScript type came back: {value:?}").into(),
        ));
    })
}

fn bytes_from_numbers(numbers: &Array) -> Result<Vec<u8>, Error> {
    numbers
        .iter()
        .map(|number| {
            number
                .as_f64()
                .filter(|number| number.fract() == 0.0 && (0.0..=255.0).contains(number))
                .map(|number| {
                    #[allow(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "the filter has just checked it is a whole number in 0..=255"
                    )]
                    let byte = number as u8;
                    byte
                })
                .ok_or_else(|| Error::Decode("a BLOB element came back that is not a byte".into()))
        })
        .collect()
}

/// A [`DatabaseError`] for what the binding threw.
///
/// The thrown error's `message` is the backend's summary, and its `cause`,
/// when there is one, carries SQLite's own text -- the part the `ErrorKind`
/// mapping reads -- so both are kept.
#[must_use]
pub fn database_error(error: &JsValue) -> Error {
    let message = match error.dyn_ref::<js_sys::Error>() {
        Some(error) => {
            let message = String::from(error.message());

            match error.cause().dyn_ref::<js_sys::Error>() {
                Some(cause) => {
                    let cause = String::from(cause.message());
                    if message.contains(&cause) {
                        message
                    } else {
                        format!("{message}: {cause}")
                    }
                }
                None => message,
            }
        }
        None => error.as_string().unwrap_or_else(|| format!("{error:?}")),
    };

    Error::Database(Box::new(DatabaseError::new(message)))
}
