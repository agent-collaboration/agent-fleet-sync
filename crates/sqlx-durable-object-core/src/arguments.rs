use sqlx_core::database::Database;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;

use crate::Value;

/// Encodes `value` onto `values`, as one placeholder's worth.
///
/// What a driver's `Arguments::add` does: whatever the encoder does, exactly
/// one value lands for the placeholder, or none if it fails.
///
/// # Errors
///
/// When encoding fails, in which case `values` is left as it was.
pub fn add_argument<'t, DB, T>(values: &mut Vec<Value>, value: T) -> Result<(), BoxDynError>
where
    DB: Database<ArgumentBuffer = Vec<Value>>,
    T: Encode<'t, DB> + Type<DB>,
{
    let len = values.len();

    match value.encode(values) {
        Ok(IsNull::Yes) => {
            // An encoder that reports NULL may still have pushed something;
            // the placeholder must line up with exactly one value.
            values.truncate(len);
            values.push(Value::Null);
        }
        Ok(IsNull::No) => {}
        Err(error) => {
            // Leave the arguments as they were, so a failed bind cannot shift
            // every later parameter one placeholder to the right.
            values.truncate(len);
            return Err(error);
        }
    }

    Ok(())
}
