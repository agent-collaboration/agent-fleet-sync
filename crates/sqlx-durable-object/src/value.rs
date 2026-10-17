use std::borrow::Cow;

use sqlx_core::value::{Value, ValueRef};
use sqlx_durable_object_core::{AsValue, Value as CoreValue};

use crate::{Do, DoTypeInfo};

/// A value read out of a Durable Object storage result, already converted from
/// JavaScript.
///
/// Owned, because every row is read out of the cursor when the query runs, and
/// there is nothing on the Rust side for a row to borrow from.
///
/// It wraps [`DoArgumentValue`](crate::DoArgumentValue): a value read and
/// a value bound are the same enum, as in sqlx's `Any` driver.
#[derive(Debug, Clone, PartialEq)]
pub struct DoValue(pub(crate) CoreValue);

impl Value for DoValue {
    type Database = Do;

    fn as_ref(&self) -> DoValueRef<'_> {
        DoValueRef(&self.0)
    }

    fn type_info(&self) -> Cow<'_, DoTypeInfo> {
        Cow::Owned(self.0.type_info())
    }

    fn is_null(&self) -> bool {
        self.0.is_null()
    }
}

/// A borrowed [`DoValue`], which is what `Decode` implementations read.
#[derive(Debug, Clone, Copy)]
pub struct DoValueRef<'r>(pub(crate) &'r CoreValue);

impl<'r> AsValue<'r> for DoValueRef<'r> {
    fn as_value(&self) -> &'r CoreValue {
        self.0
    }
}

impl ValueRef<'_> for DoValueRef<'_> {
    type Database = Do;

    fn to_owned(&self) -> DoValue {
        DoValue(self.0.clone())
    }

    fn type_info(&self) -> Cow<'_, DoTypeInfo> {
        Cow::Owned(self.0.type_info())
    }

    fn is_null(&self) -> bool {
        self.0.is_null()
    }
}
