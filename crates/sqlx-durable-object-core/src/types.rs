/// Writes `Type`, `Encode` and `Decode` for the Rust types these backends can
/// store, for the driver's database type `$db`.
///
/// A macro rather than impls here because the orphan rule leaves no choice:
/// `impl Type<D1> for i64` names two foreign types unless it is written in
/// the crate that defines `D1`. Expanding it there, from one definition, is
/// what keeps the drivers agreeing on the mapping.
///
/// `$db` must use [`TypeInfo`](crate::TypeInfo) as its `TypeInfo`,
/// `Vec<`[`Value`](crate::Value)`>` as its `ArgumentBuffer`,
/// and a `ValueRef` implementing [`AsValue`](crate::AsValue).
///
/// The mapping follows sqlx-sqlite wherever JavaScript does not force a
/// difference:
///
/// | Rust | Stored as | Notes |
/// |---|---|---|
/// | `bool` | `INTEGER` 0/1 | any non-zero integer decodes as `true` |
/// | `i8`, `i16`, `i32`, `u8`, `u16`, `u32` | `INTEGER` | decoding a value that does not fit is an error |
/// | `i64` | `INTEGER` | only within ±(2^53 − 1), in both directions |
/// | `f32`, `f64` | `REAL` | also decode from `INTEGER` |
/// | `&str`, `String`, `Box<str>`, `Arc<str>`, `Cow<str>` | `TEXT` | |
/// | `&[u8]`, `Vec<u8>`, `Box<[u8]>`, `Arc<[u8]>` | `BLOB` | also decode from `TEXT`, as its bytes |
#[macro_export]
macro_rules! impl_types {
    ($db:ty) => {
        const _: () = {
            use ::std::borrow::Cow;
            use ::std::sync::Arc;

            use $crate::__sqlx_core::decode::Decode;
            use $crate::__sqlx_core::encode::{Encode, IsNull};
            use $crate::__sqlx_core::error::BoxDynError;
            use $crate::__sqlx_core::types::Type;
            use $crate::{AsValue, TypeInfo, Value};

            type ValueRef<'r> = <$db as $crate::__sqlx_core::database::Database>::ValueRef<'r>;

            // SQLite has no boolean storage class; `true` is stored as 1. A
            // JavaScript `boolean` coming back is converted to 0/1 before it
            // gets here.
            impl Type<$db> for bool {
                fn type_info() -> TypeInfo {
                    TypeInfo::Integer
                }
            }

            impl Encode<'_, $db> for bool {
                fn encode_by_ref(&self, buf: &mut Vec<Value>) -> Result<IsNull, BoxDynError> {
                    buf.push(Value::Integer(i64::from(*self)));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, $db> for bool {
                // Any non-zero integer, as SQLite and sqlx-sqlite read it.
                fn decode(value: ValueRef<'r>) -> Result<Self, BoxDynError> {
                    Ok(value.as_value().int()? != 0)
                }
            }

            // Every integer narrower than `i64` fits a JavaScript number
            // exactly, so these only need the range check on the way out.
            $crate::__impl_narrow_int!($db; i8, i16, i32, u8, u16, u32);

            impl Type<$db> for i64 {
                fn type_info() -> TypeInfo {
                    TypeInfo::Integer
                }
            }

            impl Encode<'_, $db> for i64 {
                fn encode_by_ref(&self, buf: &mut Vec<Value>) -> Result<IsNull, BoxDynError> {
                    buf.push(Value::Integer($crate::safe_integer(*self)?));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, $db> for i64 {
                // No range check needed here: an `Integer` value only ever
                // comes from a number already classified as exact (see
                // `Value::from_number`).
                fn decode(value: ValueRef<'r>) -> Result<Self, BoxDynError> {
                    value.as_value().int()
                }
            }

            impl Type<$db> for f64 {
                fn type_info() -> TypeInfo {
                    TypeInfo::Real
                }

                // `INTEGER` too: a whole number stored as REAL comes back
                // typed INTEGER, because JavaScript cannot tell `3.0` from `3`.
                fn compatible(ty: &TypeInfo) -> bool {
                    matches!(ty, TypeInfo::Real | TypeInfo::Integer)
                }
            }

            impl Encode<'_, $db> for f64 {
                fn encode_by_ref(&self, buf: &mut Vec<Value>) -> Result<IsNull, BoxDynError> {
                    buf.push(Value::Real(*self));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, $db> for f64 {
                fn decode(value: ValueRef<'r>) -> Result<Self, BoxDynError> {
                    value.as_value().real()
                }
            }

            impl Type<$db> for f32 {
                fn type_info() -> TypeInfo {
                    TypeInfo::Real
                }

                fn compatible(ty: &TypeInfo) -> bool {
                    <f64 as Type<$db>>::compatible(ty)
                }
            }

            impl Encode<'_, $db> for f32 {
                fn encode_by_ref(&self, buf: &mut Vec<Value>) -> Result<IsNull, BoxDynError> {
                    buf.push(Value::Real(f64::from(*self)));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, $db> for f32 {
                #[allow(
                    clippy::cast_possible_truncation,
                    reason = "narrowing a stored REAL to f32 is what asking for an f32 means; sqlx-sqlite does the same"
                )]
                fn decode(value: ValueRef<'r>) -> Result<Self, BoxDynError> {
                    Ok(value.as_value().real()? as f32)
                }
            }

            impl Type<$db> for str {
                fn type_info() -> TypeInfo {
                    TypeInfo::Text
                }
            }

            impl Encode<'_, $db> for &'_ str {
                fn encode_by_ref(&self, buf: &mut Vec<Value>) -> Result<IsNull, BoxDynError> {
                    buf.push(Value::Text((*self).to_owned()));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, $db> for &'r str {
                fn decode(value: ValueRef<'r>) -> Result<Self, BoxDynError> {
                    value.as_value().text()
                }
            }

            impl Type<$db> for String {
                fn type_info() -> TypeInfo {
                    <str as Type<$db>>::type_info()
                }
            }

            impl Encode<'_, $db> for String {
                fn encode(self, buf: &mut Vec<Value>) -> Result<IsNull, BoxDynError> {
                    buf.push(Value::Text(self));
                    Ok(IsNull::No)
                }

                fn encode_by_ref(&self, buf: &mut Vec<Value>) -> Result<IsNull, BoxDynError> {
                    buf.push(Value::Text(self.clone()));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, $db> for String {
                fn decode(value: ValueRef<'r>) -> Result<Self, BoxDynError> {
                    value.as_value().text().map(ToOwned::to_owned)
                }
            }

            impl Type<$db> for [u8] {
                fn type_info() -> TypeInfo {
                    TypeInfo::Blob
                }

                // `TEXT` as well, read as its bytes -- sqlx-sqlite accepts the
                // same.
                fn compatible(ty: &TypeInfo) -> bool {
                    matches!(ty, TypeInfo::Blob | TypeInfo::Text)
                }
            }

            impl Encode<'_, $db> for &'_ [u8] {
                fn encode_by_ref(&self, buf: &mut Vec<Value>) -> Result<IsNull, BoxDynError> {
                    buf.push(Value::Blob(self.to_vec()));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, $db> for &'r [u8] {
                fn decode(value: ValueRef<'r>) -> Result<Self, BoxDynError> {
                    value.as_value().blob()
                }
            }

            impl Type<$db> for Vec<u8> {
                fn type_info() -> TypeInfo {
                    <[u8] as Type<$db>>::type_info()
                }

                fn compatible(ty: &TypeInfo) -> bool {
                    <[u8] as Type<$db>>::compatible(ty)
                }
            }

            impl Encode<'_, $db> for Vec<u8> {
                fn encode(self, buf: &mut Vec<Value>) -> Result<IsNull, BoxDynError> {
                    buf.push(Value::Blob(self));
                    Ok(IsNull::No)
                }

                fn encode_by_ref(&self, buf: &mut Vec<Value>) -> Result<IsNull, BoxDynError> {
                    buf.push(Value::Blob(self.clone()));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, $db> for Vec<u8> {
                fn decode(value: ValueRef<'r>) -> Result<Self, BoxDynError> {
                    value.as_value().blob().map(<[u8]>::to_vec)
                }
            }

            // sqlx-core's smart-pointer impls cover `Box<T>` and friends only
            // for sized `T`, so the unsized ones each need their own -- as
            // sqlx-sqlite has.
            $crate::__impl_encode_via_ref!($db; &str => Box<str>, Arc<str>, Cow<'_, str>);
            $crate::__impl_encode_via_ref!($db; &[u8] => Box<[u8]>, Arc<[u8]>);
        };
    };
}

// The two loops inside `impl_types!`, out here because a macro defined inside
// another cannot take repetitions of its own on stable Rust.

#[doc(hidden)]
#[macro_export]
macro_rules! __impl_narrow_int {
    ($db:ty; $($ty:ty),*) => {$(
        impl $crate::__sqlx_core::types::Type<$db> for $ty {
            fn type_info() -> $crate::TypeInfo {
                $crate::TypeInfo::Integer
            }
        }

        impl $crate::__sqlx_core::encode::Encode<'_, $db> for $ty {
            fn encode_by_ref(
                &self,
                buf: &mut Vec<$crate::Value>,
            ) -> Result<$crate::__sqlx_core::encode::IsNull, $crate::__sqlx_core::error::BoxDynError> {
                buf.push($crate::Value::Integer(i64::from(*self)));
                Ok($crate::__sqlx_core::encode::IsNull::No)
            }
        }

        impl<'r> $crate::__sqlx_core::decode::Decode<'r, $db> for $ty {
            fn decode(
                value: <$db as $crate::__sqlx_core::database::Database>::ValueRef<'r>,
            ) -> Result<Self, $crate::__sqlx_core::error::BoxDynError> {
                Ok(<$ty>::try_from($crate::AsValue::as_value(&value).int()?)?)
            }
        }
    )*};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __impl_encode_via_ref {
    ($db:ty; $by:ty => $($ty:ty),*) => {$(
        impl $crate::__sqlx_core::encode::Encode<'_, $db> for $ty {
            fn encode_by_ref(
                &self,
                buf: &mut Vec<$crate::Value>,
            ) -> Result<$crate::__sqlx_core::encode::IsNull, $crate::__sqlx_core::error::BoxDynError> {
                <$by as $crate::__sqlx_core::encode::Encode<$db>>::encode(&**self, buf)
            }
        }
    )*};
}
