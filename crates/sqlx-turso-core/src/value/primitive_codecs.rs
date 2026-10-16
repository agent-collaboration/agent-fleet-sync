//! Codecs for integers, booleans, floats, text, bytes and `Option`

use std::sync::Arc;

use sqlx_core::{
    database::Database,
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use super::{TursoValue, TursoValueRef};
use crate::{Turso, TursoTypeInfo};

type ArgumentBuffer = <Turso as Database>::ArgumentBuffer;

macro_rules! impl_integer_type {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl Type<Turso> for $ty {
                fn type_info() -> TursoTypeInfo {
                    TursoTypeInfo::new("INTEGER")
                }

                fn compatible(ty: &TursoTypeInfo) -> bool {
                    ty.has_integer_affinity()
                }
            }

            impl Encode<'_, Turso> for $ty {
                fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
                    buf.push(TursoValue::integer(i64::from(*self)));
                    Ok(IsNull::No)
                }
            }

            impl<'r> Decode<'r, Turso> for $ty {
                fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
                    Ok(value.integer()?.try_into()?)
                }
            }
        )+
    };
}

// `u64` decodes but does not encode: values above `i64::MAX` have no INTEGER representation.
impl_integer_type!(i8, i16, i32, i64, u8, u16, u32);

impl Type<Turso> for u64 {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("INTEGER")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_integer_affinity()
    }
}

impl<'r> Decode<'r, Turso> for u64 {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.integer()?.try_into()?)
    }
}

impl Type<Turso> for bool {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("BOOLEAN")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_bool_affinity() || ty.has_integer_affinity()
    }
}

impl Encode<'_, Turso> for bool {
    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::integer(i64::from(*self)));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for bool {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.integer()? != 0)
    }
}

impl Type<Turso> for f64 {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("REAL")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_real_affinity()
    }
}

impl Encode<'_, Turso> for f64 {
    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::real(*self));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for f64 {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        value.real()
    }
}

impl Type<Turso> for f32 {
    fn type_info() -> TursoTypeInfo {
        <f64 as Type<Turso>>::type_info()
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <f64 as Type<Turso>>::compatible(ty)
    }
}

impl Encode<'_, Turso> for f32 {
    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::real(f64::from(*self)));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for f32 {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        // REAL is stored as f64; narrowing to f32 rounds, as SQLx's SQLite driver does.
        Ok(value.real()? as f32)
    }
}

impl Type<Turso> for str {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("TEXT")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_text_affinity()
    }
}

impl Encode<'_, Turso> for &'_ str {
    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(*self));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for &'r str {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        value.text()
    }
}

impl Type<Turso> for String {
    fn type_info() -> TursoTypeInfo {
        <str as Type<Turso>>::type_info()
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <str as Type<Turso>>::compatible(ty)
    }
}

impl Encode<'_, Turso> for String {
    fn encode(self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self));
        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.as_str()));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for String {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.text()?.to_owned())
    }
}

impl Encode<'_, Turso> for Arc<str> {
    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.as_ref()));
        Ok(IsNull::No)
    }
}

impl Type<Turso> for [u8] {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("BLOB")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_blob_affinity() || ty.has_text_affinity()
    }
}

impl Encode<'_, Turso> for &'_ [u8] {
    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::blob(*self));
        Ok(IsNull::No)
    }
}

impl Type<Turso> for Vec<u8> {
    fn type_info() -> TursoTypeInfo {
        <[u8] as Type<Turso>>::type_info()
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <[u8] as Type<Turso>>::compatible(ty)
    }
}

impl Encode<'_, Turso> for Vec<u8> {
    fn encode(self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::blob(self));
        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::blob(self.as_slice()));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for Vec<u8> {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.blob()?.to_vec())
    }
}

sqlx_core::impl_encode_for_option!(Turso);
