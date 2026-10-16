//! Type checking and describe hooks the checked query macros use for Turso
//!
//! Parameter checking is weak: the high-level Turso statement exposes no parameter metadata, so
//! the macros cannot check bind arity or parameter types.

use sqlx_core::{
    config::macros::{DateTimeCrate, NumericCrate, PreferredCrates},
    type_checking::{Error as TypeCheckingError, FmtValue, ParamChecking, TypeChecking},
    types::Type,
    value::Value,
};

use crate::{Turso, TursoTypeInfo, TursoValue};

impl TypeChecking for Turso {
    const PARAM_CHECKING: ParamChecking = ParamChecking::Weak;

    fn param_type_for_id(
        info: &TursoTypeInfo,
        preferred_crates: &PreferredCrates,
    ) -> Result<&'static str, TypeCheckingError> {
        type_path_for_id(info, preferred_crates)
    }

    fn return_type_for_id(
        info: &TursoTypeInfo,
        preferred_crates: &PreferredCrates,
    ) -> Result<&'static str, TypeCheckingError> {
        type_path_for_id(info, preferred_crates)
    }

    fn get_feature_gate(_info: &TursoTypeInfo) -> Option<&'static str> {
        None
    }

    fn fmt_value_debug(value: &TursoValue) -> FmtValue<'_, Self> {
        let info = value.type_info();

        #[cfg(feature = "chrono")]
        {
            if <sqlx_core::types::chrono::NaiveDateTime as Type<Turso>>::compatible(&info) {
                return FmtValue::debug::<sqlx_core::types::chrono::NaiveDateTime>(value);
            }

            if <sqlx_core::types::chrono::NaiveDate as Type<Turso>>::compatible(&info) {
                return FmtValue::debug::<sqlx_core::types::chrono::NaiveDate>(value);
            }
        }

        if <bool as Type<Turso>>::compatible(&info) {
            return FmtValue::debug::<bool>(value);
        }

        if <i64 as Type<Turso>>::compatible(&info) {
            return FmtValue::debug::<i64>(value);
        }

        if <f64 as Type<Turso>>::compatible(&info) {
            return FmtValue::debug::<f64>(value);
        }

        if <String as Type<Turso>>::compatible(&info) {
            return FmtValue::debug::<String>(value);
        }

        if <Vec<u8> as Type<Turso>>::compatible(&info) {
            return FmtValue::debug::<Vec<u8>>(value);
        }

        FmtValue::unknown(value)
    }
}

fn type_path_for_id(
    info: &TursoTypeInfo,
    preferred_crates: &PreferredCrates,
) -> Result<&'static str, TypeCheckingError> {
    if preferred_crates.numeric == NumericCrate::BigDecimal
        || preferred_crates.numeric == NumericCrate::RustDecimal
    {
        return Err(TypeCheckingError::NumericCrateFeatureNotEnabled);
    }

    if let Some(path) = datetime_type_path(info, preferred_crates) {
        return path;
    }

    if <bool as Type<Turso>>::type_info() == *info {
        return Ok("bool");
    }

    // A declared INTEGER column maps to i32 first, matching SQLx's SQLite driver.
    if <i32 as Type<Turso>>::type_info() == *info {
        return Ok("i32");
    }

    if <i64 as Type<Turso>>::type_info() == *info || <i64 as Type<Turso>>::compatible(info) {
        return Ok("i64");
    }

    if <f64 as Type<Turso>>::type_info() == *info || <f64 as Type<Turso>>::compatible(info) {
        return Ok("f64");
    }

    if <String as Type<Turso>>::type_info() == *info || <String as Type<Turso>>::compatible(info) {
        return Ok("String");
    }

    if <Vec<u8> as Type<Turso>>::type_info() == *info || <Vec<u8> as Type<Turso>>::compatible(info)
    {
        return Ok("Vec<u8>");
    }

    Err(TypeCheckingError::NoMappingFound)
}

fn datetime_type_path(
    info: &TursoTypeInfo,
    preferred_crates: &PreferredCrates,
) -> Option<Result<&'static str, TypeCheckingError>> {
    match preferred_crates.date_time {
        DateTimeCrate::Time => Some(Err(TypeCheckingError::DateTimeCrateFeatureNotEnabled)),
        DateTimeCrate::Chrono => Some(
            chrono_type_path(info)
                .unwrap_or(Err(TypeCheckingError::DateTimeCrateFeatureNotEnabled)),
        ),
        DateTimeCrate::Inferred => chrono_type_path(info),
    }
}

#[cfg(feature = "chrono")]
fn chrono_type_path(info: &TursoTypeInfo) -> Option<Result<&'static str, TypeCheckingError>> {
    if <sqlx_core::types::chrono::NaiveDate as Type<Turso>>::type_info() == *info
        || info.has_date_affinity()
    {
        return Some(Ok("::sqlx_turso::sqlx::types::chrono::NaiveDate"));
    }

    if <sqlx_core::types::chrono::NaiveDateTime as Type<Turso>>::type_info() == *info
        || info.has_datetime_affinity()
    {
        return Some(Ok("::sqlx_turso::sqlx::types::chrono::NaiveDateTime"));
    }

    None
}

#[cfg(not(feature = "chrono"))]
fn chrono_type_path(_info: &TursoTypeInfo) -> Option<Result<&'static str, TypeCheckingError>> {
    None
}

#[cfg(feature = "macros")]
impl sqlx_macros_core::database::DatabaseExt for Turso {
    const DATABASE_PATH: &'static str = "::sqlx_turso::Turso";
    const ROW_PATH: &'static str = "::sqlx_turso::TursoRow";

    fn describe_blocking(
        query: &str,
        database_url: &str,
        driver_config: &sqlx_core::config::drivers::Config,
    ) -> sqlx_core::Result<sqlx_core::describe::Describe<Self>> {
        use sqlx_macros_core::database::CachingDescribeBlocking;

        static CACHE: CachingDescribeBlocking<Turso> = CachingDescribeBlocking::new();

        CACHE.describe(query, database_url, driver_config)
    }
}
