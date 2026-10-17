//! Chrono codecs and SQLite date and time parsing
//!
//! Datetimes encode as RFC 3339 text. Decoding accepts the SQLite date and time text formats,
//! Unix seconds stored as INTEGER and Julian days stored as REAL.

use chrono::{
    DateTime, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime, Offset, SecondsFormat,
    TimeZone, Utc,
};
use sqlx_core::{
    database::Database,
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use super::{TursoTemporalValue, TursoValue, TursoValueRef};
use crate::{Turso, TursoAdapterError, TursoTypeInfo};

type ArgumentBuffer = <Turso as Database>::ArgumentBuffer;

const SQLITE_DATETIME_FORMATS: [&str; 12] = [
    "%F %T%.f",
    "%F %R",
    "%F %RZ",
    "%F %R%:z",
    "%F %T%.fZ",
    "%F %T%.f%:z",
    "%FT%R",
    "%FT%RZ",
    "%FT%R%:z",
    "%FT%T%.f",
    "%FT%T%.fZ",
    "%FT%T%.f%:z",
];

const SQLITE_TIME_FORMATS: [&str; 7] =
    ["%T.f", "%T%.f", "%R", "%RZ", "%T%.fZ", "%R%:z", "%T%.f%:z"];

/// Julian day of the Unix epoch, 1970-01-01T00:00:00Z.
const UNIX_EPOCH_JULIAN_DAY: f64 = 2_440_587.5;
const SECONDS_PER_DAY: f64 = 86_400.0;

impl<Tz> Type<Turso> for DateTime<Tz>
where
    Tz: TimeZone,
{
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("DATETIME")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <NaiveDateTime as Type<Turso>>::compatible(ty)
    }
}

impl<Tz> Encode<'_, Turso> for DateTime<Tz>
where
    Tz: TimeZone,
    Tz::Offset: std::fmt::Display,
{
    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(
            self.to_rfc3339_opts(SecondsFormat::AutoSi, false),
        ));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for DateTime<FixedOffset> {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        decode_datetime(value)
    }
}

impl<'r> Decode<'r, Turso> for DateTime<Utc> {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Utc.from_utc_datetime(&decode_datetime(value)?.naive_utc()))
    }
}

impl<'r> Decode<'r, Turso> for DateTime<Local> {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Local.from_utc_datetime(&decode_datetime(value)?.naive_utc()))
    }
}

impl Type<Turso> for NaiveDateTime {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("DATETIME")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_datetime_affinity()
            || ty.has_text_affinity()
            || ty.has_integer_affinity()
            || ty.has_real_affinity()
    }
}

impl Encode<'_, Turso> for NaiveDateTime {
    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.format("%F %T%.f").to_string()));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for NaiveDateTime {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(decode_datetime(value)?.naive_local())
    }
}

impl Type<Turso> for NaiveDate {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("DATE")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_date_affinity() || ty.has_text_affinity()
    }
}

impl Encode<'_, Turso> for NaiveDate {
    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.format("%F").to_string()));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for NaiveDate {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        let text = value.text()?;
        NaiveDate::parse_from_str(text, "%F").map_err(|_parse_error| invalid_text("date", text))
    }
}

impl Type<Turso> for NaiveTime {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo::new("TIME")
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        ty.has_time_affinity() || ty.has_text_affinity()
    }
}

impl Encode<'_, Turso> for NaiveTime {
    fn encode_by_ref(&self, buf: &mut ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(TursoValue::text(self.format("%T%.f").to_string()));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for NaiveTime {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        let text = value.text()?;
        SQLITE_TIME_FORMATS
            .iter()
            .find_map(|format| NaiveTime::parse_from_str(text, format).ok())
            .ok_or_else(|| invalid_text("time", text))
    }
}

fn decode_datetime(value: TursoValueRef<'_>) -> Result<DateTime<FixedOffset>, BoxDynError> {
    match value.temporal()? {
        TursoTemporalValue::Text(text) => {
            datetime_from_text(text).ok_or_else(|| invalid_text("datetime", text))
        }
        TursoTemporalValue::Integer(unix_seconds) => {
            datetime_from_unix_seconds(unix_seconds).ok_or_else(datetime_out_of_range)
        }
        TursoTemporalValue::Real(julian_day) => {
            datetime_from_julian_day(julian_day).ok_or_else(datetime_out_of_range)
        }
    }
}

fn datetime_from_text(value: &str) -> Option<DateTime<FixedOffset>> {
    if let Ok(datetime) = DateTime::parse_from_rfc3339(value) {
        return Some(datetime);
    }

    SQLITE_DATETIME_FORMATS.iter().find_map(|format| {
        DateTime::parse_from_str(value, format).ok().or_else(|| {
            NaiveDateTime::parse_from_str(value, format)
                .ok()
                .map(|datetime| Utc.fix().from_utc_datetime(&datetime))
        })
    })
}

fn datetime_from_unix_seconds(value: i64) -> Option<DateTime<FixedOffset>> {
    Utc.fix().timestamp_opt(value, 0).single()
}

fn datetime_from_julian_day(value: f64) -> Option<DateTime<FixedOffset>> {
    let timestamp = (value - UNIX_EPOCH_JULIAN_DAY) * SECONDS_PER_DAY;
    if !timestamp.is_finite() {
        return None;
    }

    // Float-to-int `as` saturates, and `timestamp_opt` rejects any out-of-range result.
    let mut seconds = timestamp.floor() as i64;
    let mut nanos = ((timestamp - seconds as f64) * 1E9).round() as u32;
    if nanos == 1_000_000_000 {
        seconds = seconds.checked_add(1)?;
        nanos = 0;
    }

    Utc.fix().timestamp_opt(seconds, nanos).single()
}

fn invalid_text(kind: &'static str, value: &str) -> BoxDynError {
    Box::new(TursoAdapterError::InvalidTemporalText {
        kind,
        value: value.to_owned(),
    })
}

fn datetime_out_of_range() -> BoxDynError {
    Box::new(TursoAdapterError::TemporalOutOfRange { kind: "datetime" })
}

#[cfg(test)]
mod tests {
    use super::datetime_from_julian_day;

    #[test]
    fn decodes_negative_fractional_julian_day_timestamp() {
        // Arrange
        let julian_day = 2_440_587.5 - (0.5 / 86_400.0);

        // Act
        let datetime = datetime_from_julian_day(julian_day).expect("julian day in range");

        // Assert
        assert_eq!(datetime.timestamp(), -1);
        assert!((499_900_000..=500_100_000).contains(&datetime.timestamp_subsec_nanos()));
    }

    #[test]
    fn rejects_non_finite_julian_day() {
        assert!(datetime_from_julian_day(f64::NAN).is_none());
        assert!(datetime_from_julian_day(f64::INFINITY).is_none());
    }
}
