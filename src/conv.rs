use std::sync::LazyLock;

use chrono::{DateTime, Duration, NaiveDate, TimeZone as _, Utc};
use chrono_tz::{Tz, TzOffset};

const EPOCH_DATE: NaiveDate = NaiveDate::from_yo_opt(1970, 1).unwrap();

static UTC_TZ_OFFSET: LazyLock<TzOffset> =
    LazyLock::new(|| Tz::UTC.offset_from_utc_datetime(&DateTime::UNIX_EPOCH.naive_utc()));

#[inline]
const fn is_utc_alias(tz: Tz) -> bool {
    matches!(
        tz,
        Tz::UTC
            | Tz::Zulu
            | Tz::Universal
            | Tz::UCT
            | Tz::Etc__UTC
            | Tz::Etc__Zulu
            | Tz::Etc__Universal
            | Tz::GMT
            | Tz::Etc__GMT
            | Tz::Etc__GMTPlus0
            | Tz::Etc__GMTMinus0
    )
}

#[inline(always)]
pub fn date16(days: u16) -> NaiveDate {
    EPOCH_DATE + Duration::days(i64::from(days))
}

#[inline(always)]
pub fn date32(days: i32) -> NaiveDate {
    EPOCH_DATE + Duration::days(i64::from(days))
}

#[inline(always)]
pub fn datetime32(secs: u32) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(i64::from(secs), 0).unwrap()
}

#[inline(always)]
pub fn datetime32_tz(secs: u32, tz: Tz) -> DateTime<Tz> {
    let dt_utc = datetime32(secs);
    if is_utc_alias(tz) {
        return DateTime::<Tz>::from_naive_utc_and_offset(dt_utc.naive_utc(), *UTC_TZ_OFFSET);
    }
    dt_utc.with_timezone(&tz)
}
#[inline(always)]
pub fn datetime64(timestamp: i64, precision: u8) -> Option<DateTime<Utc>> {
    let pow = 10i64.pow(u32::from(precision));
    let secs = timestamp / pow;
    let rem_ms = (timestamp % pow).abs();
    let nsec = rem_ms.checked_mul(1_000_000)?;
    let nsec = u32::try_from(nsec).ok()?;
    DateTime::<Utc>::from_timestamp(secs, nsec)
}

#[inline(always)]
pub fn datetime64_tz(timestamp: i64, precision: u8, tz: Tz) -> Option<DateTime<Tz>> {
    let dt_utc = datetime64(timestamp, precision)?;
    if is_utc_alias(tz) {
        return Some(DateTime::<Tz>::from_naive_utc_and_offset(
            dt_utc.naive_utc(),
            *UTC_TZ_OFFSET,
        ));
    }
    Some(dt_utc.with_timezone(&tz))
}
