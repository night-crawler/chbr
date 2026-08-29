use std::sync::LazyLock;

use chrono::{DateTime, Duration, NaiveDate, TimeZone as _, Utc};
use chrono_tz::{Tz, TzOffset};

const EPOCH_DATE: NaiveDate = NaiveDate::from_yo_opt(1970, 1).expect("1970 day 1 is a valid date");

const UTC_ALIASES: [Tz; 11] = [
    Tz::UTC,
    Tz::GMT,
    Tz::Zulu,
    Tz::Etc__UTC,
    Tz::Etc__GMT,
    Tz::Universal,
    Tz::UCT,
    Tz::Etc__Zulu,
    Tz::Etc__Universal,
    Tz::Etc__GMTPlus0,
    Tz::Etc__GMTMinus0,
];

// A reminder for future self: either a lock or an unsound transmute into TzOffset because they have
// no public anything to const construct it.
static UTC_ALIAS_OFFSETS: LazyLock<[TzOffset; UTC_ALIASES.len()]> = LazyLock::new(|| {
    UTC_ALIASES.map(|tz| tz.offset_from_utc_datetime(&DateTime::UNIX_EPOCH.naive_utc()))
});

#[inline]
pub fn utc_alias_offset(tz: Tz) -> Option<TzOffset> {
    let idx = UTC_ALIASES.iter().position(|&alias| alias == tz)?;
    Some(UTC_ALIAS_OFFSETS[idx])
}

pub fn date16(days: u16) -> NaiveDate {
    EPOCH_DATE + Duration::days(i64::from(days))
}

pub fn date32(days: i32) -> NaiveDate {
    EPOCH_DATE + Duration::days(i64::from(days))
}

#[inline(always)]
pub fn datetime32(secs: u32) -> DateTime<Utc> {
    // SAFETY: every u32 timestamp is within chrono's DateTime range, and
    // zero nanoseconds is valid.
    unsafe { DateTime::<Utc>::from_timestamp(i64::from(secs), 0).unwrap_unchecked() }
}

#[inline(always)]
pub fn datetime32_tz(secs: u32, tz: Tz) -> DateTime<Tz> {
    datetime32_resolved(secs, tz, utc_alias_offset(tz))
}

#[inline(always)]
pub fn datetime32_resolved(secs: u32, tz: Tz, utc_offset: Option<TzOffset>) -> DateTime<Tz> {
    let dt_utc = datetime32(secs);
    match utc_offset {
        Some(offset) => DateTime::from_naive_utc_and_offset(dt_utc.naive_utc(), offset),
        None => dt_utc.with_timezone(&tz),
    }
}

pub fn datetime64(timestamp: i64, precision: u8) -> Option<DateTime<Utc>> {
    let pow = 10i64.checked_pow(u32::from(precision))?;
    let secs = timestamp / pow;
    let rem_ms = (timestamp % pow).abs();
    let nsec = rem_ms.checked_mul(1_000_000)?;
    let nsec = u32::try_from(nsec).ok()?;
    DateTime::<Utc>::from_timestamp(secs, nsec)
}

pub fn datetime64_tz(timestamp: i64, precision: u8, tz: Tz) -> Option<DateTime<Tz>> {
    datetime64_resolved(timestamp, precision, tz, utc_alias_offset(tz))
}

#[inline]
pub fn datetime64_resolved(
    timestamp: i64,
    precision: u8,
    tz: Tz,
    utc_offset: Option<TzOffset>,
) -> Option<DateTime<Tz>> {
    let dt_utc = datetime64(timestamp, precision)?;
    Some(match utc_offset {
        Some(offset) => DateTime::from_naive_utc_and_offset(dt_utc.naive_utc(), offset),
        None => dt_utc.with_timezone(&tz),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECS: u32 = 1_700_000_000;

    #[test]
    fn datetime32_tz_preserves_zone_identity() {
        for tz in [
            Tz::UTC,
            Tz::Zulu,
            Tz::Universal,
            Tz::UCT,
            Tz::Etc__UTC,
            Tz::GMT,
            Tz::Etc__GMT,
            Tz::Etc__GMTPlus0,
        ] {
            let actual = datetime32_tz(SECS, tz);
            assert_eq!(actual.timezone(), tz, "zone identity lost for {tz}");
        }
    }

    #[test]
    fn datetime32_tz_matches_with_timezone() {
        for tz in [Tz::GMT, Tz::Etc__GMT, Tz::Zulu] {
            let actual = datetime32_tz(SECS, tz);
            let expected = datetime32(SECS).with_timezone(&tz);
            assert_eq!(
                actual.to_string(),
                expected.to_string(),
                "fast path diverges from with_timezone for {tz}"
            );
        }
    }

    #[test]
    fn datetime32_tz_gmt_abbreviation() {
        let actual = datetime32_tz(SECS, Tz::GMT);
        assert_eq!(
            actual.format("%Z").to_string(),
            "GMT",
            "GMT column rendered with the wrong abbreviation"
        );
    }
}
