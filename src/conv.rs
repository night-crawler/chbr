use chrono::{DateTime, Duration, NaiveDate, Utc};
use chrono_tz::Tz;

const EPOCH_DATE: NaiveDate = NaiveDate::from_yo_opt(1970, 1).unwrap();

pub fn date16(days: u16) -> NaiveDate {
    EPOCH_DATE + Duration::days(i64::from(days))
}

pub fn date32(days: i32) -> NaiveDate {
    EPOCH_DATE + Duration::days(i64::from(days))
}

pub fn datetime32(secs: u32) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(i64::from(secs), 0).unwrap()
}

pub fn datetime32_tz(secs: u32, tz: Tz) -> DateTime<Tz> {
    let dt_utc = datetime32(secs);
    dt_utc.with_timezone(&tz)
}
pub fn datetime64(timestamp: i64, precision: u8) -> Option<DateTime<Utc>> {
    let pow = 10i64.pow(u32::from(precision));
    let secs = timestamp / pow;
    let rem_ms = (timestamp % pow).abs();
    let nsec = rem_ms.checked_mul(1_000_000)?;
    let nsec = u32::try_from(nsec).ok()?;
    DateTime::<Utc>::from_timestamp(secs, nsec)
}

pub fn datetime64_tz(timestamp: i64, precision: u8, tz: Tz) -> Option<DateTime<Tz>> {
    let dt_utc = datetime64(timestamp, precision)?;
    Some(dt_utc.with_timezone(&tz))
}
