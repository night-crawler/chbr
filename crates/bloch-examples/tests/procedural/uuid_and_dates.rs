use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
set session_timezone = 'UTC';

drop table if exists uuid_and_dates;

create table uuid_and_dates
(
    id         UUID,
    date       Date,
    date32     Date32,
    datetime   DateTime,
    datetime64 DateTime64(3, 'UTC')
) engine = MergeTree order by tuple();

insert into uuid_and_dates (id, date, date32, datetime, datetime64) values
    ('00000000-0000-0000-0000-000000000001', '2023-01-01', '2023-01-01', '2023-01-01 12:00:00', '2023-01-01 12:00:00.123'),
    ('00000000-0000-0000-0000-000000000002', '2023-02-01', '2023-02-01', '2023-02-01 12:00:00', '2023-02-01 12:00:00.456'),
    ('00000000-0000-0000-0000-000000000003', '2023-03-01', '2023-03-01', '2023-03-01 12:00:00', '2023-03-01 12:00:00.789'),
    ('00000000-0000-0000-0000-000000000004', '2023-03-01', -100, '2023-03-01 12:00:00', '2023-03-01 12:00:00.789');

select * from uuid_and_dates order by id format Native;
"#;

#[test]
fn uuid_and_dates() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("uuid_and_dates.native"))?;
    let (_, block) = parse_single(&buf)?;
    // UUID, Date, Date32, DateTime, DateTime64
    // 00000000-0000-0000-0000-000000000001,2023-01-01,2023-01-01,2023-01-01 12:00:00,2023-01-01T12:00:00.123Z
    // 00000000-0000-0000-0000-000000000002,2023-02-01,2023-02-01,2023-02-01 12:00:00,2023-02-01T12:00:00.456Z
    // 00000000-0000-0000-0000-000000000003,2023-03-01,2023-03-01,2023-03-01 12:00:00,2023-03-01T12:00:00.789Z
    // 00000000-0000-0000-0000-000000000004,2023-03-01,1969-09-23,2023-03-01 12:00:00,2023-03-01T12:00:00.789Z

    let uuid_marker = &block.markers[0];
    let expected_uuids = [
        uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001")?,
        uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000002")?,
        uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000003")?,
        uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000004")?,
    ];
    for (i, expected) in expected_uuids.iter().enumerate() {
        let value: uuid::Uuid = uuid_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let date_marker = &block.markers[1];
    let expected_dates = [
        chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2023, 2, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
    ];
    for (i, expected) in expected_dates.iter().enumerate() {
        let value: chrono::NaiveDate = date_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let date32_marker = &block.markers[2];
    let expected_date32 = [
        chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2023, 2, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(1969, 9, 23).unwrap(),
    ];
    for (i, expected) in expected_date32.iter().enumerate() {
        let value: chrono::NaiveDate = date32_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let datetime_marker = &block.markers[3];
    let expected_datetimes = [
        chrono::DateTime::parse_from_rfc3339("2023-01-01T12:00:00+00:00")?
            .with_timezone(&chrono_tz::UTC),
        chrono::DateTime::parse_from_rfc3339("2023-02-01T12:00:00+00:00")?
            .with_timezone(&chrono_tz::UTC),
        chrono::DateTime::parse_from_rfc3339("2023-03-01T12:00:00+00:00")?
            .with_timezone(&chrono_tz::UTC),
        chrono::DateTime::parse_from_rfc3339("2023-03-01T12:00:00+00:00")?
            .with_timezone(&chrono_tz::UTC),
    ];
    for (i, expected) in expected_datetimes.iter().enumerate() {
        let value: chrono::DateTime<chrono_tz::Tz> = datetime_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");

        let value = datetime_marker.get_datetime(i, chrono_tz::UTC)?.unwrap();
        assert_eq!(value, *expected, "Mismatch at index {i} (get_datetime)");
    }

    let datetime64_marker = &block.markers[4];
    let expected_datetime64 = [
        chrono::DateTime::parse_from_rfc3339("2023-01-01T12:00:00.123+00:00")?
            .with_timezone(&chrono_tz::UTC),
        chrono::DateTime::parse_from_rfc3339("2023-02-01T12:00:00.456+00:00")?
            .with_timezone(&chrono_tz::UTC),
        chrono::DateTime::parse_from_rfc3339("2023-03-01T12:00:00.789+00:00")?
            .with_timezone(&chrono_tz::UTC),
        chrono::DateTime::parse_from_rfc3339("2023-03-01T12:00:00.789+00:00")?
            .with_timezone(&chrono_tz::UTC),
    ];

    for (i, expected) in expected_datetime64.iter().enumerate() {
        let value: chrono::DateTime<chrono_tz::Tz> =
            datetime64_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
