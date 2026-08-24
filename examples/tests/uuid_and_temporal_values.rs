mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Date, Date32, DateTime, DateTime64, Uuid};

#[derive(FromBlock)]
struct Row<'a> {
    id: Uuid<'a>,
    date: Date<'a>,
    date32: Date32<'a>,
    datetime: DateTime<'a>,
    datetime64: DateTime64<'a>,
}

#[test]
fn reads_uuid_and_temporal_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("uuid_and_dates.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected_ids = [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
        "00000000-0000-0000-0000-000000000003",
        "00000000-0000-0000-0000-000000000004",
    ];
    let expected_date32 = ["2023-01-01", "2023-02-01", "2023-03-01", "1969-09-23"];
    let expected_millis = [123, 456, 789, 789];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id.to_string(), expected_ids[index]);
        assert_eq!(
            row.date.to_string(),
            ["2023-01-01", "2023-02-01", "2023-03-01", "2023-03-01"][index]
        );
        assert_eq!(row.date32.to_string(), expected_date32[index]);
        assert_eq!(row.datetime.timezone(), chrono_tz::UTC);
        assert_eq!(
            row.datetime64.timestamp_subsec_millis(),
            expected_millis[index]
        );
    }
    Ok(())
}
