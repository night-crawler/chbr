use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, Interval, Nullable};
use chbr::{Error, ParsedBlock};
use chrono::TimeDelta;

#[derive(FromBlock)]
struct Row<'a> {
    ns: Interval<'a>,
    us: Interval<'a>,
    ms: Interval<'a>,
    s: Interval<'a>,
    mi: Interval<'a>,
    h: Interval<'a>,
    d: Interval<'a>,
    w: Interval<'a>,
    arr: Array<'a, Interval<'a>>,
    n: Nullable<'a, Interval<'a>>,
}

#[derive(FromBlock)]
struct CalendarRow<'a> {
    #[allow(dead_code)]
    mo: Interval<'a>,
}

fn load() -> Result<Vec<u8>, std::io::Error> {
    std::fs::read(crate::common::fixture("interval.native"))
}

#[test]
fn reads_fixed_length_intervals_as_time_delta() -> Result<(), Box<dyn std::error::Error>> {
    let data = load()?;
    let (_, block) = parse_single(&data)?;
    let expected_n = [None, Some(TimeDelta::hours(1))];
    let mut rows = 0;
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(
            (row.ns, row.us, row.ms, row.s, row.mi, row.h, row.d, row.w),
            (
                TimeDelta::nanoseconds(1),
                TimeDelta::microseconds(2),
                TimeDelta::milliseconds(3),
                TimeDelta::seconds(4),
                TimeDelta::minutes(5),
                TimeDelta::hours(6),
                TimeDelta::days(7),
                TimeDelta::weeks(8),
            )
        );
        let arr = row.arr.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(arr, [TimeDelta::seconds(-1), TimeDelta::seconds(1)]);
        assert_eq!(row.n, expected_n[index]);
        rows += 1;
    }
    assert_eq!(rows, 2);
    Ok(())
}

#[test]
fn calendar_intervals_are_rejected_at_construction() -> Result<(), Box<dyn std::error::Error>> {
    let data = load()?;
    let (_, block): (_, ParsedBlock) = parse_single(&data)?;
    assert!(matches!(
        CalendarRow::from_block(&block),
        Err(Error::MismatchedType("Month", "TimeDelta"))
    ));
    Ok(())
}
