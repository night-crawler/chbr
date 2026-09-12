//! Type names as ClickHouse emits them in `Native` block headers.

use bloch::interval::Kind;
use bloch::parse::block::parse_single;
use bloch::value::{IntervalSliceIterator, Value};
use bloch::{Error, Interval};
use chrono::TimeDelta;
use testresult::TestResult;

#[test]
fn interval_types_parse() -> TestResult {
    let data = std::fs::read(crate::common::fixture("interval.native"))?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.num_rows, 2);

    let fixed = [
        ("ns", Kind::Nanosecond, TimeDelta::nanoseconds(1)),
        ("us", Kind::Microsecond, TimeDelta::microseconds(2)),
        ("ms", Kind::Millisecond, TimeDelta::milliseconds(3)),
        ("s", Kind::Second, TimeDelta::seconds(4)),
        ("mi", Kind::Minute, TimeDelta::minutes(5)),
        ("h", Kind::Hour, TimeDelta::hours(6)),
        ("d", Kind::Day, TimeDelta::days(7)),
        ("w", Kind::Week, TimeDelta::weeks(8)),
    ];
    for (name, kind, expected) in fixed {
        let mark = block.mark(name)?;
        assert_eq!(mark.as_str(), kind.as_str());
        let value = mark.get(1)?.unwrap();
        assert_eq!(TimeDelta::try_from(value.clone())?, expected, "{name}");
        assert_eq!(Interval::try_from(value)?.kind, kind, "{name}");
    }

    let calendar = [
        ("mo", Kind::Month, 9),
        ("q", Kind::Quarter, 10),
        ("y", Kind::Year, 11),
    ];
    for (name, kind, count) in calendar {
        let value = block.mark(name)?.get(0)?.unwrap();
        assert_eq!(Interval::try_from(value.clone())?, Interval { kind, count });
        assert!(matches!(
            TimeDelta::try_from(value),
            Err(Error::MismatchedType(from, "TimeDelta")) if from == kind.as_str()
        ));
    }

    let arr: IntervalSliceIterator = block.mark("arr")?.get(0)?.unwrap().try_into()?;
    let arr = arr.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(arr, [TimeDelta::seconds(-1), TimeDelta::seconds(1)]);

    let n = block.mark("n")?;
    assert!(matches!(n.get(0)?, Some(Value::Empty)));
    assert_eq!(
        TimeDelta::try_from(n.get(1)?.unwrap())?,
        TimeDelta::hours(1)
    );
    Ok(())
}
