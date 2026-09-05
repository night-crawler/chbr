//! Canonical ClickHouse type names as emitted by `NativeWriter`.
//!
//! Grammar references: `DataTypeTuple::doGetName` (backQuoteIfNeed),
//! `SerializationNothing::serializeBinaryBulk`, `DataTypeInterval : DataTypeNumberBase<Int64>`.

use chbr::interval::Kind;
use chbr::mark::Mark;
use chbr::parse::block::parse_single;
use chbr::value::{IntervalSliceIterator, NestedIterator, Value};
use chbr::{Error, Interval};
use chrono::TimeDelta;
use testresult::TestResult;

fn load(name: &str) -> std::io::Result<Vec<u8>> {
    std::fs::read(crate::common::fixture(name))
}

#[test]
fn named_tuple_field_may_start_with_underscore_or_be_backquoted() -> TestResult {
    let data = load("named_tuple_quoted.native")?;
    let (_, block) = parse_single(&data)?;
    let Mark::NamedTuple(t1) = &block.markers[0] else {
        panic!("t1 is not a NamedTuple: {:?}", block.markers[0]);
    };
    assert_eq!(&*t1.col_names, ["_id"]);
    let Mark::NamedTuple(t2) = &block.markers[1] else {
        panic!("t2 is not a NamedTuple: {:?}", block.markers[1]);
    };
    assert_eq!(&*t2.col_names, ["my field", "1x"]);
    Ok(())
}

#[test]
fn nested_field_names_may_be_backquoted() -> TestResult {
    let data = load("nested_quoted.native")?;
    let (_, block) = parse_single(&data)?;
    let rows: NestedIterator = block.markers[0].get(0)?.unwrap().try_into()?;
    let mut actual = Vec::<(u64, &str)>::new();
    for row in rows {
        let (mut id, mut name) = (None, None);
        for field in row {
            let (field_name, field_value) = field?;
            match field_name {
                "my field" => id = Some(field_value.try_into()?),
                "1x" => name = Some(field_value.try_into()?),
                other => panic!("unexpected field {other:?}"),
            }
        }
        actual.push((id.expect("missing `my field`"), name.expect("missing `1x`")));
    }
    assert_eq!(actual, [(1, "x"), (2, "y")]);
    Ok(())
}

#[test]
fn array_nothing_and_nullable_nothing_parse() -> TestResult {
    let data = load("nothing_scalar.native")?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.num_rows, 1);
    assert_eq!(block.markers.len(), 2);
    Ok(())
}

#[test]
fn interval_types_parse() -> TestResult {
    let data = load("interval.native")?;
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

#[test]
fn aggregate_function_state_parses() -> TestResult {
    let data = load("interval_and_aggregate.native")?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.markers.len(), 2);
    Ok(())
}
