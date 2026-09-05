//! Type names as ClickHouse emits them in `Native` block headers.

use chbr::interval::Kind;
use chbr::mark::Mark;
use chbr::parse::block::parse_single;
use chbr::value::{
    ArraySliceIterator, IntervalSliceIterator, NestedIterator, Time64SliceIterator,
    TimeSliceIterator, TupleSliceIterator, Value, VariantSliceIterator,
};
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
fn time_types_parse() -> TestResult {
    let data = load("time.native")?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.num_rows, 2);

    let expected = [
        ("t", "Time", TimeDelta::seconds(12 * 3600 + 34 * 60 + 56)),
        ("neg", "Time", -TimeDelta::seconds(3600 + 2 * 60 + 3)),
        (
            "t3",
            "Time64",
            TimeDelta::seconds(12 * 3600 + 34 * 60 + 56) + TimeDelta::milliseconds(789),
        ),
        ("neg6", "Time64", -TimeDelta::milliseconds(1500)),
        (
            "t9",
            "Time64",
            TimeDelta::seconds(999 * 3600 + 59 * 60 + 59) + TimeDelta::nanoseconds(999_999_999),
        ),
        ("t0", "Time64", TimeDelta::seconds(7)),
    ];
    for (name, mark_name, td) in expected {
        let mark = block.mark(name)?;
        assert_eq!(mark.as_str(), mark_name, "{name}");
        assert_eq!(TimeDelta::try_from(mark.get(1)?.unwrap())?, td, "{name}");
    }

    let arr: TimeSliceIterator = block.mark("arr")?.get(0)?.unwrap().try_into()?;
    assert_eq!(
        arr.collect::<Vec<_>>(),
        [TimeDelta::seconds(1), TimeDelta::seconds(-2)]
    );

    let n = block.mark("n")?;
    assert!(matches!(n.get(0)?, Some(Value::Empty)));
    assert_eq!(
        TimeDelta::try_from(n.get(1)?.unwrap())?,
        TimeDelta::hours(1)
    );
    let t3: Time64SliceIterator = block.mark("t3")?.slice(0..2)?.try_into()?;
    let t3 = t3.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(t3.len(), 2);
    assert_eq!(t3[0], t3[1]);
    Ok(())
}

fn points(points: TupleSliceIterator<'_>) -> chbr::Result<Vec<(f64, f64)>> {
    points.map(<(f64, f64)>::try_from).collect()
}

fn rings(value: Value<'_>) -> chbr::Result<Vec<Vec<(f64, f64)>>> {
    ArraySliceIterator::<TupleSliceIterator>::try_from(value)?
        .map(|ring| points(ring?))
        .collect()
}

#[test]
fn geometry_is_a_variant_in_discriminator_order() -> TestResult {
    let data = load("geometry_sample.native")?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.num_rows, 8);

    let geo = block.mark("geo")?;
    assert!(matches!(geo, Mark::Variant(_)), "{geo:?}");

    // Rows are inserted as Point, LineString, MultiLineString, Polygon, MultiPolygon, Ring,
    // MultiPoint, NULL: each resolves to its own shape only if the discriminators map to
    // LineString=0, MultiLineString=1, MultiPolygon=2, Point=3, Polygon=4, Ring=5, MultiPoint=6.
    assert_eq!(<(f64, f64)>::try_from(geo.get(0)?.unwrap())?, (1.0, 2.0));
    assert_eq!(
        points(geo.get(1)?.unwrap().try_into()?)?,
        [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]
    );
    assert_eq!(
        rings(geo.get(2)?.unwrap())?,
        [
            vec![(0.0, 0.0), (1.0, 1.0)],
            vec![(2.0, 2.0), (3.0, 3.0), (4.0, 2.0)],
        ]
    );
    assert_eq!(
        rings(geo.get(3)?.unwrap())?,
        [
            vec![
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0)
            ],
            vec![(4.0, 4.0), (5.0, 4.0), (5.0, 5.0), (4.0, 5.0), (4.0, 4.0)],
        ]
    );
    let multi_polygon: ArraySliceIterator<ArraySliceIterator<TupleSliceIterator>> =
        geo.get(4)?.unwrap().try_into()?;
    assert_eq!(multi_polygon.len(), 2);
    assert_eq!(
        points(geo.get(5)?.unwrap().try_into()?)?,
        [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]
    );
    assert_eq!(
        points(geo.get(6)?.unwrap().try_into()?)?,
        [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]
    );
    assert!(matches!(geo.get(7)?, Some(Value::Empty)));

    let mp = block.mark("mp")?;
    assert_eq!(mp.as_str(), "Array");
    assert_eq!(
        points(mp.get(0)?.unwrap().try_into()?)?,
        [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]
    );
    assert!(points(mp.get(1)?.unwrap().try_into()?)?.is_empty());

    let arr: VariantSliceIterator = block.mark("arr")?.get(0)?.unwrap().try_into()?;
    let arr = arr.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(arr.len(), 2);
    assert_eq!(<(f64, f64)>::try_from(arr[0].clone())?, (1.0, 2.0));
    assert!(matches!(arr[1], Value::Empty));
    Ok(())
}

#[test]
fn aggregate_function_state_is_not_implemented() -> TestResult {
    let data = load("interval_and_aggregate.native")?;
    let Err(Error::NotImplemented(message)) = parse_single(&data) else {
        panic!("expected NotImplemented");
    };
    assert_eq!(
        message,
        "aggregate function state column AggregateFunction(sum, UInt64)"
    );
    Ok(())
}
