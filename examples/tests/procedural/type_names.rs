//! Canonical ClickHouse type names as emitted by `NativeWriter`.
//!
//! Grammar references: `DataTypeTuple::doGetName` (backQuoteIfNeed),
//! `SerializationNothing::serializeBinaryBulk`, `DataTypeInterval : DataTypeNumberBase<Int64>`.

use chbr::mark::Mark;
use chbr::parse::block::parse_single;
use chbr::value::{NestedIterator, Value};
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
    assert!(matches!(block.markers[0].get(0)?, Some(Value::Int64(1))));
    Ok(())
}

#[test]
fn aggregate_function_state_parses() -> TestResult {
    let data = load("interval_and_aggregate.native")?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.markers.len(), 2);
    Ok(())
}
