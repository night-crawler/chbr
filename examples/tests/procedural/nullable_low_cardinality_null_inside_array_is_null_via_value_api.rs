use bloch::parse::block::parse_single;
use bloch::reader::{Array, LcNullableStr, TryRead as _};
use bloch::value::{LowCardinalitySliceIterator, Value};
use testresult::TestResult;

#[test]
fn nullable_low_cardinality_null_inside_array_is_null_via_value_api() -> TestResult {
    let data = std::fs::read(crate::common::fixture(
        "array_lc_nullable_string_null_slot.native",
    ))?;
    let (_, block) = parse_single(&data)?;
    let mark = &block.markers[0];

    let reader = Array::<LcNullableStr>::try_from(mark)?;
    let typed: Vec<Option<&str>> = reader.try_read(0)?.try_collect_vec()?;
    assert_eq!(typed, [Some("a"), None, Some("b")]);

    let row = mark.get(0)?.expect("row 0");
    let it = LowCardinalitySliceIterator::try_from(row)?;
    let values: Vec<Value> = it.collect::<Result<_, _>>()?;
    assert!(
        matches!(values[0], Value::String(s) if s == "a"),
        "{values:?}"
    );
    assert!(
        matches!(values[1], Value::Empty),
        "NULL element must be Value::Empty, got {:?}",
        values[1]
    );
    assert!(
        matches!(values[2], Value::String(s) if s == "b"),
        "{values:?}"
    );
    Ok(())
}
