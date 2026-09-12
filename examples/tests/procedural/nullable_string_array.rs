use bloch::parse::block::parse_single;
use bloch::value::NullableSliceIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn nullable_string_array() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("nullable_string_array.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,"['apple', 'banana', null]"
    // 1,"[null, 'date', 'elderberry']"
    // 2,"['fig', null, 'honeydew']"
    // 3,[null]
    // 4,[]
    // 5,"['lemon', null, 'mango']"

    let expected = [
        vec![Some("apple"), Some("banana"), None],
        vec![None, Some("date"), Some("elderberry")],
        vec![Some("fig"), None, Some("honeydew")],
        vec![None],
        vec![],
        vec![Some("lemon"), None, Some("mango")],
    ];

    let nullable_string_array_marker = &block.markers[1];
    for (i, expected) in expected.iter().enumerate() {
        let value: NullableSliceIterator =
            nullable_string_array_marker.get(i)?.unwrap().try_into()?;
        let mut actual = vec![];
        for item in value {
            let item: Option<&str> = item?.try_into()?;
            actual.push(item);
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
