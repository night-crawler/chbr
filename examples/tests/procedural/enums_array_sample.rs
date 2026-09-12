use bloch::parse::block::parse_single;
use bloch::value::{Enum8SliceIterator, Enum16SliceIterator};
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn enums_array_sample() -> TestResult {
    let data = std::fs::read(crate::common::fixture("enums_array_sample.native"))?;
    let (_, block) = parse_single(&data)?;

    // 0,"['Red', 'Green']",['Foo']
    // 1,"['Blue', 'Red']",['Bar']
    // 2,['Green'],"['Foo', 'Bar']"
    // 3,[],['Foo']
    // 4,"['Red', 'Blue']",[]
    // 5,"['Green', 'Red', 'Blue']",['Bar']

    let expected_e8 = [
        vec!["Red", "Green"],
        vec!["Blue", "Red"],
        vec!["Green"],
        vec![],
        vec!["Red", "Blue"],
        vec!["Green", "Red", "Blue"],
    ];

    let e8_marker = &block.markers[1];
    for (i, expected) in expected_e8.iter().enumerate() {
        let value: Enum8SliceIterator = e8_marker.get(i)?.unwrap().try_into()?;
        let mut actual = vec![];
        for item in value {
            actual.push(item);
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    let expected_e16 = [
        vec!["Foo"],
        vec!["Bar"],
        vec!["Foo", "Bar"],
        vec!["Foo"],
        vec![],
        vec!["Bar"],
    ];

    let e16_marker = &block.markers[2];
    for (i, expected) in expected_e16.iter().enumerate() {
        let value: Enum16SliceIterator = e16_marker.get(i)?.unwrap().try_into()?;
        let mut actual = vec![];
        for item in value {
            actual.push(item);
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
