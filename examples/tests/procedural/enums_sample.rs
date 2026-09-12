use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn enums_sample() -> TestResult {
    let data = std::fs::read(crate::common::fixture("enums_sample.native"))?;
    let (_, block) = parse_single(&data)?;

    // 0,Red,Foo
    // 1,Green,Bar
    // 2,Blue,Foo
    // 3,Red,Bar
    // 4,Green,Foo
    // 5,Blue,Bar

    let expected_e8 = ["Red", "Green", "Blue", "Red", "Green", "Blue"];

    let e8_marker = &block.markers[1];
    for (i, expected) in expected_e8.iter().enumerate() {
        let value: &str = e8_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let expected_e16 = ["Foo", "Bar", "Foo", "Bar", "Foo", "Bar"];
    let e16_marker = &block.markers[2];
    for (i, expected) in expected_e16.iter().enumerate() {
        let value: &str = e16_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
