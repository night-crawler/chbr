use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn lc_string() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("plain_lc_string.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,apple
    // 1,banana
    // 2,cherry
    // 3,date
    // 4,elderberry
    // 5,fig

    let expected_strings = ["apple", "banana", "cherry", "date", "elderberry", "fig"];

    let strings_marker = &block.markers[1];
    for (i, expected) in expected_strings.iter().enumerate() {
        let value: &str = strings_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
