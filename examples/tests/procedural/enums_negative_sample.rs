use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn enums_negative_sample() -> TestResult {
    let data = std::fs::read(crate::common::fixture("enums_negative_sample.native"))?;
    let (_, block) = parse_single(&data)?;

    // 0,Pos,Pos
    // 1,Neg,Neg
    // 2,Min,Min
    // 3,Neg,Pos

    let expected_e8 = ["Pos", "Neg", "Min", "Neg"];
    let e8_marker = &block.markers[1];
    for (i, expected) in expected_e8.iter().enumerate() {
        let value: &str = e8_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let expected_e16 = ["Pos", "Neg", "Min", "Pos"];
    let e16_marker = &block.markers[2];
    for (i, expected) in expected_e16.iter().enumerate() {
        let value: &str = e16_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
