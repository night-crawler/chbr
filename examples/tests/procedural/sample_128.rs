use bloch::parse::block::parse_single;
use bloch::zc;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn sample_128() -> TestResult {
    let data = std::fs::read(crate::common::fixture("sample_128.native"))?;
    let (_, block) = parse_single(&data)?;

    //    ┌─id─┬──────────────────────u128_single─┬─u128_array──────────────────────────────────────────────────────────┬──────────────────────i128_single─┬─i128_array───────────────────────────────────────────────────────────┐
    // 1. │  0 │ 12345678901234567890123456789012 │ [12345678901234567890123456789012,98765432109876543210987654321098] │ 12345678901234567890123456789012 │ [12345678901234567890123456789012,-98765432109876543210987654321098] │
    //    └────┴──────────────────────────────────┴─────────────────────────────────────────────────────────────────────┴──────────────────────────────────┴──────────────────────────────────────────────────────────────────────┘

    let u128_marker = &block.markers[1];
    let expected_u128 = [12345678901234567890123456789012u128];
    for (i, expected) in expected_u128.iter().enumerate() {
        let value: u128 = u128_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let u128_array_marker = &block.markers[2];
    let expected_u128_array = [vec![
        12345678901234567890123456789012u128,
        98765432109876543210987654321098u128,
    ]];

    for (i, expected) in expected_u128_array.iter().enumerate() {
        let value: &[zc::U128] = u128_array_marker.get(i)?.unwrap().try_into()?;
        let mut actual = vec![];
        for item in value {
            actual.push(item.get());
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
