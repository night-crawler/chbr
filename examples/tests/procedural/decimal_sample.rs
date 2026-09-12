use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn decimal_sample() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("decimal_sample.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,1.234,1.234567,1.234567890123,1.234567890123456556104338
    // 1,2.345,2.345678,2.345678901234,2.345678901234567875661641
    // 2,3.456,3.456789,3.456789012345,3.456789012345678555325440

    let expected_d32 = [
        rust_decimal::Decimal::new(1234, 3),
        rust_decimal::Decimal::new(2345, 3),
        rust_decimal::Decimal::new(3456, 3),
    ];
    let decimal32_marker = &block.markers[1];
    for (i, expected) in expected_d32.iter().enumerate() {
        let value: rust_decimal::Decimal = decimal32_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let expected_d64 = [
        rust_decimal::Decimal::new(1234567, 6),
        rust_decimal::Decimal::new(2345678, 6),
        rust_decimal::Decimal::new(3456789, 6),
    ];

    let decimal64_marker = &block.markers[2];
    for (i, expected) in expected_d64.iter().enumerate() {
        let value: rust_decimal::Decimal = decimal64_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let expected_d128 = [
        rust_decimal::Decimal::new(1234567890123, 12),
        rust_decimal::Decimal::new(2345678901234, 12),
        rust_decimal::Decimal::new(3456789012345, 12),
    ];
    let decimal128_marker = &block.markers[3];
    for (i, expected) in expected_d128.iter().enumerate() {
        let value: rust_decimal::Decimal = decimal128_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let decimal256_marker = &block.markers[4];
    assert!(matches!(
        rust_decimal::Decimal::try_from(decimal256_marker.get(0)?.unwrap()),
        Err(bloch::Error::NotImplemented(_))
    ));

    Ok(())
}
