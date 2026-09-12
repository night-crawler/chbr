use bloch::BStr;
use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn fixed_string_sample() -> TestResult {
    let data = std::fs::read(crate::common::fixture("fixed_string_sample.native"))?;
    let (_, block) = parse_single(&data)?;

    // 0,fixed string 1
    // 1,fixed string 2
    // 2,fixed string 3
    // 3,fixed string 4
    // 4,fixed string 5 q

    let expected = [
        "fixed string 1",
        "fixed string 2",
        "fixed string 3",
        "fixed string 4",
        "fixed string 5 q",
    ];

    let fixed_string_marker = &block.markers[1];
    for (i, expected) in expected.iter().enumerate() {
        let value = fixed_string_marker.get(i)?.unwrap();
        // `&str` trims the zero padding; `&BStr` is the raw 16-byte record.
        let text: &str = value.clone().try_into()?;
        assert_eq!(text, *expected, "Mismatch at index {i}");
        let raw: &BStr = value.try_into()?;
        assert_eq!(raw.len(), 16, "Mismatch at index {i}");
        assert_eq!(
            &raw[..expected.len()],
            expected.as_bytes(),
            "Mismatch at index {i}"
        );
        assert!(
            raw[expected.len()..].iter().all(|byte| *byte == 0),
            "Mismatch at index {i}"
        );
    }

    Ok(())
}
