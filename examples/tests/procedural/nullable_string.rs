use bloch::BStr;
use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn nullable_string() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("nullable_string.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,hello
    // 1,
    // 2,world
    // 3,clickhouse
    // 4,
    // 5,test
    let expected_col = [
        Some("hello"),
        None,
        Some("world"),
        Some("clickhouse"),
        None,
        Some("test"),
    ];

    let strings_marker = &block.markers[1];
    for (i, expected) in expected_col.iter().enumerate() {
        let value: Option<&str> = strings_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");

        let value = strings_marker.get_opt_str(i)?.unwrap();
        assert_eq!(
            value,
            expected.map(BStr::new),
            "Mismatch at index {i} (get_opt_str)"
        );
    }

    // Out of range is the outer None; NULL (index 1) is the inner None.
    assert_eq!(strings_marker.get_opt_str(expected_col.len())?, None);
    assert_eq!(strings_marker.get_opt_str(1)?, Some(None));
    // Same contract for non-nullable columns via the convenience path.
    assert_eq!(block.markers[0].get_opt_i64(block.num_rows)?, None);
    assert_eq!(block.markers[0].get_opt_i64(0)?, Some(Some(0)));

    Ok(())
}
