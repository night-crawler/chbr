use bloch::BStr;
use bloch::error::Error;
use bloch::parse::block::parse_single;
use bloch::value::LowCardinalitySliceIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn lc_array_nullable_string() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("array_lc_nullable_string.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,"['apple', 'banana', NULL]"
    // 1,"[NULL, 'date', 'elderberry']"
    // 2,"['fig', NULL, 'honeydew']"
    // 3,[NULL]
    // 4,[]
    // 5,"['lemon', NULL, 'mango']"

    let expected_arrays = [
        vec![Some("apple"), Some("banana"), None],
        vec![None, Some("date"), Some("elderberry")],
        vec![Some("fig"), None, Some("honeydew")],
        vec![None],
        vec![],
        vec![Some("lemon"), None, Some("mango")],
    ];

    let strings_marker = &block.markers[1];
    for (i, expected) in expected_arrays.iter().enumerate() {
        let it: LowCardinalitySliceIterator = strings_marker.get(i)?.unwrap().try_into()?;
        let mut actual = vec![];
        for value in it {
            let value: Option<&str> = value?.try_into()?;
            actual.push(value);
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");

        let actual = strings_marker
            .get_array_lc_opt_strs(i)?
            .unwrap()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            actual,
            expected
                .iter()
                .map(|s| s.map(BStr::new))
                .collect::<Vec<_>>(),
            "Mismatch at index {i} (get_array_lc_opt_strs)"
        );
    }
    assert!(matches!(
        strings_marker.get_array_lc_strs(0),
        Err(Error::MismatchedType(
            "LowCardinality(Nullable)",
            "LowCardinality"
        ))
    ));

    Ok(())
}
