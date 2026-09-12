use bloch::parse::block::parse_single;
use bloch::value::LowCardinalitySliceIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn lc_array_string() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("array_lc_string.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,"['apple', 'banana', 'cherry']"
    // 1,"['date', 'elderberry']"
    // 2,"['fig', 'grape', 'honeydew']"
    // 3,['kiwi']
    // 4,[]
    // 5,"['lemon', 'mango']"
    // 6,"['apple', 'banana', 'cherry', 'date']"
    // 7,"['elderberry', 'fig', 'grape']"
    // 8,"['honeydew', 'kiwi', 'lemon']"
    // 9,"['mango', 'apple', 'banana']"
    // 10,"['cherry', 'date', 'elderberry']"
    // 11,"['fig', 'grape', 'honeydew', 'kiwi']"

    let expected_arrays = [
        vec!["apple", "banana", "cherry"],
        vec!["date", "elderberry"],
        vec!["fig", "grape", "honeydew"],
        vec!["kiwi"],
        vec![],
        vec!["lemon", "mango"],
        vec!["apple", "banana", "cherry", "date"],
        vec!["elderberry", "fig", "grape"],
        vec!["honeydew", "kiwi", "lemon"],
        vec!["mango", "apple", "banana"],
        vec!["cherry", "date", "elderberry"],
        vec!["fig", "grape", "honeydew", "kiwi"],
    ];

    let strings_marker = &block.markers[1];
    for (i, expected) in expected_arrays.iter().enumerate() {
        let it: LowCardinalitySliceIterator = strings_marker.get(i)?.unwrap().try_into()?;
        let mut actual = vec![];
        for value in it {
            let value: &str = value?.try_into()?;
            actual.push(value);
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");

        let actual = strings_marker
            .get_array_lc_strs(i)?
            .unwrap()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            actual, *expected,
            "Mismatch at index {i} (get_array_lc_strs)"
        );
    }

    Ok(())
}
