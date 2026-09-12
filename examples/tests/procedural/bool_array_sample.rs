use bloch::parse::block::parse_single;
use bloch::value::BoolSliceIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn bool_array_sample() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("bool_array_sample.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,"[true, false, true]"
    // 1,"[false, false, true]"
    // 2,"[true, true, false]"
    // 3,"[false, true, false]"
    // 4,[]
    // 5,[true]

    let expected = [
        vec![true, false, true],
        vec![false, false, true],
        vec![true, true, false],
        vec![false, true, false],
        vec![],
        vec![true],
    ];
    let bool_array_marker = &block.markers[1];
    for (i, expected) in expected.iter().enumerate() {
        let value: BoolSliceIterator = bool_array_marker.get(i)?.unwrap().try_into()?;
        let mut actual = vec![];
        for b in value {
            actual.push(b);
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
