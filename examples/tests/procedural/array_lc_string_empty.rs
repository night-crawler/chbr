use bloch::parse::block::parse_single;
use testresult::TestResult;

#[test]
fn array_lc_string_empty() -> TestResult {
    let data = std::fs::read(crate::common::fixture("array_lc_string_empty.native"))?;
    let (_, block) = parse_single(&data)?;

    let marker = &block.markers[1];
    for i in 0..block.num_rows {
        let mut it = marker
            .get_array_lc_strs(i)?
            .expect("expected to get an iterator");
        assert!(it.next().is_none(), "expected iterator to yield no items");
    }

    Ok(())
}
