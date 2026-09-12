use bloch::parse::block::parse_single;
use bloch::value::BoolSliceIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn bool_nonzero_bytes() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("bool_nonzero_bytes.native"))?;
    let (_, block) = parse_single(&buf)?;

    // `b` holds raw bytes 0x00..0x03; `arr` holds [id, 0x00, 0xff].
    // ClickHouse treats every non-zero byte as `true`.
    let expected = [
        (false, vec![false, false, true]),
        (true, vec![true, false, true]),
        (true, vec![true, false, true]),
        (true, vec![true, false, true]),
    ];
    let b = &block.markers[1];
    let arr = &block.markers[2];
    for (i, (scalar, slice)) in expected.iter().enumerate() {
        assert_eq!(
            b.get_bool(i)?,
            Some(*scalar),
            "get_bool mismatch at index {i}"
        );
        let value: bool = b.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *scalar, "Value::Bool mismatch at index {i}");

        let iter: BoolSliceIterator = arr.get(i)?.unwrap().try_into()?;
        assert_eq!(
            iter.collect::<Vec<_>>(),
            *slice,
            "BoolSliceIterator mismatch at index {i}"
        );
        let iter = arr.get_arr_bool_iter(i)?.unwrap();
        assert_eq!(
            iter.collect::<Vec<_>>(),
            *slice,
            "get_arr_bool_iter mismatch at index {i}"
        );
    }

    Ok(())
}
