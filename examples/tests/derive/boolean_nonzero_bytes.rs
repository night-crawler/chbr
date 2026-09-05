use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, Bool, U64};

#[derive(FromBlock)]
struct Row<'a> {
    id: U64<'a>,
    b: Bool<'a>,
    arr: Array<'a, Bool<'a>>,
}

#[test]
fn reads_nonzero_bytes_as_true() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("bool_nonzero_bytes.native"))?;
    let (_, block) = parse_single(&data)?;
    // `b` holds raw bytes 0x00..0x03; `arr` holds [id, 0x00, 0xff].
    // ClickHouse treats every non-zero byte as `true`.
    let expected = [
        (false, vec![false, false, true]),
        (true, vec![true, false, true]),
        (true, vec![true, false, true]),
        (true, vec![true, false, true]),
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, u64::try_from(index)?);
        assert_eq!(row.b, expected[index].0, "b mismatch at index {index}");
        assert_eq!(
            row.arr.try_collect_vec()?,
            expected[index].1,
            "arr mismatch at index {index}"
        );
    }
    Ok(())
}
