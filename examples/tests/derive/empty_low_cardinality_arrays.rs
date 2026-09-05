use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64, Value};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Value<'a>>,
}

#[test]
fn reads_empty_low_cardinality_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("array_lc_string_empty.native"))?;
    let (_, block) = parse_single(&data)?;
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert!(row.arr.try_collect_vec()?.is_empty());
    }
    Ok(())
}
