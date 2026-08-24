mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, Bool, I64};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Bool<'a>>,
}

#[test]
fn reads_boolean_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("bool_array_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![true, false, true],
        vec![false, false, true],
        vec![true, true, false],
        vec![false, true, false],
        vec![],
        vec![true],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.arr.try_collect_vec()?, expected[index]);
    }
    Ok(())
}
