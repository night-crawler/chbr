mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, I64<'a>>,
}

#[test]
fn reads_integer_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("array.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected_ids = [
        0, 128969003, 214500519, 301458964, 475251162, 1228122092, 1873422981, 2172352370,
        2181458171, 2793473513, 3697287021,
    ];
    let expected_arrays = [
        vec![],
        vec![1],
        vec![1],
        vec![],
        vec![],
        vec![1, 2, 3, 4, 5],
        vec![1, 2, 3, 4],
        vec![1, 2, 3],
        vec![1, 2],
        vec![],
        vec![1, 2, 3],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, expected_ids[index]);
        assert_eq!(row.arr.try_collect_vec()?, expected_arrays[index]);
    }
    Ok(())
}
