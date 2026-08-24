mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Array<'a, I64<'a>>>,
}

#[test]
fn reads_nested_integer_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("array_in_array_in64.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![vec![11, 22, 22, 77, 123], vec![333, 41]],
        vec![vec![11, 22], vec![7, 844, 12, 12, 0], vec![5, 5, 5]],
        vec![vec![9], vec![10, 11]],
        vec![vec![123, 134], vec![145]],
        vec![vec![156]],
        vec![vec![]],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let actual = row
            .arr
            .map(|values| values?.try_collect_vec())
            .collect::<chbr::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
