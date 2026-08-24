mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, Bf16, I64};
use half::bf16;

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr_bf16: Array<'a, Bf16<'a>>,
}

#[test]
fn reads_bfloat16_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("bfloat16_array_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![3.125, 2.703125, 1.40625],
        vec![0.57421875, 1.6171875],
        vec![2.234375],
        vec![],
        vec![1.4140625, 3.140625],
    ]
    .map(|values| values.into_iter().map(bf16::from_f32).collect::<Vec<_>>());
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.arr_bf16.try_collect_vec()?, expected[index]);
    }
    Ok(())
}
