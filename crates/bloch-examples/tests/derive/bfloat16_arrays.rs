use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, Bf16, I64};
use half::bf16;

const _SQL: &str = r#"
drop table if exists bfloat16_array_sample;

create table bfloat16_array_sample
(
    id       Int64,
    arr_bf16 Array(BFloat16)
) engine = MergeTree order by tuple();

insert into bfloat16_array_sample (id, arr_bf16) values
    (0, [3.14, 2.71, 1.41]),
    (1, [0.57721, 1.61803]),
    (2, [2.23607]),
    (3, []),
    (4, [1.41421, 3.14159]);

select * from bfloat16_array_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    arr_bf16: Array<'a, Bf16<'a>>,
}

#[test]
fn reads_bfloat16_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("bfloat16_array_sample.native"))?;
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
