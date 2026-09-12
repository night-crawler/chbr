use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64};

const _SQL: &str = r#"
drop table if exists array_in_array_in64;

create table array_in_array_in64
(
    id  Int64,
    arr Array(Array(Int64))
) engine = MergeTree order by tuple();

insert into array_in_array_in64 (id, arr) values
    (0, [[11, 22, 22, 77, 123], [333, 41]]),
    (1, [[11, 22], [7, 844, 12, 12, 0], [5, 5, 5]]),
    (2, [[9], [10, 11]]),
    (3, [[123, 134], [145]]),
    (4, [[156]]),
    (5, [[]]);

select * from array_in_array_in64 order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Array<'a, I64<'a>>>,
}

#[test]
fn reads_nested_integer_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("array_in_array_in64.native"))?;
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
            .collect::<bloch::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
