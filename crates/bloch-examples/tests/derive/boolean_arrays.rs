use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, Bool, I64};

const _SQL: &str = r#"
drop table if exists bool_array_sample;

create table bool_array_sample
(
    id  Int64,
    arr Array(Boolean)
) engine = MergeTree order by tuple();

insert into bool_array_sample (id, arr) values
    (0, [true, false, true]),
    (1, [false, false, true]),
    (2, [true, true, false]),
    (3, [false, true, false]),
    (4, []),
    (5, [true]);

select * from bool_array_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Bool<'a>>,
}

#[test]
fn reads_boolean_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("bool_array_sample.native"))?;
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
