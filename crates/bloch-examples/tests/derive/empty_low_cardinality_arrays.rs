use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64, Value};

const _SQL: &str = r#"
drop table if exists array_lc_string_empty;

create table array_lc_string_empty
(
    id  Int64,
    arr Array(LowCardinality(String)) default []
) engine = MergeTree order by tuple();

insert into array_lc_string_empty (id, arr) values
    (0, []),
    (1, []),
    (2, []),
    (3, []),
    (4, []);

select * from array_lc_string_empty order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
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
