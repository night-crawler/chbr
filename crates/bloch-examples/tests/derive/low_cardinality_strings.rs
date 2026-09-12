use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, LcStr};

const _SQL: &str = r#"
drop table if exists plain_lc_string;

create table plain_lc_string
(
    id     Int64,
    lc_str LowCardinality(String)
) engine = MergeTree order by tuple();

insert into plain_lc_string (id, lc_str) values
    (0, 'apple'),
    (1, 'banana'),
    (2, 'cherry'),
    (3, 'date'),
    (4, 'elderberry'),
    (5, 'fig');

select * from plain_lc_string order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    lc_str: LcStr<'a>,
}

#[test]
fn reads_low_cardinality_strings() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("plain_lc_string.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = ["apple", "banana", "cherry", "date", "elderberry", "fig"];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.lc_str, expected[index]);
    }
    Ok(())
}
