use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, Str};

const _SQL: &str = r#"
drop table if exists plain_strings;

create table plain_strings
(
    id  Int64,
    str String
) engine = MergeTree order by tuple();

insert into plain_strings (id, str) values
    (0, 'hello'),
    (1, 'world'),
    (2, 'clickhouse'),
    (3, 'test'),
    (4, 'example'),
    (5, 'data');

select * from plain_strings order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "str")]
    value: Str<'a>,
}

#[test]
fn reads_string_column() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("plain_strings.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = ["hello", "world", "clickhouse", "test", "example", "data"];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.value, expected[index]);
    }
    Ok(())
}
