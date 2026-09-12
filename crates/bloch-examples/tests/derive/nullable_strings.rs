use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, Nullable, Str};

const _SQL: &str = r#"
drop table if exists nullable_string;

create table nullable_string
(
    id   Int64,
    nstr Nullable(String)
) engine = MergeTree order by tuple();

insert into nullable_string (id, nstr) values
    (0, 'hello'),
    (1, null),
    (2, 'world'),
    (3, 'clickhouse'),
    (4, null),
    (5, 'test');

select * from nullable_string order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    nstr: Nullable<'a, Str<'a>>,
}

#[test]
fn reads_nullable_strings() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("nullable_string.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        Some("hello"),
        None,
        Some("world"),
        Some("clickhouse"),
        None,
        Some("test"),
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.nstr, expected[index]);
    }
    Ok(())
}
