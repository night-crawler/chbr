use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, Str, Tuple};

const _SQL: &str = r#"
drop table if exists tuple_sample;

create table tuple_sample
(
    id  Int64,
    tup Tuple(Int64, String)
) engine = MergeTree order by tuple();

insert into tuple_sample (id, tup) values
    (0, (1, 'a')),
    (1, (3, 'ab')),
    (2, (7, 'ac')),
    (3, (9, 'ad')),
    (4, (11, 'ae')),
    (5, (2, 'af')),
    (6, (3, 'ag'));

select * from tuple_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    tup: Tuple<(I64<'a>, Str<'a>)>,
}

#[test]
fn reads_tuple_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("tuple.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        (1, "a"),
        (3, "ab"),
        (7, "ac"),
        (9, "ad"),
        (11, "ae"),
        (2, "af"),
        (3, "ag"),
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.tup, expected[index]);
    }
    Ok(())
}
