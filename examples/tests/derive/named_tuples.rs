use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, Str};

const _SQL: &str = r#"
drop table if exists named_tuple;

create table named_tuple
(
    id  Int64,
    tup Tuple(name String, rank Int64)
) engine = MergeTree order by tuple();

insert into named_tuple (id, tup) values
    (0, ('apple', 0)),
    (1, ('banana', 10)),
    (2, ('cherry', 20)),
    (3, ('date', 30)),
    (4, ('elderberry', 40)),
    (5, ('fig', 50));

select * from named_tuple order by id format Native;
"#;

// Field order deliberately differs from the ClickHouse tuple definition.
// Named tuple fields are resolved through `#[col(name = ...)]`.
#[derive(FromBlock, Copy, Clone)]
struct Fruit<'a> {
    rank: I64<'a>,
    #[col(name = "name")]
    title: Str<'a>,
}

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    tup: Fruit<'a>,
}

#[test]
fn reads_named_tuple_fields_by_name() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("named_tuple.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected_titles = ["apple", "banana", "cherry", "date", "elderberry", "fig"];

    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.tup.title, expected_titles[index]);
        assert_eq!(row.tup.rank, row.id * 10);
    }

    Ok(())
}
