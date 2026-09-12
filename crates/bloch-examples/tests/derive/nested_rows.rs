use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64, Str, U64};

const _SQL: &str = r#"
set flatten_nested = 0;

drop table if exists simple_nested;

create table simple_nested
(
    id  Int64,
    nes Nested(child_id UInt64, child_name String)
) engine = MergeTree order by tuple();

insert into simple_nested (id, nes) values
    (0, [(1, 'Alice'), (2, 'Bob')]),
    (1, [(3, 'Charlie'), (4, 'Diana')]),
    (2, [(5, 'Eve')]),
    (3, []),
    (4, [(6, 'Frank'), (7, 'Grace')]),
    (5, [(8, 'Heidi')]);

select * from simple_nested order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Child<'a> {
    child_id: U64<'a>,
    child_name: Str<'a>,
}
#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    nes: Array<'a, Child<'a>>,
}

#[test]
fn reads_nested_rows() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("simple_nested.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![(1, "Alice"), (2, "Bob")],
        vec![(3, "Charlie"), (4, "Diana")],
        vec![(5, "Eve")],
        vec![],
        vec![(6, "Frank"), (7, "Grace")],
        vec![(8, "Heidi")],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let actual = row
            .nes
            .map(|child| child.map(|child| (child.child_id, child.child_name)))
            .collect::<bloch::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
