use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64, Str, U64};

const _SQL: &str = r#"
set flatten_nested = 0;

drop table if exists array_of_nested;

create table array_of_nested
(
    id  Int64,
    arr Array(Nested(child_id UInt64, child_name String))
) engine = MergeTree order by tuple();

insert into array_of_nested (id, arr) values
    (0, [[(1, 'Alice'), (2, 'Bob')]]),
    (1, [[(3, 'Charlie'), (4, 'Diana')]]),
    (2, [[(5, 'Eve')]]),
    (3, [[]]),
    (4, [[(6, 'Frank'), (7, 'Grace')]]),
    (5, [[(8, 'Heidi')]]);

select * from array_of_nested order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Child<'a> {
    child_id: U64<'a>,
    child_name: Str<'a>,
}
#[derive(FromBlock, Copy, Clone)]
struct Row<'a, C>
where
    C: bloch::reader::TryRead<'a> + 'a,
{
    id: I64<'a>,
    arr: Array<'a, Array<'a, C>>,
}

#[test]
fn reads_arrays_of_nested_rows() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("array_of_nested.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![vec![(1, "Alice"), (2, "Bob")]],
        vec![vec![(3, "Charlie"), (4, "Diana")]],
        vec![vec![(5, "Eve")]],
        vec![vec![]],
        vec![vec![(6, "Frank"), (7, "Grace")]],
        vec![vec![(8, "Heidi")]],
    ];
    for (index, row) in Row::<Child>::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let actual = row
            .arr
            .map(|children| {
                children?
                    .map(|child| child.map(|child| (child.child_id, child.child_name)))
                    .collect::<bloch::Result<Vec<_>>>()
            })
            .collect::<bloch::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
