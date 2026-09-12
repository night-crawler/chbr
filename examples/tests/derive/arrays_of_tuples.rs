use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64, LcStr};

const _SQL: &str = r#"
drop table if exists array_of_tuples;

create table array_of_tuples
(
    id  Int64,
    arr Array(Tuple(LowCardinality(String), Int64))
) engine = MergeTree order by tuple();

insert into array_of_tuples (id, arr) values
    (0, [('apple', 1), ('banana', 2), ('cherry', 3)]),
    (1, [('date', 4), ('elderberry', 5)]),
    (2, [('fig', 6), ('grape', 7), ('honeydew', 8)]),
    (3, [('kiwi', 9)]),
    (4, []),
    (5, [('lemon', 10), ('mango', 11)]);

select * from array_of_tuples order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Fruit<'a> {
    name: LcStr<'a>,
    rank: I64<'a>,
}
#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Fruit<'a>>,
}

#[test]
fn reads_arrays_of_tuples() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("array_of_tuples.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![("apple", 1), ("banana", 2), ("cherry", 3)],
        vec![("date", 4), ("elderberry", 5)],
        vec![("fig", 6), ("grape", 7), ("honeydew", 8)],
        vec![("kiwi", 9)],
        vec![],
        vec![("lemon", 10), ("mango", 11)],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let actual = row
            .arr
            .map(|fruit| fruit.map(|fruit| (fruit.name, fruit.rank)))
            .collect::<bloch::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
