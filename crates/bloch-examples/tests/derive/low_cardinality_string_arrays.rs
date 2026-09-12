use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64, LcStr};

const _SQL: &str = r#"
drop table if exists array_lc_string;

create table array_lc_string
(
    id  Int64,
    arr Array(LowCardinality(String))
) engine = MergeTree order by tuple();

insert into array_lc_string (id, arr) values
    (0, ['apple', 'banana', 'cherry']),
    (1, ['date', 'elderberry']),
    (2, ['fig', 'grape', 'honeydew']),
    (3, ['kiwi']),
    (4, []),
    (5, ['lemon', 'mango']),
    (6, ['apple', 'banana', 'cherry', 'date']),
    (7, ['elderberry', 'fig', 'grape']),
    (8, ['honeydew', 'kiwi', 'lemon']),
    (9, ['mango', 'apple', 'banana']),
    (10, ['cherry', 'date', 'elderberry']),
    (11, ['fig', 'grape', 'honeydew', 'kiwi']);

select * from array_lc_string order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, LcStr<'a>>,
}

#[test]
fn reads_low_cardinality_string_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("array_lc_string.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec!["apple", "banana", "cherry"],
        vec!["date", "elderberry"],
        vec!["fig", "grape", "honeydew"],
        vec!["kiwi"],
        vec![],
        vec!["lemon", "mango"],
        vec!["apple", "banana", "cherry", "date"],
        vec!["elderberry", "fig", "grape"],
        vec!["honeydew", "kiwi", "lemon"],
        vec!["mango", "apple", "banana"],
        vec!["cherry", "date", "elderberry"],
        vec!["fig", "grape", "honeydew", "kiwi"],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.arr.try_collect_vec()?, expected[index]);
    }
    Ok(())
}
