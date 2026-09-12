use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64, Str};

const _SQL: &str = r#"
drop table if exists plain_strings_array;

create table plain_strings_array
(
    id  Int64,
    arr Array(String)
) engine = MergeTree order by tuple();

insert into plain_strings_array (id, arr) values
    (0, ['apple', 'banana', 'cherry']),
    (1, ['date', 'elderberry']),
    (2, ['fig', 'grape', 'honeydew']),
    (3, ['kiwi']),
    (4, []),
    (5, ['lemon', 'mango']);

select * from plain_strings_array order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Str<'a>>,
}

#[test]
fn reads_string_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("plain_strings_array.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec!["apple", "banana", "cherry"],
        vec!["date", "elderberry"],
        vec!["fig", "grape", "honeydew"],
        vec!["kiwi"],
        vec![],
        vec!["lemon", "mango"],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.arr.try_collect_vec()?, expected[index]);
    }
    Ok(())
}
