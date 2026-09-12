use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64, Nullable, Str};

const _SQL: &str = r#"
drop table if exists nullable_string_array;

create table nullable_string_array
(
    id  Int64,
    arr Array(Nullable(String))
) engine = MergeTree order by tuple();

insert into nullable_string_array (id, arr) values
    (0, ['apple', 'banana', null]),
    (1, [null, 'date', 'elderberry']),
    (2, ['fig', null, 'honeydew']),
    (3, [null]),
    (4, []),
    (5, ['lemon', null, 'mango']);

select * from nullable_string_array order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Nullable<'a, Str<'a>>>,
}

#[test]
fn reads_nullable_string_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("nullable_string_array.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![Some("apple"), Some("banana"), None],
        vec![None, Some("date"), Some("elderberry")],
        vec![Some("fig"), None, Some("honeydew")],
        vec![None],
        vec![],
        vec![Some("lemon"), None, Some("mango")],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.arr.try_collect_vec()?, expected[index]);
    }
    Ok(())
}
