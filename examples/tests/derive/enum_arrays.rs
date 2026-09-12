use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, Enum8, Enum16, I64};

const _SQL: &str = r#"
drop table if exists enums_array_sample;

create table enums_array_sample
(
    id      Int64,
    arr_e8  Array(Enum8('Red' = 11, 'Green' = 2, 'Blue' = -23)),
    arr_e16 Array(Enum16('Foo' = 2000, 'Bar' = 200))
) engine = MergeTree order by tuple();

insert into enums_array_sample (id, arr_e8, arr_e16) values
    (0, ['Red', 'Green'], ['Foo']),
    (1, ['Blue', 'Red'], ['Bar']),
    (2, ['Green'], ['Foo', 'Bar']),
    (3, [], ['Foo']),
    (4, ['Red', 'Blue'], []),
    (5, ['Green', 'Red', 'Blue'], ['Bar']);

select * from enums_array_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    arr_e8: Array<'a, Enum8<'a>>,
    arr_e16: Array<'a, Enum16<'a>>,
}

#[test]
fn reads_enum_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("enums_array_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected8 = [
        vec!["Red", "Green"],
        vec!["Blue", "Red"],
        vec!["Green"],
        vec![],
        vec!["Red", "Blue"],
        vec!["Green", "Red", "Blue"],
    ];
    let expected16 = [
        vec!["Foo"],
        vec!["Bar"],
        vec!["Foo", "Bar"],
        vec!["Foo"],
        vec![],
        vec!["Bar"],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.arr_e8.try_collect_vec()?, expected8[index]);
        assert_eq!(row.arr_e16.try_collect_vec()?, expected16[index]);
    }
    Ok(())
}
