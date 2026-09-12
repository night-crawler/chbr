use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Enum8, Enum16, I64};

const _SQL: &str = r#"
drop table if exists enums_sample;

create table enums_sample
(
    id  Int64,
    e8  Enum8('Red' = 11, 'Green' = 2, 'Blue' = -23),
    e16 Enum16('Foo' = 2000, 'Bar' = 200)
) engine = MergeTree order by tuple();

insert into enums_sample (id, e8, e16) values
    (0, 'Red', 'Foo'),
    (1, 'Green', 'Bar'),
    (2, 'Blue', 'Foo'),
    (3, 'Red', 'Bar'),
    (4, 'Green', 'Foo'),
    (5, 'Blue', 'Bar');

select * from enums_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    e8: Enum8<'a>,
    e16: Enum16<'a>,
}

#[test]
fn reads_enum_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("enums_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected8 = ["Red", "Green", "Blue", "Red", "Green", "Blue"];
    let expected16 = ["Foo", "Bar", "Foo", "Bar", "Foo", "Bar"];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!((row.e8, row.e16), (expected8[index], expected16[index]));
    }
    Ok(())
}
