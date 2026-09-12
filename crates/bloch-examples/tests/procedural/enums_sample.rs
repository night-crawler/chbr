use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

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

#[test]
fn enums_sample() -> TestResult {
    let data = std::fs::read(crate::common::fixture("enums_sample.native"))?;
    let (_, block) = parse_single(&data)?;

    // 0,Red,Foo
    // 1,Green,Bar
    // 2,Blue,Foo
    // 3,Red,Bar
    // 4,Green,Foo
    // 5,Blue,Bar

    let expected_e8 = ["Red", "Green", "Blue", "Red", "Green", "Blue"];

    let e8_marker = &block.markers[1];
    for (i, expected) in expected_e8.iter().enumerate() {
        let value: &str = e8_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let expected_e16 = ["Foo", "Bar", "Foo", "Bar", "Foo", "Bar"];
    let e16_marker = &block.markers[2];
    for (i, expected) in expected_e16.iter().enumerate() {
        let value: &str = e16_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
