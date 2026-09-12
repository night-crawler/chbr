use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists plain_strings;

create table plain_strings
(
    id  Int64,
    str String
) engine = MergeTree order by tuple();

insert into plain_strings (id, str) values
    (0, 'hello'),
    (1, 'world'),
    (2, 'clickhouse'),
    (3, 'test'),
    (4, 'example'),
    (5, 'data');

select * from plain_strings order by id format Native;
"#;

#[test]
fn plain_strings() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("plain_strings.native"))?;
    // 0,hello
    // 1,world
    // 2,clickhouse
    // 3,test
    // 4,example
    // 5,data

    let (_, block) = parse_single(&buf)?;

    let expected_strings = ["hello", "world", "clickhouse", "test", "example", "data"];

    let strings_marker = &block.markers[1];
    for (i, expected) in expected_strings.iter().enumerate() {
        let value: &str = strings_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
