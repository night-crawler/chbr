use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists plain_lc_string;

create table plain_lc_string
(
    id     Int64,
    lc_str LowCardinality(String)
) engine = MergeTree order by tuple();

insert into plain_lc_string (id, lc_str) values
    (0, 'apple'),
    (1, 'banana'),
    (2, 'cherry'),
    (3, 'date'),
    (4, 'elderberry'),
    (5, 'fig');

select * from plain_lc_string order by id format Native;
"#;

#[test]
fn lc_string() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("plain_lc_string.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,apple
    // 1,banana
    // 2,cherry
    // 3,date
    // 4,elderberry
    // 5,fig

    let expected_strings = ["apple", "banana", "cherry", "date", "elderberry", "fig"];

    let strings_marker = &block.markers[1];
    for (i, expected) in expected_strings.iter().enumerate() {
        let value: &str = strings_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
