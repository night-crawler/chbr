use bloch::BStr;
use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

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

#[test]
fn plain_strings_array() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("plain_strings_array.native"))?;

    // 0,"['apple', 'banana', 'cherry']"
    // 1,"['date', 'elderberry']"
    // 2,"['fig', 'grape', 'honeydew']"
    // 3,['kiwi']
    // 4,[]
    // 5,"['lemon', 'mango']"

    let (_, block) = parse_single(&buf)?;

    let expected_arrays = [
        vec!["apple", "banana", "cherry"],
        vec!["date", "elderberry"],
        vec!["fig", "grape", "honeydew"],
        vec!["kiwi"],
        vec![],
        vec!["lemon", "mango"],
    ];

    let strings_marker = &block.markers[1];

    for (i, expected) in expected_arrays.iter().enumerate() {
        let slice: &[&BStr] = strings_marker.get(i)?.unwrap().try_into()?;
        let actual = slice.to_vec();

        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
