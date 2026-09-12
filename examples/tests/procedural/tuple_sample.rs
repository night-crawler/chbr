use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists tuple_sample;

create table tuple_sample
(
    id  Int64,
    tup Tuple(Int64, String)
) engine = MergeTree order by tuple();

insert into tuple_sample (id, tup) values
    (0, (1, 'a')),
    (1, (3, 'ab')),
    (2, (7, 'ac')),
    (3, (9, 'ad')),
    (4, (11, 'ae')),
    (5, (2, 'af')),
    (6, (3, 'ag'));

select * from tuple_sample order by id format Native;
"#;

#[test]
fn tuple_sample() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("tuple.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,"(1, 'a')"
    // 1,"(3, 'ab')"
    // 2,"(7, 'ac')"
    // 3,"(9, 'ad')"
    // 4,"(11, 'ae')"
    // 5,"(2, 'af')"
    // 6,"(3, 'ag')"

    let expected_tuples = [
        (1, "a"),
        (3, "ab"),
        (7, "ac"),
        (9, "ad"),
        (11, "ae"),
        (2, "af"),
        (3, "ag"),
    ];

    let tuples_marker = &block.markers[1];

    for (i, expected) in expected_tuples.iter().enumerate() {
        let value: (i64, &str) = tuples_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
