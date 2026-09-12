use bloch::parse::block::parse_single;
use bloch::value::TupleSliceIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists array_of_tuples;

create table array_of_tuples
(
    id  Int64,
    arr Array(Tuple(LowCardinality(String), Int64))
) engine = MergeTree order by tuple();

insert into array_of_tuples (id, arr) values
    (0, [('apple', 1), ('banana', 2), ('cherry', 3)]),
    (1, [('date', 4), ('elderberry', 5)]),
    (2, [('fig', 6), ('grape', 7), ('honeydew', 8)]),
    (3, [('kiwi', 9)]),
    (4, []),
    (5, [('lemon', 10), ('mango', 11)]);

select * from array_of_tuples order by id format Native;
"#;

#[test]
fn array_of_tuples() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("array_of_tuples.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,"[('apple', 1), ('banana', 2), ('cherry', 3)]"
    // 1,"[('date', 4), ('elderberry', 5)]"
    // 2,"[('fig', 6), ('grape', 7), ('honeydew', 8)]"
    // 3,"[('kiwi', 9)]"
    // 4,[]
    // 5,"[('lemon', 10), ('mango', 11)]"

    let expected_arrays = [
        vec![("apple", 1), ("banana", 2), ("cherry", 3)],
        vec![("date", 4), ("elderberry", 5)],
        vec![("fig", 6), ("grape", 7), ("honeydew", 8)],
        vec![("kiwi", 9)],
        vec![],
        vec![("lemon", 10), ("mango", 11)],
    ];

    let tuples_marker = &block.markers[1];

    for (i, expected) in expected_arrays.iter().enumerate() {
        let slice: TupleSliceIterator = tuples_marker.get(i)?.unwrap().try_into()?;
        let mut actual = vec![];
        for tup in slice {
            let (s, n): (&str, i64) = tup.try_into()?;
            actual.push((s, n));
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
