use bloch::parse::block::parse_single;
use bloch::value::ArraySliceIterator;
use bloch::zc;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists array_in_array_in64;

create table array_in_array_in64
(
    id  Int64,
    arr Array(Array(Int64))
) engine = MergeTree order by tuple();

insert into array_in_array_in64 (id, arr) values
    (0, [[11, 22, 22, 77, 123], [333, 41]]),
    (1, [[11, 22], [7, 844, 12, 12, 0], [5, 5, 5]]),
    (2, [[9], [10, 11]]),
    (3, [[123, 134], [145]]),
    (4, [[156]]),
    (5, [[]]);

select * from array_in_array_in64 order by id format Native;
"#;

#[test]
fn array_in_array_in64() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("array_in_array_in64.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,"[[11, 22, 22, 77, 123], [333, 41]]"
    // 1,"[[11, 22], [7, 844, 12, 12, 0], [5, 5, 5]]"
    // 2,"[[9], [10, 11]]"
    // 3,"[[123, 134], [145]]"
    // 4,[[156]]
    // 5,[[]]

    let expected_arrays = [
        vec![vec![11, 22, 22, 77, 123], vec![333, 41]],
        vec![vec![11, 22], vec![7, 844, 12, 12, 0], vec![5, 5, 5]],
        vec![vec![9], vec![10, 11]],
        vec![vec![123, 134], vec![145]],
        vec![vec![156]],
        vec![vec![]],
    ];

    let arrs_marker = &block.markers[1];

    for (i, expected) in expected_arrays.iter().enumerate() {
        let v = arrs_marker.get(i)?.unwrap();
        let outer: ArraySliceIterator<&[zc::I64]> = v.try_into()?;
        let mut actual_outer = vec![];
        for slice in outer.flatten() {
            let inner = slice.iter().map(|&v| v.get()).collect::<Vec<_>>();
            actual_outer.push(inner);
        }

        assert_eq!(actual_outer, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
