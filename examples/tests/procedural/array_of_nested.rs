use bloch::parse::block::parse_single;
use bloch::value::{NestedIterator, NestedSliceIterator};
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
set flatten_nested = 0;

drop table if exists array_of_nested;

create table array_of_nested
(
    id  Int64,
    arr Array(Nested(child_id UInt64, child_name String))
) engine = MergeTree order by tuple();

insert into array_of_nested (id, arr) values
    (0, [[(1, 'Alice'), (2, 'Bob')]]),
    (1, [[(3, 'Charlie'), (4, 'Diana')]]),
    (2, [[(5, 'Eve')]]),
    (3, [[]]),
    (4, [[(6, 'Frank'), (7, 'Grace')]]),
    (5, [[(8, 'Heidi')]]);

select * from array_of_nested order by id format Native;
"#;

#[test]
fn array_of_nested() -> TestResult {
    let data = std::fs::read(crate::common::fixture("array_of_nested.native"))?;
    let (_, block) = parse_single(&data)?;

    let expected: [Vec<Vec<(i64, &str)>>; 6] = [
        vec![vec![(1, "Alice"), (2, "Bob")]],
        vec![vec![(3, "Charlie"), (4, "Diana")]],
        vec![vec![(5, "Eve")]],
        vec![vec![]],
        vec![vec![(6, "Frank"), (7, "Grace")]],
        vec![vec![(8, "Heidi")]],
    ];

    let nested_mark = &block.markers[1];

    for (row_idx, expected_outer) in expected.iter().enumerate() {
        let outer_slice: NestedSliceIterator = nested_mark.get(row_idx)?.unwrap().try_into()?;

        let mut actual_outer = Vec::<Vec<(i64, &str)>>::new();

        for nested_res in outer_slice {
            let nested_iter: NestedIterator = nested_res?;

            let mut inner_rows = Vec::<(i64, &str)>::new();

            for nested_row in nested_iter {
                let (mut id, mut name) = (None, None);

                for field in nested_row {
                    let (field_name, field_value) = field?;
                    match field_name {
                        "child_id" => id = Some(field_value.try_into()?),
                        "child_name" => name = Some(field_value.try_into()?),
                        _ => {}
                    }
                }

                inner_rows.push((
                    id.expect("missing child_id"),
                    name.expect("missing child_name"),
                ));
            }

            actual_outer.push(inner_rows);
        }

        assert_eq!(
            actual_outer, *expected_outer,
            "Mismatch in Array(Nested) at top-level row {row_idx}"
        );
    }

    Ok(())
}
