use bloch::parse::block::parse_single;
use bloch::value::NestedIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn simple_nested() -> TestResult {
    let data = std::fs::read(crate::common::fixture("simple_nested.native"))?;
    let (_, block) = parse_single(&data)?;

    let expected: [Vec<(i64, &str)>; 6] = [
        vec![(1, "Alice"), (2, "Bob")],
        vec![(3, "Charlie"), (4, "Diana")],
        vec![(5, "Eve")],
        vec![],
        vec![(6, "Frank"), (7, "Grace")],
        vec![(8, "Heidi")],
    ];

    let nested_marker = &block.markers[1];

    for (row_idx, expected_nested) in expected.iter().enumerate() {
        let nested_iter: NestedIterator = nested_marker.get(row_idx)?.unwrap().try_into()?;

        let mut actual_nested = Vec::<(i64, &str)>::new();
        for nested_row in nested_iter {
            let mut id: Option<i64> = None;
            let mut name: Option<&str> = None;

            for field in nested_row {
                let (field_name, field_value) = field?;
                match field_name {
                    "child_id" => id = Some(field_value.try_into()?),
                    "child_name" => name = Some(field_value.try_into()?),
                    _ => {}
                }
            }

            actual_nested.push((
                id.expect("missing child_id"),
                name.expect("missing child_name"),
            ));
        }

        assert_eq!(
            actual_nested, *expected_nested,
            "Mismatch in nested data at top-level row {row_idx}"
        );
    }

    Ok(())
}
