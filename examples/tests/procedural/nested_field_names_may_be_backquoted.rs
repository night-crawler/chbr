//! Type names as ClickHouse emits them in `Native` block headers.

use bloch::parse::block::parse_single;
use bloch::value::NestedIterator;
use testresult::TestResult;

#[test]
fn nested_field_names_may_be_backquoted() -> TestResult {
    let data = std::fs::read(crate::common::fixture("nested_quoted.native"))?;
    let (_, block) = parse_single(&data)?;
    let rows: NestedIterator = block.markers[0].get(0)?.unwrap().try_into()?;
    let mut actual = Vec::<(u64, &str)>::new();
    for row in rows {
        let (mut id, mut name) = (None, None);
        for field in row {
            let (field_name, field_value) = field?;
            match field_name {
                "my field" => id = Some(field_value.try_into()?),
                "1x" => name = Some(field_value.try_into()?),
                other => panic!("unexpected field {other:?}"),
            }
        }
        actual.push((id.expect("missing `my field`"), name.expect("missing `1x`")));
    }
    assert_eq!(actual, [(1, "x"), (2, "y")]);
    Ok(())
}
