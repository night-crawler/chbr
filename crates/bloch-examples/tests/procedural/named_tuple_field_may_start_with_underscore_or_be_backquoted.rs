//! Type names as ClickHouse emits them in `Native` block headers.

use bloch::mark::Mark;
use bloch::parse::block::parse_single;
use testresult::TestResult;

const _SQL: &str = r#"
select
    tuple(1)::Tuple(_id UInt64)                            as t1,
    tuple(2, 'x')::Tuple(`my field` UInt64, `1x` String)   as t2
format Native;
"#;

#[test]
fn named_tuple_field_may_start_with_underscore_or_be_backquoted() -> TestResult {
    let data = std::fs::read(crate::common::fixture("named_tuple_quoted.native"))?;
    let (_, block) = parse_single(&data)?;
    let Mark::NamedTuple(t1) = &block.markers[0] else {
        panic!("t1 is not a NamedTuple: {:?}", block.markers[0]);
    };
    assert_eq!(&*t1.col_names, ["_id"]);
    let Mark::NamedTuple(t2) = &block.markers[1] else {
        panic!("t2 is not a NamedTuple: {:?}", block.markers[1]);
    };
    assert_eq!(&*t2.col_names, ["my field", "1x"]);
    Ok(())
}
