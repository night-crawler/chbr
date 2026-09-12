//! Type names as ClickHouse emits them in `Native` block headers.

use bloch::parse::block::parse_single;
use testresult::TestResult;

const _SQL: &str = r#"
select
    [] as a,
    NULL as n
format Native;
"#;

#[test]
fn array_nothing_and_nullable_nothing_parse() -> TestResult {
    let data = std::fs::read(crate::common::fixture("nothing_scalar.native"))?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.num_rows, 1);
    assert_eq!(block.markers.len(), 2);
    Ok(())
}
