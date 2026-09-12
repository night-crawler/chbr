//! Type names as ClickHouse emits them in `Native` block headers.

use bloch::Error;
use bloch::parse::block::parse_single;
use testresult::TestResult;

const _SQL: &str = r#"
select toIntervalDay(1) as i, sumState(toUInt64(1)) as s format Native;
"#;

#[test]
fn aggregate_function_state_is_not_implemented() -> TestResult {
    let data = std::fs::read(crate::common::fixture("interval_and_aggregate.native"))?;
    let Err(Error::NotImplemented(message)) = parse_single(&data) else {
        panic!("expected NotImplemented");
    };
    assert_eq!(
        message,
        "aggregate function state column AggregateFunction(sum, UInt64)"
    );
    Ok(())
}
