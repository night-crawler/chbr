use bloch::parse::block::parse_single;
use bloch::value::Value;
use testresult::TestResult;

/// `SimpleAggregateFunction(f, T)` is read as a plain `T`.
#[test]
fn simple_aggregate_function_is_its_storage_type() -> TestResult {
    let data = std::fs::read(crate::common::fixture("simple_aggregate.native"))?;
    let (_, block) = parse_single(&data)?;
    assert!(matches!(block.mark("x")?.get(0)?, Some(Value::UInt64(7))));
    let Some(Value::String(y)) = block.mark("y")?.get(0)? else {
        panic!("y");
    };
    assert_eq!(y, "a");
    Ok(())
}
