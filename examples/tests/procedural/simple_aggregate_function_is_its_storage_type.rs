use bloch::parse::block::parse_single;
use bloch::value::Value;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists simple_agg;

create table simple_agg
(
    x SimpleAggregateFunction(sum, UInt64),
    y SimpleAggregateFunction(anyLast, Nullable(String))
) engine = AggregatingMergeTree order by tuple();

insert into simple_agg values (7, 'a');

select * from simple_agg format Native;
"#;

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
