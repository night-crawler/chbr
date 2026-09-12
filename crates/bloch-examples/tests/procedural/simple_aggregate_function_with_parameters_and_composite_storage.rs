use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use bloch::zc;
use testresult::TestResult;

const _SQL: &str = r#"
select
    CAST([1, 2], 'SimpleAggregateFunction(groupArrayArray(3), Array(UInt64))') as a,
    CAST('a', 'SimpleAggregateFunction(anyLast, LowCardinality(String))') as lc,
    CAST(map('k', 5), 'SimpleAggregateFunction(sumMap, Map(String, UInt64))') as m
format Native;
"#;

/// `f` may carry parameters (`groupArrayArray(3)`), and `T` may be `LowCardinality(..)` or
/// `Map(..)`.
#[test]
fn simple_aggregate_function_with_parameters_and_composite_storage() -> TestResult {
    let data = std::fs::read(crate::common::fixture("simple_aggregate_parametric.native"))?;
    let (_, block) = parse_single(&data)?;

    let a: &[zc::U64] = block.mark("a")?.get(0)?.unwrap().try_into()?;
    assert_eq!(a.iter().map(|v| v.get()).collect::<Vec<_>>(), [1, 2]);

    assert_eq!(
        block.mark("lc")?.get_str(0)?.map(|s| &**s),
        Some(b"a".as_slice())
    );

    let m: MapIterator<&str, u64> = block.mark("m")?.get(0)?.unwrap().try_into()?;
    assert_eq!(m.collect::<Result<Vec<_>, _>>()?, [("k", 5)]);
    Ok(())
}
