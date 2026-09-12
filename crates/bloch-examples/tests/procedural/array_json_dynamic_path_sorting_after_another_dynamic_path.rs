//! JSON columns whose typed, dynamic, and array-valued paths are declared in different orders
//! all iterate their paths in name order with the right values.

use bloch::parse::block::parse_single;
use bloch::reader::JsonIterator;
use testresult::TestResult;

const _SQL: &str = r#"
set enable_json_type = 1;

select CAST('{"b":[{"x":1}],"a":2}', 'JSON') as j format Native;
"#;

fn paths_of_row0(data: &[u8]) -> TestResult<Vec<String>> {
    let (_, block) = parse_single(data)?;
    let value = block.markers[0].get(0)?.expect("row 0");
    let it = JsonIterator::try_from(value)?;
    let mut out = Vec::new();
    for r in it {
        let (path, value) = r?;
        out.push(format!("{path}={value:?}"));
    }
    Ok(out)
}

#[test]
fn array_json_dynamic_path_sorting_after_another_dynamic_path() -> TestResult {
    let data = std::fs::read(crate::common::fixture("json_array_path_last.native"))?;
    let paths = paths_of_row0(&data)?;
    assert_eq!(paths.len(), 2, "{paths:?}");
    assert!(paths[0].starts_with("a=Int64(2)"), "{paths:?}");
    Ok(())
}
