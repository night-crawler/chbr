//! JSON columns whose typed, dynamic, and array-valued paths are declared in different orders
//! all iterate their paths in name order with the right values.

use chbr::parse::block::parse_single;
use chbr::reader::JsonIterator;
use chbr::value::Value;
use testresult::TestResult;

fn load(name: &str) -> std::io::Result<Vec<u8>> {
    std::fs::read(crate::common::fixture(name))
}

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
fn typed_low_cardinality_path_followed_by_dynamic_path() -> TestResult {
    let data = load("json_typed_lc_dynamic.native")?;
    let (_, block) = parse_single(&data)?;
    let value = block.markers[0].get(0)?.expect("row 0");
    let it = JsonIterator::try_from(value)?;
    let mut seen = Vec::new();
    for r in it {
        let (path, value) = r?;
        match (path, value) {
            ("a.b", Value::String(s)) => {
                assert_eq!(s, "x");
                seen.push("a.b");
            }
            ("c", Value::Int64(1)) => seen.push("c"),
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(seen, ["a.b", "c"]);
    Ok(())
}

#[test]
fn array_json_dynamic_path_sorting_before_another_dynamic_path() -> TestResult {
    let data = load("json_array_path_first.native")?;
    let paths = paths_of_row0(&data)?;
    assert_eq!(paths.len(), 2, "{paths:?}");
    assert!(paths[1].starts_with("b=Int64(2)"), "{paths:?}");
    Ok(())
}

#[test]
fn array_json_dynamic_path_sorting_after_another_dynamic_path() -> TestResult {
    let data = load("json_array_path_last.native")?;
    let paths = paths_of_row0(&data)?;
    assert_eq!(paths.len(), 2, "{paths:?}");
    assert!(paths[0].starts_with("a=Int64(2)"), "{paths:?}");
    Ok(())
}
