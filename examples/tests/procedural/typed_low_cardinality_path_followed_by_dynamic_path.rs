//! JSON columns whose typed, dynamic, and array-valued paths are declared in different orders
//! all iterate their paths in name order with the right values.

use bloch::parse::block::parse_single;
use bloch::reader::JsonIterator;
use bloch::value::Value;
use testresult::TestResult;

#[test]
fn typed_low_cardinality_path_followed_by_dynamic_path() -> TestResult {
    let data = std::fs::read(crate::common::fixture("json_typed_lc_dynamic.native"))?;
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
