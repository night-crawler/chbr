use bloch::parse::block::parse_single;
use bloch::reader::JsonIterator;
use bloch::value::VariantSliceIterator;
use bloch::zc;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn variant_arr() -> TestResult {
    let data = std::fs::read(crate::common::fixture("variant_arr.native"))?;
    let (_, block) = parse_single(&data)?;

    // │  0 │ ['string value',12345,[1,2,3],'{"key":"value"}'] │
    // │  1 │ ['another string',1232,[4,5],'{"array":[6,7]}']  │
    // │  2 │ ['more strings',3333,[],'{"nested":{"a":"1"}}']  │
    // │  3 │ ['test json',44,[8,9],'{"boolean":true}']        │

    let variant_marker = &block.markers[1];
    assert_eq!(block.num_rows, 4, "Expected 4 rows in variant_arr");

    // Every row is [String, Int64, Array(UInt64), JSON]; the JSON is given as its first path.
    let expected: [(&str, i64, &[u64], &str); 4] = [
        ("string value", 12345, &[1, 2, 3], "key"),
        ("another string", 1232, &[4, 5], "array"),
        ("more strings", 3333, &[], "nested.a"),
        ("test json", 44, &[8, 9], "boolean"),
    ];

    for (i, (exp_str, exp_int, exp_arr, exp_json_path)) in expected.into_iter().enumerate() {
        let mut it: VariantSliceIterator = variant_marker.get(i)?.unwrap().try_into()?;

        let s: &str = it.next().unwrap()?.try_into()?;
        assert_eq!(s, exp_str, "Row {i}");

        let n: i64 = it.next().unwrap()?.try_into()?;
        assert_eq!(n, exp_int, "Row {i}");

        let arr: &[zc::U64] = it.next().unwrap()?.try_into()?;
        assert_eq!(
            arr.iter().map(|v| v.get()).collect::<Vec<_>>(),
            exp_arr,
            "Row {i}"
        );

        let mut json: JsonIterator = it.next().unwrap()?.try_into()?;
        let (path, _) = json.next().unwrap()?;
        assert_eq!(path, exp_json_path, "Row {i}");

        assert!(it.next().is_none(), "Row {i}: more than 4 elements");
    }

    Ok(())
}
