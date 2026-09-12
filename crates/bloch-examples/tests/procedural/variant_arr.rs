use bloch::parse::block::parse_single;
use bloch::reader::JsonIterator;
use bloch::value::VariantSliceIterator;
use bloch::zc;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
set enable_json_type = 1;
set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;

drop table if exists variant_arr;

create table variant_arr
(
    id      Int64,
    variant Array(Variant(String, UInt64, Array(UInt64), JSON))
) engine = MergeTree order by tuple();

insert into variant_arr (id, variant) values
    (0, array(
        CAST('string value', 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST(toUInt64(12345), 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST(array(toUInt64(1), toUInt64(2), toUInt64(3)), 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST('{"key":"value"}'::JSON, 'Variant(String, UInt64, Array(UInt64), JSON)'))),
    (1, array(
        CAST('another string', 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST(toUInt64(1232), 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST(array(toUInt64(4), toUInt64(5)), 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST('{"array":[6,7]}'::JSON, 'Variant(String, UInt64, Array(UInt64), JSON)'))),
    (2, array(
        CAST('more strings', 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST(toUInt64(3333), 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST(CAST(array(), 'Array(UInt64)'), 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST('{"nested":{"a":1}}'::JSON, 'Variant(String, UInt64, Array(UInt64), JSON)'))),
    (3, array(
        CAST('test json', 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST(toUInt64(44), 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST(array(toUInt64(8), toUInt64(9)), 'Variant(String, UInt64, Array(UInt64), JSON)'),
        CAST('{"boolean":true}'::JSON, 'Variant(String, UInt64, Array(UInt64), JSON)')));

select * from variant_arr order by id format Native;
"#;

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
