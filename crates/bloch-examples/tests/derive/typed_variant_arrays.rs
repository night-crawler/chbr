use bloch::parse::block::parse_single;
use bloch::reader::{Array, ArrayIter, I64, JsonValue, U64, Variant};
use bloch::{FromBlock, FromVariant};

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

// Variant alternatives follow ClickHouse's canonical type order:
// Array(UInt64), JSON, String, UInt64.
#[derive(FromVariant)]
enum VariantValue<'a> {
    Array(ArrayIter<'a, U64<'a>>),
    Json(JsonValue<'a>),
    String(&'a str),
    Integer(u64),
}

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "variant")]
    values: Array<'a, Variant<'a, VariantValue<'a>>>,
}

#[test]
fn reads_variant_arrays_into_typed_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("variant_arr.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected_strings = [
        "string value",
        "another string",
        "more strings",
        "test json",
    ];
    let expected_integers = [12345, 1232, 3333, 44];
    let expected_arrays: &[&[u64]] = &[&[1, 2, 3], &[4, 5], &[], &[8, 9]];

    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);

        let mut values = row.values;
        let Some(VariantValue::String(string)) = values.next().transpose()? else {
            panic!("expected String at row {index}");
        };
        assert_eq!(string, expected_strings[index]);

        let Some(VariantValue::Integer(integer)) = values.next().transpose()? else {
            panic!("expected UInt64 at row {index}");
        };
        assert_eq!(integer, expected_integers[index]);

        let Some(VariantValue::Array(array)) = values.next().transpose()? else {
            panic!("expected Array(UInt64) at row {index}");
        };
        assert_eq!(array.try_collect_vec()?, expected_arrays[index]);

        let Some(VariantValue::Json(json)) = values.next().transpose()? else {
            panic!("expected JSON at row {index}");
        };
        let _ = json.paths();
        assert!(values.next().is_none());
    }

    Ok(())
}
