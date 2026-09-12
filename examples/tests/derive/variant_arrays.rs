use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64, Value as ValueReader};
use bloch::{reader::JsonIterator, value::Value};

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

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "variant")]
    values: Array<'a, ValueReader<'a>>,
}

#[test]
fn reads_variant_arrays() -> Result<(), Box<dyn std::error::Error>> {
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
        let values = row.values.collect::<bloch::Result<Vec<_>>>()?;
        assert_eq!(values.len(), 4);
        assert_eq!(
            <&str>::try_from(values[0].clone())?,
            expected_strings[index]
        );
        assert_eq!(i64::try_from(values[1].clone())?, expected_integers[index]);
        let Value::UInt64Slice(array) = &values[2] else {
            panic!("expected UInt64 array at row {index}");
        };
        assert_eq!(
            array.iter().map(|value| value.get()).collect::<Vec<_>>(),
            expected_arrays[index]
        );
        let _: JsonIterator = values[3].clone().try_into()?;
    }
    Ok(())
}
