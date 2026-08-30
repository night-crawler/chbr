use chbr::parse::block::parse_single;
use chbr::reader::{Array, ArrayIter, I64, JsonValue, U64, Variant};
use chbr::{FromBlock, FromVariant};

// Variant alternatives follow ClickHouse's canonical type order:
// Array(UInt64), JSON, String, UInt64.
#[derive(FromVariant)]
enum VariantValue<'a> {
    Array(ArrayIter<'a, U64<'a>>),
    Json(JsonValue<'a>),
    String(&'a str),
    Integer(u64),
}

#[derive(FromBlock)]
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
