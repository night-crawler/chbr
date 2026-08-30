use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64, Value as ValueReader};
use chbr::{reader::JsonIterator, value::Value};

#[derive(FromBlock)]
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
        let values = row.values.collect::<chbr::Result<Vec<_>>>()?;
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
