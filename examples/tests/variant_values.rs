mod common;

use chbr::parse::block::parse_single;
use chbr::reader::{ArrayIter, I64, Variant};
use chbr::{FromBlock, FromVariant};

#[derive(FromVariant)]
enum Value<'a> {
    Array(ArrayIter<'a, I64<'a>>),
    Integer(i64),
    String(&'a str),
}
#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "var")]
    value: Variant<'a, Value<'a>>,
}

#[test]
fn reads_variant_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("variant.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = ["1", "a", "[1, 2, 3]", "2", "b", "[4, 5, 6]", "3"];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let actual = match row.value {
            Value::Array(values) => format!("{:?}", values.try_collect_vec()?),
            Value::Integer(value) => value.to_string(),
            Value::String(value) => value.to_owned(),
        };
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
