mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Enum8, Enum16, I64};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    e8: Enum8<'a>,
    e16: Enum16<'a>,
}

#[test]
fn reads_enum_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("enums_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected8 = ["Red", "Green", "Blue", "Red", "Green", "Blue"];
    let expected16 = ["Foo", "Bar", "Foo", "Bar", "Foo", "Bar"];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!((row.e8, row.e16), (expected8[index], expected16[index]));
    }
    Ok(())
}
