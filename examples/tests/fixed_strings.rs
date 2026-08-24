mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{FixedStr, I64};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "fs")]
    value: FixedStr<'a>,
}

#[test]
fn reads_fixed_strings() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("fixed_string_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        "fixed string 1",
        "fixed string 2",
        "fixed string 3",
        "fixed string 4",
        "fixed string 5 q",
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.value, expected[index]);
    }
    Ok(())
}
