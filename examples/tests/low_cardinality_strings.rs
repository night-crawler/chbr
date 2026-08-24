mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{I64, LcStr};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    lc_str: LcStr<'a>,
}

#[test]
fn reads_low_cardinality_strings() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("plain_lc_string.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = ["apple", "banana", "cherry", "date", "elderberry", "fig"];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.lc_str, expected[index]);
    }
    Ok(())
}
