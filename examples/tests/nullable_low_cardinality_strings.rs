mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{I64, LcNullableStr};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    nlc_str: LcNullableStr<'a>,
}

#[test]
fn reads_nullable_low_cardinality_strings() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("nullable_lc_str.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        Some("apple"),
        None,
        Some("banana"),
        Some("cherry"),
        None,
        Some("date"),
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.nlc_str, expected[index]);
    }
    Ok(())
}
