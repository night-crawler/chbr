use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, LcNullableStr};

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    nlc_str: LcNullableStr<'a>,
}

#[test]
fn reads_nullable_low_cardinality_strings() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("nullable_lc_str.native"))?;
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
