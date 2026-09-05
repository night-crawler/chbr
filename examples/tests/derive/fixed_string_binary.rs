use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{FixedBytes, FixedStr, I64};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "fb")]
    value: FixedBytes<'a>,
}

#[derive(FromBlock)]
struct TextRow<'a> {
    #[col(name = "fb")]
    #[allow(dead_code)]
    value: FixedStr<'a>,
}

#[test]
fn reads_fixed_string_binary_payloads() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("fixed_string_binary.native"))?;
    let (_, block) = parse_single(&data)?;
    // Every record is exactly FixedString(4) wide: trailing zeros are payload.
    let expected: [&[u8]; 4] = [
        b"\x01\x00\x00\x00",
        b"\x00\x00\x00\x00",
        b"\xde\xad\xbe\xef",
        b"ab\x00\x00",
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.value, expected[index]);
        assert_eq!(row.value.len(), 4);
    }
    // The `&str` reader refuses the column: row 2 is not UTF-8.
    assert!(matches!(
        TextRow::rows(&block),
        Err(chbr::Error::Utf8Decode(_, _))
    ));
    Ok(())
}
