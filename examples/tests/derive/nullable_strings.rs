use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, Nullable, Str};

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    nstr: Nullable<'a, Str<'a>>,
}

#[test]
fn reads_nullable_strings() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("nullable_string.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        Some("hello"),
        None,
        Some("world"),
        Some("clickhouse"),
        None,
        Some("test"),
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.nstr, expected[index]);
    }
    Ok(())
}
