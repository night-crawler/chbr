use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, Str};

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "str")]
    value: Str<'a>,
}

#[test]
fn reads_string_column() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("plain_strings.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = ["hello", "world", "clickhouse", "test", "example", "data"];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.value, expected[index]);
    }
    Ok(())
}
