use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{I64, Str, Tuple};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    tup: Tuple<(I64<'a>, Str<'a>)>,
}

#[test]
fn reads_tuple_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("tuple.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        (1, "a"),
        (3, "ab"),
        (7, "ac"),
        (9, "ad"),
        (11, "ae"),
        (2, "af"),
        (3, "ag"),
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.tup, expected[index]);
    }
    Ok(())
}
