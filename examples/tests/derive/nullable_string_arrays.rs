use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64, Nullable, Str};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Nullable<'a, Str<'a>>>,
}

#[test]
fn reads_nullable_string_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("nullable_string_array.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![Some("apple"), Some("banana"), None],
        vec![None, Some("date"), Some("elderberry")],
        vec![Some("fig"), None, Some("honeydew")],
        vec![None],
        vec![],
        vec![Some("lemon"), None, Some("mango")],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.arr.try_collect_vec()?, expected[index]);
    }
    Ok(())
}
