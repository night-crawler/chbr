use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64, Str};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Str<'a>>,
}

#[test]
fn reads_string_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("plain_strings_array.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec!["apple", "banana", "cherry"],
        vec!["date", "elderberry"],
        vec!["fig", "grape", "honeydew"],
        vec!["kiwi"],
        vec![],
        vec!["lemon", "mango"],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.arr.try_collect_vec()?, expected[index]);
    }
    Ok(())
}
