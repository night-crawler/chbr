use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, FixedStr, I64};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, FixedStr<'a>>,
}

#[test]
fn reads_fixed_string_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("fixed_string_array.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec!["fixed string 1", "fixed string 2"],
        vec!["fixed string 3", "fixed string 4"],
        vec!["fixed string 5", "fixed string 6"],
        vec!["fixed string 7"],
        vec![],
        vec!["fixed string 8", "fixed string 9"],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.arr.try_collect_vec()?, expected[index]);
    }
    Ok(())
}
