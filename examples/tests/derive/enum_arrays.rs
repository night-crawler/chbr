use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, Enum8, Enum16, I64};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr_e8: Array<'a, Enum8<'a>>,
    arr_e16: Array<'a, Enum16<'a>>,
}

#[test]
fn reads_enum_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("enums_array_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected8 = [
        vec!["Red", "Green"],
        vec!["Blue", "Red"],
        vec!["Green"],
        vec![],
        vec!["Red", "Blue"],
        vec!["Green", "Red", "Blue"],
    ];
    let expected16 = [
        vec!["Foo"],
        vec!["Bar"],
        vec!["Foo", "Bar"],
        vec!["Foo"],
        vec![],
        vec!["Bar"],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.arr_e8.try_collect_vec()?, expected8[index]);
        assert_eq!(row.arr_e16.try_collect_vec()?, expected16[index]);
    }
    Ok(())
}
