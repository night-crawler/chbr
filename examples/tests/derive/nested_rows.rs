use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64, Str, U64};

#[derive(FromBlock)]
struct Child<'a> {
    child_id: U64<'a>,
    child_name: Str<'a>,
}
#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    nes: Array<'a, Child<'a>>,
}

#[test]
fn reads_nested_rows() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("simple_nested.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![(1, "Alice"), (2, "Bob")],
        vec![(3, "Charlie"), (4, "Diana")],
        vec![(5, "Eve")],
        vec![],
        vec![(6, "Frank"), (7, "Grace")],
        vec![(8, "Heidi")],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let actual = row
            .nes
            .map(|child| child.map(|child| (child.child_id, child.child_name)))
            .collect::<chbr::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
