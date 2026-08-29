mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64, Str, U64};

#[derive(FromBlock)]
struct Child<'a> {
    child_id: U64<'a>,
    child_name: Str<'a>,
}
#[derive(FromBlock)]
struct Row<'a, C>
where
    C: chbr::reader::TryRead<'a> + 'a,
{
    id: I64<'a>,
    arr: Array<'a, Array<'a, C>>,
}

#[test]
fn reads_arrays_of_nested_rows() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("array_of_nested.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![vec![(1, "Alice"), (2, "Bob")]],
        vec![vec![(3, "Charlie"), (4, "Diana")]],
        vec![vec![(5, "Eve")]],
        vec![vec![]],
        vec![vec![(6, "Frank"), (7, "Grace")]],
        vec![vec![(8, "Heidi")]],
    ];
    for (index, row) in Row::<Child>::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let actual = row
            .arr
            .map(|children| {
                children?
                    .map(|child| child.map(|child| (child.child_id, child.child_name)))
                    .collect::<chbr::Result<Vec<_>>>()
            })
            .collect::<chbr::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
