use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, Nothing, Nullable, U64};

/// `Nothing` is the element type of `[]` and the inner type of `NULL`.
#[derive(FromBlock)]
struct Row<'a> {
    id: U64<'a>,
    arr: Array<'a, Nothing>,
    n: Nullable<'a, Nothing>,
    arr_n: Array<'a, Nullable<'a, Nothing>>,
    arr_arr: Array<'a, Array<'a, Nothing>>,
}

#[test]
fn reads_empty_arrays_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("nothing.native"))?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.num_rows, 3);

    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, u64::try_from(index)?);
        assert!(row.arr.try_collect_vec()?.is_empty());
        assert!(row.n.is_none());
        let arr_n = row.arr_n.try_collect_vec()?;
        assert_eq!(arr_n.len(), 1);
        assert!(arr_n.iter().all(Option::is_none));
        let mut arr_arr = row.arr_arr;
        assert_eq!(arr_arr.len(), 1);
        assert!(arr_arr.next().unwrap()?.try_collect_vec()?.is_empty());
    }
    Ok(())
}
