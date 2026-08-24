mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64, Map, Str};
use std::collections::HashMap;

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr_map: Array<'a, Map<'a, Str<'a>, Str<'a>>>,
}

#[test]
fn reads_arrays_of_maps() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("array_map_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        vec![
            HashMap::from([("a", "apple"), ("b", "banana")]),
            HashMap::from([("c", "cherry")]),
        ],
        vec![
            HashMap::from([("d", "date")]),
            HashMap::from([("e", "elderberry"), ("f", "fig")]),
        ],
        vec![HashMap::from([("g", "grape"), ("h", "honeydew")])],
        vec![HashMap::from([("i", "kiwi")])],
        vec![],
        vec![HashMap::from([("j", "lemon"), ("k", "mango")])],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let actual = row
            .arr_map
            .map(|map| map?.collect::<chbr::Result<HashMap<_, _>>>())
            .collect::<chbr::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
