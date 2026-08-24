mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{I64, Map, Str};
use std::collections::HashMap;

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "m")]
    values: Map<'a, Str<'a>, Str<'a>>,
}

#[test]
fn reads_string_maps() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("map_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        HashMap::from([("a", "apple"), ("b", "banana"), ("c", "cherry")]),
        HashMap::from([("d", "date"), ("e", "elderberry")]),
        HashMap::from([("f", "fig"), ("g", "grape"), ("h", "honeydew")]),
        HashMap::from([("i", "kiwi")]),
        HashMap::new(),
        HashMap::from([("j", "lemon"), ("k", "mango")]),
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(
            row.values.collect::<chbr::Result<HashMap<_, _>>>()?,
            expected[index]
        );
    }
    Ok(())
}
