mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{I64, Map, Str};
use std::collections::HashMap;

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "m")]
    values: Map<'a, Str<'a>, Map<'a, Str<'a>, Str<'a>>>,
}

#[test]
fn reads_nested_maps() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("map_in_map.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        HashMap::from([
            ("a", HashMap::from([("x", "apple"), ("y", "banana")])),
            ("b", HashMap::from([("z", "cherry")])),
        ]),
        HashMap::from([("c", HashMap::from([("d", "date")]))]),
        HashMap::from([
            ("e", HashMap::from([("g", "elderberry")])),
            ("f", HashMap::from([("h", "fig"), ("i", "grape")])),
        ]),
        HashMap::from([("j", HashMap::from([("k", "kiwi")]))]),
        HashMap::new(),
        HashMap::from([
            ("l", HashMap::from([("n", "lemon")])),
            ("m", HashMap::from([("o", "mango"), ("p", "nectarine")])),
        ]),
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let actual = row
            .values
            .map(|entry| {
                let (key, values) = entry?;
                Ok((key, values.collect::<chbr::Result<HashMap<_, _>>>()?))
            })
            .collect::<chbr::Result<HashMap<_, _>>>()?;
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
