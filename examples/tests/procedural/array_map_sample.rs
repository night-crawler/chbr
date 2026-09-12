use std::collections::HashMap;

use bloch::parse::block::parse_single;
use bloch::value::MapSliceIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn array_map_sample() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("array_map_sample.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,"[{'a': 'apple', 'b': 'banana'}, {'c': 'cherry'}]"
    // 1,"[{'d': 'date'}, {'e': 'elderberry', 'f': 'fig'}]"
    // 2,"[{'g': 'grape', 'h': 'honeydew'}]"
    // 3,[{'i': 'kiwi'}]
    // 4,[]
    // 5,"[{'j': 'lemon', 'k': 'mango'}]"

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

    let map_marker = &block.markers[1];
    for (i, expected) in expected.iter().enumerate() {
        let map_slice_iterator: MapSliceIterator<&str, &str> =
            map_marker.get(i)?.unwrap().try_into()?;
        let mut actual = vec![];

        for map in map_slice_iterator.flatten() {
            let map = map.flatten().collect::<HashMap<&str, &str>>();
            actual.push(map);
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
