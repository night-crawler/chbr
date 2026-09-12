use std::collections::HashMap;

use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn map_in_map() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("map_in_map.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,"{'a': {'x': 'apple', 'y': 'banana'}, 'b': {'z': 'cherry'}}"
    // 1,{'c': {'d': 'date'}}
    // 2,"{'e': {'g': 'elderberry'}, 'f': {'h': 'fig', 'i': 'grape'}}"
    // 3,{'j': {'k': 'kiwi'}}
    // 4,{}
    // 5,"{'l': {'n': 'lemon'}, 'm': {'o': 'mango', 'p': 'nectarine'}}"

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

    let map_marker = &block.markers[1];

    for (i, expected) in expected.iter().enumerate() {
        let map_value = map_marker.get(i)?.unwrap();
        let map_iter: MapIterator<&str, MapIterator<&str, &str>> = map_value.try_into()?;

        let mut actual = HashMap::new();

        for (map_key, map_value) in map_iter.flatten() {
            let inner_map = map_value.flatten().collect::<HashMap<&str, &str>>();
            actual.insert(map_key, inner_map);
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
