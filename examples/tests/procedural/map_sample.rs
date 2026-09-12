use std::collections::HashMap;

use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn map_sample() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("map_sample.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,"{'a': 'apple', 'b': 'banana', 'c': 'cherry'}"
    // 1,"{'d': 'date', 'e': 'elderberry'}"
    // 2,"{'f': 'fig', 'g': 'grape', 'h': 'honeydew'}"
    // 3,{'i': 'kiwi'}
    // 4,{}
    // 5,"{'j': 'lemon', 'k': 'mango'}"

    let expected = [
        HashMap::from([("a", "apple"), ("b", "banana"), ("c", "cherry")]),
        HashMap::from([("d", "date"), ("e", "elderberry")]),
        HashMap::from([("f", "fig"), ("g", "grape"), ("h", "honeydew")]),
        HashMap::from([("i", "kiwi")]),
        HashMap::new(),
        HashMap::from([("j", "lemon"), ("k", "mango")]),
    ];

    let map_marker = &block.markers[1];
    for (i, expected) in expected.iter().enumerate() {
        let map_value = map_marker.get(i)?.unwrap();
        let map_iter: MapIterator<&str, &str> = map_value.try_into()?;
        let map = map_iter.flatten().collect::<HashMap<&str, &str>>();
        assert_eq!(map, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
