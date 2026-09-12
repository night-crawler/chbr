use std::collections::HashMap;

use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists map_in_map;

create table map_in_map
(
    id Int64,
    m  Map(String, Map(String, String))
) engine = MergeTree order by tuple();

insert into map_in_map (id, m) values
    (0, mapFromArrays(['a', 'b'], [mapFromArrays(['x', 'y'], ['apple', 'banana']), mapFromArrays(['z'], ['cherry'])])),
    (1, mapFromArrays(['c'], [mapFromArrays(['d'], ['date'])])),
    (2, mapFromArrays(['e', 'f'], [mapFromArrays(['g'], ['elderberry']), mapFromArrays(['h', 'i'], ['fig', 'grape'])])),
    (3, mapFromArrays(['j'], [mapFromArrays(['k'], ['kiwi'])])),
    (4, map()),
    (5, mapFromArrays(['l', 'm'], [mapFromArrays(['n'], ['lemon']), mapFromArrays(['o', 'p'], ['mango', 'nectarine'])]));

select * from map_in_map order by id format Native;
"#;

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
