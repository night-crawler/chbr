use std::collections::HashMap;

use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists map_sample;

create table map_sample
(
    id Int64,
    m  Map(String, String)
) engine = MergeTree order by tuple();

insert into map_sample (id, m) values
    (0, mapFromArrays(['a', 'b', 'c'], ['apple', 'banana', 'cherry'])),
    (1, mapFromArrays(['d', 'e'], ['date', 'elderberry'])),
    (2, mapFromArrays(['f', 'g', 'h'], ['fig', 'grape', 'honeydew'])),
    (3, mapFromArrays(['i'], ['kiwi'])),
    (4, map()),
    (5, mapFromArrays(['j', 'k'], ['lemon', 'mango']));

select * from map_sample order by id format Native;
"#;

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
