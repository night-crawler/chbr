use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, Map, Str};
use std::collections::HashMap;

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

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "m")]
    values: Map<'a, Str<'a>, Map<'a, Str<'a>, Str<'a>>>,
}

#[test]
fn reads_nested_maps() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("map_in_map.native"))?;
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
                Ok((key, values.collect::<bloch::Result<HashMap<_, _>>>()?))
            })
            .collect::<bloch::Result<HashMap<_, _>>>()?;
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
