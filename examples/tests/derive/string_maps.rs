use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, Map, Str};
use std::collections::HashMap;

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

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "m")]
    values: Map<'a, Str<'a>, Str<'a>>,
}

#[test]
fn reads_string_maps() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("map_sample.native"))?;
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
            row.values.collect::<bloch::Result<HashMap<_, _>>>()?,
            expected[index]
        );
    }
    Ok(())
}
