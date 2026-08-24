mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{I64, Str};

// Field order deliberately differs from the ClickHouse tuple definition.
// Named tuple fields are resolved through `#[col(name = ...)]`.
#[derive(FromBlock)]
struct Fruit<'a> {
    rank: I64<'a>,
    #[col(name = "name")]
    title: Str<'a>,
}

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    tup: Fruit<'a>,
}

#[test]
fn reads_named_tuple_fields_by_name() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("named_tuple.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected_titles = ["apple", "banana", "cherry", "date", "elderberry", "fig"];

    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.tup.title, expected_titles[index]);
        assert_eq!(row.tup.rank, row.id * 10);
    }

    Ok(())
}
