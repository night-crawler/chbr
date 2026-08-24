mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{I64, Value};
use chbr::value::JsonIterator;

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    json: Value<'a>,
}

#[test]
fn reads_json_paths() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("json.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected: &[&[&str]] = &[
        &["key"],
        &["array"],
        &["nested.a", "nested.b"],
        &["boolean"],
        &[],
        &["date"],
        &["datetime"],
        &["array.haha"],
        &["complex.nested.array", "complex.nested.value"],
        &[],
        &["empty_array"],
        &["mixed_types"],
        &["uuid"],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let json: JsonIterator = row.json.try_into()?;
        let mut actual = json
            .map(|item| item.map(|(path, _)| path))
            .collect::<chbr::Result<Vec<_>>>()?;
        actual.sort_unstable();
        let mut expected = expected[index].to_vec();
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }
    Ok(())
}
