use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::JsonIterator;
use bloch::reader::{Array, I64, Value};

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    json_arr: Array<'a, Value<'a>>,
}

#[test]
fn reads_json_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("json_arr.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected: &[&[&str]] = &[
        &["key", "array"],
        &["nested.a", "nested.b", "boolean"],
        &["date"],
        &["datetime", "uuid"],
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let mut actual = Vec::new();
        for value in row.json_arr {
            let json: JsonIterator = value?.try_into()?;
            actual.extend(
                json.map(|item| item.map(|(path, _)| path))
                    .collect::<bloch::Result<Vec<_>>>()?,
            );
        }
        actual.sort_unstable();
        let mut expected = expected[index].to_vec();
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }
    Ok(())
}
