mod common;

use chbr::parse::block::parse_single;
use chbr::reader::{I64, Value};
use chbr::{Error, FromBlock};
use chbr::{reader::JsonIterator, value::Value as JsonValue};

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

#[test]
fn reads_typed_and_dynamic_json_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("json_typed.native"))?;
    let (remainder, block) = parse_single(&data)?;
    assert!(remainder.is_empty());

    let expected = [
        vec![
            ("a", "42".to_owned()),
            ("nested.name", "alpha".to_owned()),
            ("extra", "true".to_owned()),
        ],
        vec![
            ("a", "7".to_owned()),
            ("nested.name", "beta".to_owned()),
            ("extra", "text".to_owned()),
        ],
        vec![("a", "0".to_owned()), ("nested.name", String::new())],
    ];

    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        let json: JsonIterator = row.json.try_into()?;
        let actual = json
            .map(|item| {
                let (path, value) = item?;
                let value = match value {
                    JsonValue::Bool(value) => value.to_string(),
                    JsonValue::UInt64(value) => value.to_string(),
                    JsonValue::String(value) => value.to_owned(),
                    other => panic!("unexpected JSON value: {other:?}"),
                };
                Ok((path, value))
            })
            .collect::<chbr::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[index]);
    }

    Ok(())
}

#[test]
fn rejects_non_empty_shared_json_data() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("json_shared.native"))?;
    let error = match parse_single(&data) {
        Ok(_) => panic!("non-empty shared JSON data must not be silently skipped"),
        Err(error) => error,
    };
    assert!(matches!(
        &error,
        Error::NotImplemented(message) if message == "non-empty JSON shared data"
    ));
    Ok(())
}
