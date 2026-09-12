use bloch::parse::block::parse_single;
use bloch::reader::{I64, Value};
use bloch::{Error, FromBlock};
use bloch::{reader::JsonIterator, value::Value as JsonValue};

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    json: Value<'a>,
}

#[test]
fn reads_json_paths() -> Result<(), Box<dyn std::error::Error>> {
    const _SQL: &str = r#"
drop table if exists json_sample;

create table json_sample
(
    id   Int64,
    json JSON
) engine = MergeTree order by tuple();

insert into json_sample (id, json) values
    (0, '{"key": "value"}'),
    (1, '{"array": [1, 2, 3]}'),
    (2, '{"nested": {"a": 1, "b": 2}}'),
    (3, '{"boolean": true}'),
    (4, '{"null_value": null}'),
    (5, '{"date": "2023-01-01"}'),
    (6, '{"datetime": "2023-01-01T12:00:00Z"}');

insert into json_sample (id, json) values
    (7, '{"array": {"haha": true}}');

insert into json_sample (id, json) values
    (8, '{"complex": {"nested": {"array": [1, 2, 3], "value": "test"}}}'),
    (9, '{"empty_object": {}}'),
    (10, '{"empty_array": []}'),
    (11, '{"mixed_types": [1, "string", true, null]}'),
    (12, '{"uuid": "bb679be2-e161-4e5e-b09b-d66f3ed12464"}');

optimize table json_sample;

select * from json_sample order by id format Native;
"#;

    let data = std::fs::read(crate::common::fixture("json.native"))?;
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
            .collect::<bloch::Result<Vec<_>>>()?;
        actual.sort_unstable();
        let mut expected = expected[index].to_vec();
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }
    Ok(())
}

#[test]
fn reads_typed_and_dynamic_json_values() -> Result<(), Box<dyn std::error::Error>> {
    const _SQL: &str = r#"
set enable_json_type = 1;

drop table if exists json_typed_sample;

create table json_typed_sample
(
    id   Int64,
    json JSON(a UInt64, `nested.name` String)
) engine = MergeTree order by tuple();

insert into json_typed_sample (id, json) values
    (0, '{"a": 42, "extra": true, "nested": {"name": "alpha"}}'),
    (1, '{"a": 7, "extra": "text", "nested": {"name": "beta"}}'),
    (2, '{}');

optimize table json_typed_sample;

select * from json_typed_sample order by id format Native;
"#;

    let data = std::fs::read(crate::common::fixture("json_typed.native"))?;
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
                    JsonValue::String(value) => {
                        let value: &str = JsonValue::String(value).try_into()?;
                        value.to_owned()
                    }
                    other => panic!("unexpected JSON value: {other:?}"),
                };
                Ok((path, value))
            })
            .collect::<bloch::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[index]);
    }

    Ok(())
}

#[test]
fn rejects_non_empty_shared_json_data() -> Result<(), Box<dyn std::error::Error>> {
    const _SQL: &str = r#"
set enable_json_type = 1;

drop table if exists json_shared_sample;

create table json_shared_sample
(
    id   UInt64,
    json JSON(max_dynamic_paths=0)
) engine = MergeTree order by tuple();

insert into json_shared_sample (id, json) values
    (1, '{"a": 42, "nested": {"name": "alpha"}}');

optimize table json_shared_sample;

select * from json_shared_sample order by id format Native;
"#;

    let data = std::fs::read(crate::common::fixture("json_shared.native"))?;
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
