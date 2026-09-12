use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::JsonIterator;
use bloch::reader::{Array, I64, Value};

const _SQL: &str = r#"
set enable_json_type = 1;

drop table if exists json_arr_sample;

create table json_arr_sample
(
    id       Int64,
    json_arr Array(JSON)
) engine = MergeTree order by tuple();

insert into json_arr_sample (id, json_arr) values
    (0, ['{"key": "value"}', '{"array": [1, 2, 3]}']),
    (1, ['{"nested": {"a": 1, "b": 2}}', '{"boolean": true}']),
    (2, ['{}', '{"date": "2023-01-01"}']),
    (3, ['{"datetime": "2023-01-01T12:00:00Z"}', '{"uuid": "c995b14b-ff14-4f4f-8d25-8eb934785e90"}']);

optimize table json_arr_sample;

select * from json_arr_sample order by id format Native;
"#;

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
