use bloch::parse::block::parse_single;
use bloch::reader::JsonSliceIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

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

#[test]
fn json_arr() -> TestResult {
    let data = std::fs::read(crate::common::fixture("json_arr.native"))?;
    let (_, block) = parse_single(&data)?;

    // ┌─id─┬─json_arr──────────────────────────────────────────────────────────────────────────────────┐
    // │  0 │ ['{"key":"value"}','{"array":["1","2","3"]}']                                             │
    // │  1 │ ['{"nested":{"a":"1","b":"2"}}','{"boolean":true}']                                       │
    // │  2 │ ['{}','{"date":"2023-01-01"}']                                                            │
    // │  3 │ ['{"datetime":"2023-01-01T12:00:00Z"}','{"uuid":"c995b14b-ff14-4f4f-8d25-8eb934785e90"}'] │
    // └────┴───────────────────────────────────────────────────────────────────────────────────────────┘

    let json_arr_marker = &block.markers[1];
    assert_eq!(block.num_rows, 4, "Expected 4 rows in json_arr");

    let expected: [&[_]; 4] = [
        &["key", "array"],
        &["nested.a", "nested.b", "boolean"],
        &["date"],
        &["datetime", "uuid"],
    ];

    for (row_idx, exp_paths) in expected.iter().enumerate() {
        let slice: JsonSliceIterator = json_arr_marker.get(row_idx)?.unwrap().try_into()?;

        let mut actual_paths: Vec<&str> = Vec::new();
        for mut json_it in slice {
            for item in &mut json_it {
                let (path, _value) = item?;
                actual_paths.push(path);
            }
        }

        actual_paths.sort_unstable();
        let mut expected_paths = exp_paths.to_vec();
        expected_paths.sort_unstable();

        assert_eq!(
            actual_paths, expected_paths,
            "Mismatch in row {row_idx}: expected {:?}, got {:?}",
            expected_paths, actual_paths
        );
    }

    Ok(())
}
