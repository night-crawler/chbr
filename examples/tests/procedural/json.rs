use bloch::parse::block::parse_single;
use bloch::reader::JsonIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

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

#[test]
fn json() -> TestResult {
    let data = std::fs::read(crate::common::fixture("json.native"))?;
    let (_, block) = parse_single(&data)?;

    //    ┌─id─┬─json──────────────────────────────────────────────────────────┐
    //  1. │  0 │ {"key":"value"}                                               │
    //  2. │  1 │ {"array":["1","2","3"]}                                       │
    //  3. │  2 │ {"nested":{"a":"1","b":"2"}}                                  │
    //  4. │  3 │ {"boolean":true}                                              │
    //  5. │  4 │ {}                                                            │
    //  6. │  5 │ {"date":"2023-01-01"}                                         │
    //  7. │  6 │ {"datetime":"2023-01-01T12:00:00Z"}                           │
    //  8. │  7 │ {"array":{"haha":true}}                                       │
    //  9. │  8 │ {"complex":{"nested":{"array":["1","2","3"],"value":"test"}}} │
    // 10. │  9 │ {}                                                            │
    // 11. │ 10 │ {"empty_array":[]}                                            │
    // 12. │ 11 │ {"mixed_types":["1","string","true",null]}                    │
    // 13. │ 12 │ {"uuid":"bb679be2-e161-4e5e-b09b-d66f3ed12464"}               │
    //     └────┴───────────────────────────────────────────────────────────────┘

    let expected_paths = &[
        ["key"].as_slice(),
        ["array"].as_slice(),
        ["nested.a", "nested.b"].as_slice(),
        ["boolean"].as_slice(),
        [].as_slice(),
        ["date"].as_slice(),
        ["datetime"].as_slice(),
        ["array.haha"].as_slice(),
        ["complex.nested.array", "complex.nested.value"].as_slice(),
        [].as_slice(),
        ["empty_array"].as_slice(),
        ["mixed_types"].as_slice(),
        ["uuid"].as_slice(),
    ];

    let json_marker = &block.markers[1];
    for (i, expected_paths) in expected_paths.iter().copied().enumerate() {
        let mut expected_paths = Vec::from(expected_paths);
        expected_paths.sort_unstable();

        let it: JsonIterator = json_marker.get(i)?.unwrap().try_into()?;
        let mut actual_paths: Vec<&str> = Vec::new();
        for item in it {
            let (path, _value) = item?;
            actual_paths.push(path);
        }
        actual_paths.sort_unstable();
        assert_eq!(
            actual_paths, expected_paths,
            "Mismatch at index {i}: expected {:?}, got {:?}",
            expected_paths, actual_paths
        );
    }

    Ok(())
}
