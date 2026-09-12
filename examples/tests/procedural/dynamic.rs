use std::collections::HashMap;

use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use bloch::zc;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
set session_timezone = 'UTC';
set allow_experimental_dynamic_type = 1;

drop table if exists dynamic_sample;

create table dynamic_sample
(
    id  Int64,
    dyn Dynamic
) engine = MergeTree order by tuple();

insert into dynamic_sample (id, dyn) values
    (0, 'string value'),
    (1, 12345),
    (2, [1, 2, 3]),
    (3, {'key': 'value'}),
    (4, toDate('2023-01-01')),
    (5, 0),
    (6, toDateTime('2023-01-01 12:00:00'));

insert into dynamic_sample (id, dyn) values
    (7, toUUID('d60b7c85-0739-4786-a8d9-f1bbc72104df')),
    (8, toFloat64(3.14)),
    (9, toDecimal32(1.23, 2));

optimize table dynamic_sample final;

select * from dynamic_sample order by id format Native;
"#;

#[expect(clippy::approx_constant)]
#[test]
fn dynamic() -> TestResult {
    let data = std::fs::read(crate::common::fixture("dynamic.native"))?;
    let (_, block) = parse_single(&data)?;
    println!("{}", block.num_rows);

    // ┌─id─┬─dyn──────────────────────────────────┐
    // │  0 │ string value                         │
    // │  1 │ 12345                                │
    // │  2 │ [1,2,3]                              │
    // │  3 │ {'key':'value'}                      │
    // │  4 │ 2023-01-01                           │
    // │  5 │ 0                                    │
    // │  6 │ 2023-01-01 12:00:00                  │
    // │  7 │ d60b7c85-0739-4786-a8d9-f1bbc72104df │
    // │  8 │ 3.14                                 │
    // │  9 │ 1.23                                 │
    // └────┴──────────────────────────────────────┘

    let expected = [
        "string value",
        "12345",
        "[1,2,3]",
        "{'key':'value'}",
        "2023-01-01",
        "0",
        "2023-01-01 12:00:00",
        "d60b7c85-0739-4786-a8d9-f1bbc72104df",
        "3.14",
        "1.23",
    ];

    let dynamic_marker = &block.markers[1];

    let row0: &str = dynamic_marker.get(0)?.unwrap().try_into()?;
    assert_eq!(row0, expected[0], "Mismatch at index 0");

    let row1: i64 = dynamic_marker.get(1)?.unwrap().try_into()?;
    assert_eq!(row1, 12345, "Mismatch at index 1");

    let row2: &[zc::I64] = dynamic_marker.get(2)?.unwrap().try_into()?;
    assert_eq!(row2, &[1, 2, 3], "Mismatch at index 2");

    let row3: MapIterator<&str, &str> = dynamic_marker.get(3)?.unwrap().try_into()?;
    let mut map = HashMap::new();
    for (key, value) in row3.flatten() {
        map.insert(key, value);
    }
    assert_eq!(map.get("key"), Some(&"value"), "Mismatch at index 3");

    let row4: chrono::NaiveDate = dynamic_marker.get(4)?.unwrap().try_into()?;
    assert_eq!(
        row4,
        chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        "Mismatch at index 4"
    );

    let row5: i64 = dynamic_marker.get(5)?.unwrap().try_into()?;
    assert_eq!(row5, 0, "Mismatch at index 5");

    let row6: chrono::DateTime<chrono_tz::Tz> = dynamic_marker.get(6)?.unwrap().try_into()?;
    assert_eq!(row6.to_string(), "2023-01-01 12:00:00 UTC");

    let row7: uuid::Uuid = dynamic_marker.get(7)?.unwrap().try_into()?;
    assert_eq!(
        row7,
        uuid::Uuid::parse_str("d60b7c85-0739-4786-a8d9-f1bbc72104df")?,
        "Mismatch at index 7"
    );

    let row8: f64 = dynamic_marker.get(8)?.unwrap().try_into()?;
    assert_eq!(row8, 3.14, "Mismatch at index 8");

    let row9: rust_decimal::Decimal = dynamic_marker.get(9)?.unwrap().try_into()?;
    assert_eq!(
        row9,
        rust_decimal::Decimal::try_from(1.23f32)?,
        "Mismatch at index 9"
    );

    Ok(())
}
