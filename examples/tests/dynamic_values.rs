mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{I64, Value as ValueReader};
use chbr::value::{MapIterator, Value};
use std::collections::HashMap;

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "dyn")]
    value: ValueReader<'a>,
}

#[test]
#[expect(clippy::approx_constant)]
fn reads_dynamic_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("dynamic.native"))?;
    let (_, block) = parse_single(&data)?;
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        match (index, row.value) {
            (0, Value::String(value)) => assert_eq!(value, "string value"),
            (1, Value::Int64(value)) => assert_eq!(value, 12345),
            (2, Value::Int64Slice(values)) => assert_eq!(
                values.iter().map(|value| value.get()).collect::<Vec<_>>(),
                [1, 2, 3]
            ),
            (3, value) => {
                let map: MapIterator<&str, &str> = value.try_into()?;
                assert_eq!(
                    map.collect::<chbr::Result<HashMap<_, _>>>()?.get("key"),
                    Some(&"value")
                );
            }
            (4, value) => assert_eq!(
                chrono::NaiveDate::try_from(value)?.to_string(),
                "2023-01-01"
            ),
            (5, Value::Int64(value)) => assert_eq!(value, 0),
            (6, value) => assert_eq!(
                chrono::DateTime::<chrono_tz::Tz>::try_from(value)?.to_string(),
                "2023-01-01 12:00:00 UTC"
            ),
            (7, value) => assert_eq!(
                uuid::Uuid::try_from(value)?.to_string(),
                "d60b7c85-0739-4786-a8d9-f1bbc72104df"
            ),
            (8, Value::Float64(value)) => assert_eq!(value, 3.14),
            (9, value) => assert_eq!(
                rust_decimal::Decimal::try_from(value)?,
                rust_decimal::Decimal::try_from(1.23f32)?
            ),
            _ => panic!("unexpected dynamic value at row {index}"),
        }
    }
    Ok(())
}
