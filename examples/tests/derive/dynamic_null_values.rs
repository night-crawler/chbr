use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64, Value as ValueReader};
use chbr::value::Value;

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "dyn")]
    value: ValueReader<'a>,
    arr: Array<'a, ValueReader<'a>>,
}

fn render(value: Value<'_>) -> String {
    match value {
        Value::Empty => "null".to_owned(),
        Value::Int64(value) => value.to_string(),
        Value::String(value) => value.to_string(),
        other => panic!("unexpected value {other:?}"),
    }
}

/// `Value` is the only reader for Dynamic; NULL rows must come through as `Value::Empty`
/// instead of failing the whole row.
#[test]
fn reads_null_dynamic_rows() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("dynamic_null.native"))?;
    let (_, block) = parse_single(&data)?;
    let mut rows = Vec::new();
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let elements = row
            .arr
            .map(|value| value.map(render))
            .collect::<chbr::Result<Vec<_>>>()?;
        rows.push(format!("{} {}", render(row.value), elements.join(",")));
    }
    assert_eq!(rows, ["42 1,null,a", "null ", "x null", "null null,null"]);
    Ok(())
}
