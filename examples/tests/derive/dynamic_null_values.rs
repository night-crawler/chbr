use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64, Value as ValueReader};
use bloch::value::Value;

const _SQL: &str = r#"
set allow_experimental_dynamic_type = 1;

drop table if exists dynamic_null_sample;

create table dynamic_null_sample
(
    id  Int64,
    dyn Dynamic,
    arr Array(Dynamic)
) engine = MergeTree order by tuple();

insert into dynamic_null_sample (id, dyn, arr) values
    (0, 42::Int64, [CAST(1::Int64, 'Dynamic'), CAST(NULL, 'Dynamic'), CAST('a', 'Dynamic')]),
    (1, CAST(NULL, 'Dynamic'), []),
    (2, 'x', [CAST(NULL, 'Dynamic')]),
    (3, CAST(NULL, 'Dynamic'), [CAST(NULL, 'Dynamic'), CAST(NULL, 'Dynamic')]);

optimize table dynamic_null_sample final;

select id, dyn, arr from dynamic_null_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
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
            .collect::<bloch::Result<Vec<_>>>()?;
        rows.push(format!("{} {}", render(row.value), elements.join(",")));
    }
    assert_eq!(rows, ["42 1,null,a", "null ", "x null", "null null,null"]);
    Ok(())
}
