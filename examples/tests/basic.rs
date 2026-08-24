use chbr::{BlocksIterator, parse::block::parse_many, value::Value};
use std::path::Path;

#[test]
fn basic() -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../testdata/example.native");
    let data = std::fs::read(path)?;

    let mut blocks = parse_many(&data)?;
    let rows = BlocksIterator::new_ordered(&mut blocks, &["id", "tags", "attrs", "payload"])?;
    let mut actual = Vec::new();

    for row in rows {
        let [id, tags, attrs, payload] = row.cols() else {
            return Err("unexpected column count".into());
        };
        let i = row.row_index();

        let id = id.get_u32(i)?.expect("valid row index");
        let tags: &[&str] = tags.get(i)?.expect("valid row index").try_into()?;

        let mut attrs_vec = Vec::new();
        if let Some(map) = attrs.get_map::<&str, &str>(i)? {
            for kv in map {
                attrs_vec.push(kv?);
            }
        }

        let payload = match payload.get(i)?.expect("valid row index") {
            Value::String(value) => format!("string: {value}"),
            Value::Int64(value) => format!("int: {value}"),
            Value::Int64Slice(values) => {
                let values = values.iter().map(|value| value.get()).collect::<Vec<_>>();
                format!("array: {values:?}")
            }
            other => format!("unexpected: {other:?}"),
        };

        actual.push(format!(
            "id={id} tags={tags:?} attrs={attrs_vec:?} payload={payload}"
        ));
    }

    assert_eq!(
        actual,
        [
            r#"id=1 tags=["fast", "cpu"] attrs=[("region", "eu"), ("host", "a1")] payload=string: hello"#,
            r#"id=2 tags=[] attrs=[("region", "us")] payload=int: 42"#,
            r#"id=3 tags=["gpu"] attrs=[] payload=array: [1, 2, 3]"#,
        ]
    );

    Ok(())
}
