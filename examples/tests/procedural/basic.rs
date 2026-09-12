use bloch::{BlocksIterator, parse::block::parse_many, value::Value};

const _SQL: &str = r#"
drop table if exists example;

create table example
(
    id      UInt32,
    tags    Array(String),
    attrs   Map(String, String),
    payload Variant(Array(Int64), Int64, String)
) engine = MergeTree order by tuple();

insert into example (id, tags, attrs, payload) values
    (1, ['fast', 'cpu'], mapFromArrays(['region', 'host'], ['eu', 'a1']), 'hello'),
    (2, [], mapFromArrays(['region'], ['us']), 42::Int64),
    (3, ['gpu'], map(), [1, 2, 3]::Array(Int64)),
    (4, ['idle'], mapFromArrays(['region'], ['ap']), NULL);

select * from example order by id format Native;
"#;

#[test]
fn basic() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("example.native"))?;

    let mut blocks = parse_many(&data)?;
    let rows = BlocksIterator::new_ordered(&mut blocks, &["id", "tags", "attrs", "payload"])?;
    let mut actual = Vec::new();

    for row in rows {
        let [id, tags, attrs, payload] = row.cols() else {
            return Err("unexpected column count".into());
        };
        let i = row.row_index();

        let id = id.get_u32(i)?.expect("valid row index");
        let tags: &[&bloch::BStr] = tags.get(i)?.expect("valid row index").try_into()?;
        let tags = tags
            .iter()
            .map(|value| std::str::from_utf8(value))
            .collect::<Result<Vec<_>, _>>()?;

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
            Value::Empty => "null".to_owned(),
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
            r#"id=4 tags=["idle"] attrs=[("region", "ap")] payload=null"#,
        ]
    );

    Ok(())
}
