use chbr::{BlocksIterator, parse::block::parse_many, value::Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "testdata/example.native".to_owned());
    let data = std::fs::read(path)?;

    let mut blocks = parse_many(&data)?;

    let it = BlocksIterator::new_ordered(&mut blocks, &["id", "tags", "attrs", "payload"])?;

    for row in it {
        let [id, tags, attrs, payload] = row.cols() else {
            return Err("unexpected column count".into());
        };
        let i = row.row_index();

        let id = id.get_u32(i)?.expect("valid row index");

        let tags: &[&str] = tags.get(i)?.expect("valid row index").try_into()?;

        let mut attrs_vec = vec![];
        if let Some(map) = attrs.get_map::<&str, &str>(i)? {
            for kv in map {
                let (key, value) = kv?;
                attrs_vec.push((key, value));
            }
        }

        let payload = match payload.get(i)?.expect("valid row index") {
            Value::String(s) => format!("string: {s}"),
            Value::Int64(n) => format!("int: {n}"),
            Value::Int64Slice(xs) => {
                let xs = xs.iter().map(|x| x.get()).collect::<Vec<i64>>();
                format!("array: {xs:?}")
            }
            other => format!("unexpected: {other:?}"),
        };

        println!("id={id} tags={tags:?} attrs={attrs_vec:?} payload={payload}");
    }

    Ok(())
}
