use chbr::parse::block::parse_single;
use chbr::reader::{Array, ArrayIter, I64, Value as ValueReader, VariantNullable};
use chbr::value::Value;
use chbr::{FromBlock, FromVariant};

#[derive(FromVariant)]
enum Var<'a> {
    Array(ArrayIter<'a, I64<'a>>),
    Integer(i64),
    String(&'a str),
}

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    var: VariantNullable<'a, Var<'a>>,
    arr: Array<'a, ValueReader<'a>>,
}

#[test]
fn reads_null_variant_rows() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("variant_null.native"))?;
    let (_, block) = parse_single(&data)?;
    let mut rows = Vec::new();
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let var = match row.var {
            Some(Var::Array(values)) => format!("{:?}", values.try_collect_vec()?),
            Some(Var::Integer(value)) => value.to_string(),
            Some(Var::String(value)) => value.to_owned(),
            None => "null".to_owned(),
        };
        let elements = row
            .arr
            .map(|value| {
                Ok(match value? {
                    Value::Empty => "null".to_owned(),
                    Value::Int64(value) => value.to_string(),
                    Value::String(value) => value.to_string(),
                    other => panic!("unexpected element {other:?}"),
                })
            })
            .collect::<chbr::Result<Vec<_>>>()?;
        rows.push(format!("{var} {}", elements.join(",")));
    }
    assert_eq!(
        rows,
        [
            "1 1,null,a",
            "null ",
            "a null",
            "[1, 2, 3] b",
            "null null,null"
        ]
    );
    Ok(())
}
