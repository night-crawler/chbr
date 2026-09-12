use bloch::parse::block::parse_single;
use bloch::reader::{Array, ArrayIter, I64, Value as ValueReader, VariantNullable};
use bloch::value::Value;
use bloch::{FromBlock, FromVariant};

const _SQL: &str = r#"
set allow_experimental_variant_type = 1;

drop table if exists variant_null_sample;

create table variant_null_sample
(
    id  Int64,
    var Variant(Int64, String, Array(Int64)),
    arr Array(Variant(Int64, String))
) engine = MergeTree order by tuple();

insert into variant_null_sample (id, var, arr) values
    (0, 1, [CAST(1::Int64, 'Variant(Int64, String)'), CAST(NULL, 'Variant(Int64, String)'), CAST('a', 'Variant(Int64, String)')]),
    (1, NULL, []),
    (2, 'a', [CAST(NULL, 'Variant(Int64, String)')]),
    (3, [1, 2, 3], [CAST('b', 'Variant(Int64, String)')]),
    (4, NULL, [CAST(NULL, 'Variant(Int64, String)'), CAST(NULL, 'Variant(Int64, String)')]);

optimize table variant_null_sample final;

select id, var, arr from variant_null_sample order by id format Native;
"#;

#[derive(FromVariant)]
enum Var<'a> {
    Array(ArrayIter<'a, I64<'a>>),
    Integer(i64),
    String(&'a str),
}

#[derive(FromBlock, Copy, Clone)]
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
            .collect::<bloch::Result<Vec<_>>>()?;
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
