use bloch::parse::block::parse_single;
use bloch::value::{Value, VariantSliceIterator};
use pretty_assertions::assert_eq;
use testresult::TestResult;

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

fn render_variant_value(value: Value<'_>) -> TestResult<String> {
    Ok(match value {
        Value::Empty => "null".to_owned(),
        Value::Int64(n) => n.to_string(),
        Value::String(s) => s.to_string(),
        Value::Int64Slice(xs) => format!("{:?}", xs.iter().map(|x| x.get()).collect::<Vec<_>>()),
        other => panic!("unexpected value {other:?}"),
    })
}

#[test]
fn variant_null() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("variant_null.native"))?;
    let (_, block) = parse_single(&buf)?;
    // ┌─id─┬─var─────┬─arr──────────┐
    // │  0 │ 1       │ [1,NULL,'a'] │
    // │  1 │ ᴺᵁᴸᴸ    │ []           │
    // │  2 │ a       │ [NULL]       │
    // │  3 │ [1,2,3] │ ['b']        │
    // │  4 │ ᴺᵁᴸᴸ    │ [NULL,NULL]  │
    // └────┴─────────┴──────────────┘
    assert_eq!(block.num_rows, 5);
    let var = &block.markers[1];
    let arr = &block.markers[2];

    let mut rows = Vec::new();
    for i in 0..block.num_rows {
        let scalar = render_variant_value(var.get(i)?.expect("in range"))?;
        let it: VariantSliceIterator = arr.get(i)?.expect("in range").try_into()?;
        let expected_len = it.len();
        let elements = it
            .map(|value| render_variant_value(value?))
            .collect::<TestResult<Vec<_>>>()?;
        assert_eq!(elements.len(), expected_len, "row {i}: iterator truncated");
        rows.push(format!("{scalar} {}", elements.join(",")));
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

    // NULL is distinguishable from an out-of-range index
    assert!(var.get(block.num_rows)?.is_none());
    let null: Option<i64> = var.get(1)?.expect("in range").try_into()?;
    assert_eq!(null, None);
    let one: Option<i64> = var.get(0)?.expect("in range").try_into()?;
    assert_eq!(one, Some(1));

    Ok(())
}
