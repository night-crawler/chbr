use bloch::parse::block::parse_single;
use bloch::value::{Value, VariantSliceIterator};
use pretty_assertions::assert_eq;
use testresult::TestResult;

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
