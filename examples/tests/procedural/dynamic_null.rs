use bloch::parse::block::parse_single;
use bloch::value::{DynamicSliceIterator, Value};
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
fn dynamic_null() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("dynamic_null.native"))?;
    let (_, block) = parse_single(&buf)?;
    // ┌─id─┬─dyn──┬─arr──────────┐
    // │  0 │ 42   │ [1,NULL,'a'] │
    // │  1 │ ᴺᵁᴸᴸ │ []           │
    // │  2 │ x    │ [NULL]       │
    // │  3 │ ᴺᵁᴸᴸ │ [NULL,NULL]  │
    // └────┴──────┴──────────────┘
    assert_eq!(block.num_rows, 4);
    let dyn_mark = &block.markers[1];
    let arr = &block.markers[2];

    let mut rows = Vec::new();
    for i in 0..block.num_rows {
        let scalar = render_variant_value(dyn_mark.get(i)?.expect("in range"))?;
        let it: DynamicSliceIterator = arr.get(i)?.expect("in range").try_into()?;
        let expected_len = it.len();
        let elements = it
            .map(|value| render_variant_value(value?))
            .collect::<TestResult<Vec<_>>>()?;
        assert_eq!(elements.len(), expected_len, "row {i}: iterator truncated");
        rows.push(format!("{scalar} {}", elements.join(",")));
    }
    assert_eq!(rows, ["42 1,null,a", "null ", "x null", "null null,null"]);

    assert!(dyn_mark.get(block.num_rows)?.is_none());
    let null: Option<&str> = dyn_mark.get(1)?.expect("in range").try_into()?;
    assert_eq!(null, None);

    Ok(())
}
