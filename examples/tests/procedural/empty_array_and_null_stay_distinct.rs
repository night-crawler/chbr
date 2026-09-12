use bloch::parse::block::parse_single;
use bloch::value::Value;
use bloch::zc;
use testresult::TestResult;

#[test]
fn empty_array_and_null_stay_distinct() -> TestResult {
    let data = std::fs::read(crate::common::fixture("empty_arrays.native"))?;
    let (_, block) = parse_single(&data)?;

    for col in ["v", "d", "d_nothing"] {
        let mark = block.mark(col)?;
        let rows: Vec<String> = (0..block.num_rows)
            .map(|row| {
                let value = mark.get(row)?.expect("row within the block");
                Ok(match value {
                    Value::NothingSlice => "[]".to_owned(),
                    value => match <Option<&[zc::I64]>>::try_from(value.clone()) {
                        Ok(Some(elements)) => format!("{elements:?}"),
                        Ok(None) => "null".to_owned(),
                        Err(_) => i64::try_from(value)?.to_string(),
                    },
                })
            })
            .collect::<bloch::Result<_>>()?;
        assert_eq!(rows, ["[]", "null", "7"], "{col}");
    }
    Ok(())
}
