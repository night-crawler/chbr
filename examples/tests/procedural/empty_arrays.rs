use chbr::parse::block::parse_single;
use chbr::value::{
    ArraySliceIterator, BoolSliceIterator, DateTime64SliceIterator, Decimal64SliceIterator,
    DynamicSliceIterator, Enum8SliceIterator, FixedStringSliceIterator,
    LowCardinalitySliceIterator, MapSliceIterator, NamedTupleSliceIterator, NestedIterator,
    NullableSliceIterator, TupleSliceIterator, Value, VariantSliceIterator,
};
use chbr::zc;
use chbr::{BStr, ParsedBlock};
use testresult::TestResult;

fn all_rows_empty<'a, I>(block: &'a ParsedBlock<'a>, col: &str) -> TestResult
where
    I: TryFrom<Value<'a>, Error = chbr::Error> + ExactSizeIterator,
{
    let mark = block.mark(col)?;
    for row in 0..block.num_rows {
        let value = mark.get(row)?.expect("row within the block");
        assert!(!matches!(value, Value::Empty), "{col}: `[]` read as NULL");
        let iter = I::try_from(value).map_err(|error| format!("{col}: {error}"))?;
        assert_eq!(iter.len(), 0, "{col}");
    }
    Ok(())
}

#[test]
fn empty_arrays_are_typed_empty_slices() -> TestResult {
    let data = std::fs::read(crate::common::fixture("empty_arrays.native"))?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.num_rows, 3);

    all_rows_empty::<BoolSliceIterator>(&block, "a_bool")?;
    all_rows_empty::<FixedStringSliceIterator>(&block, "a_fs")?;
    all_rows_empty::<Enum8SliceIterator>(&block, "a_e8")?;
    all_rows_empty::<DateTime64SliceIterator>(&block, "a_dt64")?;
    all_rows_empty::<Decimal64SliceIterator>(&block, "a_dec")?;
    all_rows_empty::<LowCardinalitySliceIterator>(&block, "a_lc")?;
    all_rows_empty::<LowCardinalitySliceIterator>(&block, "a_lcn")?;
    all_rows_empty::<NullableSliceIterator>(&block, "a_n")?;
    all_rows_empty::<ArraySliceIterator<&[u8]>>(&block, "a_a")?;
    all_rows_empty::<TupleSliceIterator>(&block, "a_t")?;
    all_rows_empty::<NamedTupleSliceIterator>(&block, "a_nt")?;
    all_rows_empty::<MapSliceIterator<&str, u8>>(&block, "a_m")?;
    all_rows_empty::<NestedIterator>(&block, "n")?;
    all_rows_empty::<VariantSliceIterator>(&block, "a_v")?;
    all_rows_empty::<DynamicSliceIterator>(&block, "a_d")?;

    let strings = block.mark("a_str")?;
    for row in 0..block.num_rows {
        let value = strings.get(row)?.expect("row within the block");
        assert_eq!(<&[&BStr]>::try_from(value)?, &[] as &[&BStr]);
    }
    Ok(())
}

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
            .collect::<chbr::Result<_>>()?;
        assert_eq!(rows, ["[]", "null", "7"], "{col}");
    }
    Ok(())
}
