use bloch::error::Error;
use bloch::parse::block::parse_single;
use bloch::value::{ArraySliceIterator, NullableSliceIterator, Value};
use pretty_assertions::assert_eq;
use testresult::TestResult;

/// A type for `Array(Nothing)` (CH: `[]`)
struct EmptyRow;

impl TryFrom<Value<'_>> for EmptyRow {
    type Error = Error;

    fn try_from(value: Value<'_>) -> Result<Self, Self::Error> {
        match value {
            Value::NothingSlice => Ok(Self),
            _ => Err(Error::MismatchedType("non-Nothing", "NothingSlice")),
        }
    }
}

#[test]
fn nothing() -> TestResult {
    let data = std::fs::read(crate::common::fixture("nothing.native"))?;
    let (_, block) = parse_single(&data)?;

    //    ┌─id─┬─arr─┬─n────┬─arr_n──┬─arr_arr─┐
    // 1. │  0 │ []  │ ᴺᵁᴸᴸ │ [NULL] │ [[]]    │
    // 2. │  1 │ []  │ ᴺᵁᴸᴸ │ [NULL] │ [[]]    │
    // 3. │  2 │ []  │ ᴺᵁᴸᴸ │ [NULL] │ [[]]    │
    //    └────┴─────┴──────┴────────┴─────────┘
    assert_eq!(block.num_rows, 3);
    let [_, arr, n, arr_n, arr_arr] = &*block.markers else {
        panic!("unexpected columns {:?}", block.col_names);
    };

    for i in 0..block.num_rows {
        let empty = arr.get(i)?.unwrap();
        assert!(
            matches!(empty, Value::NothingSlice),
            "arr mismatch at index {i}: {empty:?}"
        );
        assert!(
            <&[u8]>::try_from(empty).is_err(),
            "`[]` is not an `Array(UInt8)`"
        );

        let null: Option<&str> = n.get(i)?.unwrap().try_into()?;
        assert_eq!(null, None, "n mismatch at index {i}");
        assert_eq!(n.get_opt_str(i)?, Some(None), "n mismatch at index {i}");

        let nulls: NullableSliceIterator = arr_n.get(i)?.unwrap().try_into()?;
        let nulls = nulls
            .map(|item| item?.try_into())
            .collect::<Result<Vec<Option<&str>>, _>>()?;
        assert_eq!(nulls, [None], "arr_n mismatch at index {i}");

        let outer: ArraySliceIterator<EmptyRow> = arr_arr.get(i)?.unwrap().try_into()?;
        assert_eq!(outer.len(), 1, "arr_arr mismatch at index {i}");
        assert_eq!(outer.filter_map(Result::ok).count(), 1);
    }

    assert!(n.get(block.num_rows)?.is_none());
    assert_eq!(n.get_opt_str(block.num_rows)?, None);

    Ok(())
}
