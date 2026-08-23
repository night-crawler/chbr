use std::collections::HashMap;

use testresult::TestResult;

use super::*;
use crate::common::load;
use crate::error::Error;
use crate::parse::block::parse_single;
use crate::{FromBlock, FromVariant};

#[test]
fn array_map_sample_typed() -> TestResult {
    type ArrMap<'a> = Array<'a, Map<'a, Str<'a>, Str<'a>>>;

    let buf = load("./testdata/array_map_sample.native")?;
    let (_, block) = parse_single(&buf)?;

    let reader: ArrMap = Array::try_from(&block.markers[1])?;

    let expected: [Vec<HashMap<&str, &str>>; 6] = [
        vec![
            HashMap::from([("a", "apple"), ("b", "banana")]),
            HashMap::from([("c", "cherry")]),
        ],
        vec![
            HashMap::from([("d", "date")]),
            HashMap::from([("e", "elderberry"), ("f", "fig")]),
        ],
        vec![HashMap::from([("g", "grape"), ("h", "honeydew")])],
        vec![HashMap::from([("i", "kiwi")])],
        vec![],
        vec![HashMap::from([("j", "lemon"), ("k", "mango")])],
    ];

    assert_eq!(block.num_rows, expected.len());
    for (row, expected_row) in expected.iter().enumerate() {
        let outer = reader.try_read(row)?;
        let mut actual_row = Vec::new();

        for mp in outer {
            let mut h = HashMap::new();
            for kv in mp? {
                let (k, v) = kv?;
                h.insert(k, v);
            }
            actual_row.push(h);
        }

        assert_eq!(actual_row, *expected_row, "mismatch at top-level row {row}");
    }

    Ok(())
}

#[test]
fn derive_from_block_with_names() -> TestResult {
    const ID_COL: &str = "id";

    #[derive(FromBlock)]
    struct ArrMapRow<'a> {
        #[col(name = ID_COL)]
        id: I64<'a>,

        #[col(name = "arr_map")]
        maps: Array<'a, Map<'a, Str<'a>, Str<'a>>>,
    }

    let buf = load("./testdata/array_map_sample.native")?;
    let (_, block) = parse_single(&buf)?;

    let expected: [Vec<HashMap<&str, &str>>; 6] = [
        vec![
            HashMap::from([("a", "apple"), ("b", "banana")]),
            HashMap::from([("c", "cherry")]),
        ],
        vec![
            HashMap::from([("d", "date")]),
            HashMap::from([("e", "elderberry"), ("f", "fig")]),
        ],
        vec![HashMap::from([("g", "grape"), ("h", "honeydew")])],
        vec![HashMap::from([("i", "kiwi")])],
        vec![],
        vec![HashMap::from([("j", "lemon"), ("k", "mango")])],
    ];

    let mut num_rows = 0;
    for (row_idx, row) in ArrMapRow::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(row_idx)?);

        let mut actual = Vec::new();
        for map in row.maps {
            actual.push(map?.collect::<crate::Result<HashMap<_, _>>>()?);
        }
        assert_eq!(actual, expected[row_idx], "mismatch at row {row_idx}");
        num_rows += 1;
    }
    assert_eq!(num_rows, 6);

    Ok(())
}

#[test]
fn derive_nested_struct_in_array_of_tuples() -> TestResult {
    #[derive(FromBlock)]
    struct Fruit<'a> {
        name: LcStr<'a>,
        rank: I64<'a>,
    }

    #[derive(FromBlock)]
    struct Row<'a> {
        id: I64<'a>,
        arr: Array<'a, Fruit<'a>>,
    }

    let buf = load("./testdata/array_of_tuples.native")?;
    let (_, block) = parse_single(&buf)?;

    let expected: [Vec<(&str, i64)>; 6] = [
        vec![("apple", 1), ("banana", 2), ("cherry", 3)],
        vec![("date", 4), ("elderberry", 5)],
        vec![("fig", 6), ("grape", 7), ("honeydew", 8)],
        vec![("kiwi", 9)],
        vec![],
        vec![("lemon", 10), ("mango", 11)],
    ];

    for (row_idx, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(row_idx)?);
        let actual = row
            .arr
            .map(|f| f.map(|f| (f.name, f.rank)))
            .collect::<crate::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[row_idx], "mismatch at row {row_idx}");
    }

    Ok(())
}

#[test]
fn derive_nested_column_positional() -> TestResult {
    #[derive(FromBlock)]
    struct Child<'a> {
        child_id: U64<'a>,
        child_name: Str<'a>,
    }

    #[derive(FromBlock)]
    struct Row<'a> {
        id: I64<'a>,
        nes: Array<'a, Child<'a>>,
    }

    let buf = load("./testdata/simple_nested.native")?;
    let (_, block) = parse_single(&buf)?;

    let expected: [Vec<(u64, &str)>; 6] = [
        vec![(1, "Alice"), (2, "Bob")],
        vec![(3, "Charlie"), (4, "Diana")],
        vec![(5, "Eve")],
        vec![],
        vec![(6, "Frank"), (7, "Grace")],
        vec![(8, "Heidi")],
    ];

    for (row_idx, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(row_idx)?);
        let actual = row
            .nes
            .map(|c| c.map(|c| (c.child_id, c.child_name)))
            .collect::<crate::Result<Vec<_>>>()?;
        assert_eq!(actual, expected[row_idx], "mismatch at row {row_idx}");
    }

    Ok(())
}

#[test]
fn derive_col_tuple() -> TestResult {
    #[derive(FromBlock)]
    struct Row<'a> {
        id: I64<'a>,
        tup: Tuple<(I64<'a>, Str<'a>)>,
    }

    let buf = load("./testdata/tuple.native")?;
    let (_, block) = parse_single(&buf)?;

    let expected: [(i64, &str); 7] = [
        (1, "a"),
        (3, "ab"),
        (7, "ac"),
        (9, "ad"),
        (11, "ae"),
        (2, "af"),
        (3, "ag"),
    ];

    for (row_idx, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(row_idx)?);
        assert_eq!(row.tup, expected[row_idx], "mismatch at row {row_idx}");
    }

    Ok(())
}

#[test]
fn derive_nullable() -> TestResult {
    #[derive(FromBlock)]
    struct NullableRow<'a> {
        id: I64<'a>,
        nstr: Nullable<'a, Str<'a>>,
    }

    let buf = load("./testdata/nullable_string.native")?;
    let (_, block) = parse_single(&buf)?;

    let expected = [
        Some("hello"),
        None,
        Some("world"),
        Some("clickhouse"),
        None,
        Some("test"),
    ];

    for (row_idx, row) in NullableRow::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(row_idx)?);
        assert_eq!(row.nstr, expected[row_idx], "mismatch at row {row_idx}");
    }
    Ok(())
}

#[test]
fn derive_lc_nullable() -> TestResult {
    #[derive(FromBlock)]
    struct LcRow<'a> {
        id: I64<'a>,
        nlc_str: LcNullableStr<'a>,
    }

    let buf = load("./testdata/nullable_lc_str.native")?;
    let (_, block) = parse_single(&buf)?;

    let expected = [
        Some("apple"),
        None,
        Some("banana"),
        Some("cherry"),
        None,
        Some("date"),
    ];

    for (row_idx, row) in LcRow::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(row_idx)?);
        assert_eq!(row.nlc_str, expected[row_idx], "mismatch at row {row_idx}");
    }

    Ok(())
}

#[test]
fn derive_missing_column() -> TestResult {
    #[derive(FromBlock)]
    struct Row<'a> {
        #[col(name = "no_such_column")]
        id: I64<'a>,
    }

    let buf = load("./testdata/nullable_string.native")?;
    let (_, block) = parse_single(&buf)?;

    let Err(err) = Row::from_block(&block) else {
        panic!("expected missing column error");
    };
    assert!(
        matches!(&err, Error::ColumnNotFound(name) if name == "no_such_column"),
        "unexpected error: {err:?}"
    );

    Ok(())
}

#[test]
fn array_try_as_slice() -> TestResult {
    #[derive(FromBlock)]
    struct Row<'a> {
        id: I64<'a>,
        u128_array: Array<'a, U128<'a>>,
    }

    let buf = load("./testdata/sample_128.native")?;
    let (_, block) = parse_single(&buf)?;

    let row = Row::rows(&block)?.next().expect("expected a row")?;
    assert_eq!(row.id, 0);

    let slice = row.u128_array.try_as_slice()?;
    let values = slice.iter().map(|v| v.get()).collect::<Vec<_>>();
    assert_eq!(
        values,
        [
            12_345_678_901_234_567_890_123_456_789_012u128,
            98_765_432_109_876_543_210_987_654_321_098u128,
        ]
    );

    // the slice matches element-wise iteration
    let iterated = row
        .u128_array
        .map(|v| v.map_err(Into::into))
        .collect::<TestResult<Vec<_>>>()?;
    assert_eq!(values, iterated);

    Ok(())
}

#[test]
fn derive_col_value_escape_hatch() -> TestResult {
    #[derive(FromBlock)]
    struct Row<'a> {
        id: I64<'a>,
        #[col(name = "dyn")]
        value: Value<'a>,
    }

    let buf = load("./testdata/dynamic.native")?;
    let (_, block) = parse_single(&buf)?;

    let first = Row::rows(&block)?.next().expect("expected a row")?;
    assert_eq!(first.id, 0);

    let s: &str = first.value.try_into()?;
    assert_eq!(s, "string value");

    Ok(())
}

#[test]
fn derive_iter_blocks_flat() -> TestResult {
    #[derive(FromBlock)]
    struct Row<'a> {
        id: Uuid<'a>,
    }

    let buf = load("./testdata/benchmark_sample.native")?;
    let blocks = crate::parse::block::parse_many(&buf)?;
    assert!(blocks.len() > 1, "expected a multi-block file");

    let expected: usize = blocks.iter().map(|b| b.num_rows).sum();
    let mut count = 0usize;
    for row in Row::iter_blocks(&blocks) {
        let row = row?;
        let _: uuid::Uuid = row.id;
        count += 1;
    }
    assert_eq!(count, expected);

    Ok(())
}

#[test]
fn try_read_out_of_bounds() -> TestResult {
    let buf = load("./testdata/nullable_string.native")?;
    let (_, block) = parse_single(&buf)?;

    let reader = Nullable::<Str>::try_from(&block.markers[1])?;
    let Err(err) = reader.try_read(block.num_rows) else {
        panic!("expected out of bounds error");
    };
    assert!(
        matches!(err, Error::IndexOutOfBounds(idx, "Nullable") if idx == block.num_rows),
        "unexpected error: {err:?}"
    );

    Ok(())
}

#[test]
fn named_tuple_by_name() -> TestResult {
    // Field order deliberately doesn't match the def
    #[derive(FromBlock)]
    struct Fruit<'a> {
        rank: I64<'a>,
        #[col(name = "name")]
        title: Str<'a>,
    }

    #[derive(FromBlock)]
    struct Row<'a> {
        id: I64<'a>,
        tup: Fruit<'a>,
    }

    let buf = load("./testdata/named_tuple.native")?;
    let (_, block) = parse_single(&buf)?;

    let expected = ["apple", "banana", "cherry", "date", "elderberry", "fig"];

    let mut num_rows = 0;
    for (row_idx, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(row_idx)?);
        assert_eq!(
            row.tup.title, expected[row_idx],
            "mismatch at row {row_idx}"
        );
        assert_eq!(row.tup.rank, row.id * 10);
        num_rows += 1;
    }
    assert_eq!(num_rows, expected.len());

    Ok(())
}

#[test]
fn named_tuple_missing_field() -> TestResult {
    #[derive(FromBlock)]
    struct Fruit<'a> {
        #[col(name = "no_such_field")]
        rank: I64<'a>,
    }

    let buf = load("./testdata/named_tuple.native")?;
    let (_, block) = parse_single(&buf)?;

    let Err(err) = Fruit::try_from(&block.markers[1]) else {
        panic!("expected missing tuple field error");
    };
    assert!(
        matches!(&err, Error::ColumnNotFound(name) if name == "no_such_field"),
        "unexpected error: {err:?}"
    );

    Ok(())
}

#[test]
fn col_tuple_reads_named_tuple_positionally() -> TestResult {
    let buf = load("./testdata/named_tuple.native")?;
    let (_, block) = parse_single(&buf)?;

    let reader = Tuple::<(Str, I64)>::try_from(&block.markers[1])?;
    assert_eq!(reader.try_read(0)?, ("apple", 0));
    assert_eq!(reader.try_read(5)?, ("fig", 50));

    Ok(())
}

#[test]
fn derive_variant_enum() -> TestResult {
    // Variants in the server-canonicalized order of Variant(Array(Int64), Int64, String).
    #[derive(FromVariant)]
    enum Var<'a> {
        Arr(ArrayIter<'a, I64<'a>>),
        Num(i64),
        Str(&'a str),
    }

    let buf = load("./testdata/variant.native")?;
    let (_, block) = parse_single(&buf)?;

    let reader: Variant<Var> = Variant::try_from(&block.markers[1])?;

    let mut repr = Vec::with_capacity(block.num_rows);
    for i in 0..block.num_rows {
        repr.push(match reader.try_read(i)? {
            Var::Arr(it) => format!("{:?}", it.try_collect_vec()?),
            Var::Num(n) => n.to_string(),
            Var::Str(s) => s.to_owned(),
        });
    }
    assert_eq!(repr, ["1", "a", "[1, 2, 3]", "2", "b", "[4, 5, 6]", "3"]);

    Ok(())
}

#[test]
fn derive_variant_reader_override() -> TestResult {
    #[derive(FromVariant)]
    enum Var<'a> {
        #[col(reader = Array<'a, I64<'a>>)]
        Arr(ArrayIter<'a, I64<'a>>),
        #[col(reader = I64<'a>)]
        Num(i64),
        Str(&'a str),
    }

    let buf = load("./testdata/variant.native")?;
    let (_, block) = parse_single(&buf)?;

    let reader: Variant<Var> = Variant::try_from(&block.markers[1])?;
    let mut repr = Vec::with_capacity(block.num_rows);
    for i in 0..block.num_rows {
        repr.push(match reader.try_read(i)? {
            Var::Arr(it) => format!("{:?}", it.try_collect_vec()?),
            Var::Num(n) => n.to_string(),
            Var::Str(s) => s.to_owned(),
        });
    }
    assert_eq!(repr, ["1", "a", "[1, 2, 3]", "2", "b", "[4, 5, 6]", "3"]);

    Ok(())
}

#[test]
fn variant_null_rows_and_arity() -> TestResult {
    use crate::mark::{Mark, Variant as VariantMark};
    use crate::slice::ByteView;

    // No lifetime on purpose: exercises the synthesized-lifetime derive path.
    #[derive(FromVariant)]
    enum JustNum {
        Num(i64),
    }

    // Enum arity must match the number of inner types.
    #[derive(FromVariant)]
    #[expect(dead_code, reason = "construction is expected to fail")]
    enum TooWide<'a> {
        Num(i64),
        Str(&'a str),
    }

    let data = [1i64, 2]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect::<Vec<u8>>();
    // Rows: 1, NULL, 2.
    let mark = Mark::Variant(VariantMark {
        offsets: vec![0, 0, 1],
        discriminators: &[0, VariantMark::NULL_DISCRIMINATOR, 0],
        types: vec![Mark::Int64(ByteView::try_from(data.as_slice())?)],
    });

    let strict: Variant<JustNum> = Variant::try_from(&mark)?;
    let JustNum::Num(n) = strict.try_read(0)?;
    assert_eq!(n, 1);
    assert!(matches!(
        strict.try_read(1),
        Err(Error::MismatchedType("Null", _))
    ));

    let nullable: VariantNullable<JustNum> = VariantNullable::try_from(&mark)?;
    assert!(matches!(nullable.try_read(0)?, Some(JustNum::Num(1))));
    assert!(nullable.try_read(1)?.is_none());
    assert!(matches!(nullable.try_read(2)?, Some(JustNum::Num(2))));
    assert!(matches!(
        nullable.try_read(3),
        Err(Error::IndexOutOfBounds(3, _))
    ));

    assert!(matches!(
        Variant::<TooWide>::try_from(&mark),
        Err(Error::MismatchedType("Variant", _))
    ));

    Ok(())
}

#[test]
fn derive_variant_in_from_block() -> TestResult {
    #[derive(FromVariant)]
    enum Var<'a> {
        Arr(ArrayIter<'a, I64<'a>>),
        Num(i64),
        Str(&'a str),
    }

    #[derive(FromBlock)]
    struct Row<'a> {
        id: I64<'a>,
        var: VariantNullable<'a, Var<'a>>,
    }

    let buf = load("./testdata/variant.native")?;
    let (_, block) = parse_single(&buf)?;

    let mut repr = Vec::with_capacity(block.num_rows);
    for row in Row::rows(&block)? {
        let row = row?;
        repr.push(match row.var {
            Some(Var::Arr(it)) => format!("{}: {:?}", row.id, it.try_collect_vec()?),
            Some(Var::Num(n)) => format!("{}: {n}", row.id),
            Some(Var::Str(s)) => format!("{}: {s}", row.id),
            None => format!("{}: null", row.id),
        });
    }
    assert_eq!(
        repr,
        [
            "0: 1",
            "1: a",
            "2: [1, 2, 3]",
            "3: 2",
            "4: b",
            "5: [4, 5, 6]",
            "6: 3"
        ]
    );

    Ok(())
}
