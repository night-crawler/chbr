use chbr::parse::block::parse_single;
use chbr::reader::{
    Array, ArrayIter, Bool, DateTime64, Decimal64, Enum8, FixedStr, I64, LcNullableStr, LcStr, Map,
    Nullable, Str, Tuple, U8, Value as ValueReader, VariantNullable,
};
use chbr::value::Value;
use chbr::{FromBlock, FromVariant};

// `a_v` has no elements, so no payload is ever read.
#[allow(dead_code)]
#[derive(FromVariant)]
enum Elem<'a> {
    Integer(i64),
    String(&'a str),
}

// Payload sizes follow the mirrored `Variant(Array(Int64), Int64)`.
#[allow(clippy::large_enum_variant)]
#[derive(FromVariant)]
enum Var<'a> {
    Array(ArrayIter<'a, I64<'a>>),
    Integer(i64),
}

#[derive(FromBlock)]
struct Pair<'a> {
    a: Str<'a>,
    b: U8<'a>,
}

#[derive(FromBlock)]
struct Child<'a> {
    x: U8<'a>,
    y: Str<'a>,
}

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    a_bool: Array<'a, Bool<'a>>,
    a_str: Array<'a, Str<'a>>,
    a_fs: Array<'a, FixedStr<'a>>,
    a_e8: Array<'a, Enum8<'a>>,
    a_dt64: Array<'a, DateTime64<'a>>,
    a_dec: Array<'a, Decimal64<'a>>,
    a_lc: Array<'a, LcStr<'a>>,
    a_lcn: Array<'a, LcNullableStr<'a>>,
    a_n: Array<'a, Nullable<'a, I64<'a>>>,
    a_a: Array<'a, Array<'a, U8<'a>>>,
    a_t: Array<'a, Tuple<(Str<'a>, U8<'a>)>>,
    a_nt: Array<'a, Pair<'a>>,
    a_m: Array<'a, Map<'a, Str<'a>, U8<'a>>>,
    n: Array<'a, Child<'a>>,
    a_v: Array<'a, VariantNullable<'a, Elem<'a>>>,
    a_d: Array<'a, ValueReader<'a>>,
    v: VariantNullable<'a, Var<'a>>,
    d: ValueReader<'a>,
    d_nothing: ValueReader<'a>,
}

fn render(value: Value<'_>) -> String {
    match value {
        // `d` stores `[]` as `Array(Int64)`, `d_nothing` as `Array(Nothing)`.
        Value::Int64Slice(values) => format!("{values:?}"),
        Value::NothingSlice => "[]".to_owned(),
        Value::Int64(value) => value.to_string(),
        Value::Empty => "null".to_owned(),
        other => panic!("unexpected value {other:?}"),
    }
}

/// Every array in the block is `[]`, so ClickHouse serialized each element column with 0 rows.
/// Typed readers still bind to them, and `[]` stays distinct from NULL in the Variant/Dynamic
/// columns.
#[test]
fn reads_empty_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("empty_arrays.native"))?;
    let (_, block) = parse_single(&data)?;
    let mut variants = Vec::new();
    let mut dynamics = Vec::new();
    let mut nothings = Vec::new();
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);

        assert_eq!(row.a_bool.len(), 0);
        assert_eq!(row.a_str.len(), 0);
        assert_eq!(row.a_fs.len(), 0);
        assert_eq!(row.a_e8.len(), 0);
        assert_eq!(row.a_dt64.len(), 0);
        assert_eq!(row.a_dec.len(), 0);
        assert_eq!(row.a_lc.len(), 0);
        assert_eq!(row.a_lcn.len(), 0);
        assert_eq!(row.a_n.len(), 0);
        assert_eq!(row.a_a.len(), 0);
        assert_eq!(row.a_t.len(), 0);
        assert_eq!(row.a_nt.len(), 0);
        assert_eq!(row.a_m.len(), 0);
        assert_eq!(row.n.len(), 0);
        assert_eq!(row.a_v.len(), 0);
        assert_eq!(row.a_d.len(), 0);

        variants.push(match row.v {
            Some(Var::Array(values)) => format!("{:?}", values.try_collect_vec()?),
            Some(Var::Integer(value)) => value.to_string(),
            None => "null".to_owned(),
        });
        dynamics.push(render(row.d));
        nothings.push(render(row.d_nothing));
    }
    assert_eq!(variants, ["[]", "null", "7"]);
    assert_eq!(dynamics, ["[]", "null", "7"]);
    assert_eq!(nothings, ["[]", "null", "7"]);
    Ok(())
}
