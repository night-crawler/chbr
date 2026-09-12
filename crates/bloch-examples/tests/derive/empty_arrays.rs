use bloch::parse::block::parse_single;
use bloch::reader::{
    Array, ArrayIter, Bool, DateTime64, Decimal64, Enum8, FixedStr, I64, LcNullableStr, LcStr, Map,
    Nullable, Str, Tuple, U8, Value as ValueReader, VariantNullable,
};
use bloch::value::Value;
use bloch::{FromBlock, FromVariant};

const _SQL: &str = r#"
set allow_experimental_dynamic_type = 1;
set allow_experimental_variant_type = 1;
set flatten_nested = 0;

drop table if exists empty_arrays;

create table empty_arrays
(
    id        Int64,
    a_bool    Array(Bool),
    a_str     Array(String),
    a_fs      Array(FixedString(2)),
    a_e8      Array(Enum8('a' = 1, 'b' = 2)),
    a_dt64    Array(DateTime64(3, 'UTC')),
    a_dec     Array(Decimal64(2)),
    a_lc      Array(LowCardinality(String)),
    a_lcn     Array(LowCardinality(Nullable(String))),
    a_n       Array(Nullable(Int64)),
    a_a       Array(Array(UInt8)),
    a_t       Array(Tuple(String, UInt8)),
    a_nt      Array(Tuple(a String, b UInt8)),
    a_m       Array(Map(String, UInt8)),
    n         Nested(x UInt8, y String),
    a_v       Array(Variant(Int64, String)),
    a_d       Array(Dynamic),
    v         Variant(Array(Int64), Int64),
    d         Dynamic,
    d_nothing Dynamic
) engine = Memory;

insert into empty_arrays values
    (0, [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []::Array(Int64)::Dynamic, array()::Dynamic),
    (1, [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], NULL::Variant(Array(Int64), Int64), NULL::Dynamic, NULL::Dynamic),
    (2, [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], 7::Int64, 7::Int64::Dynamic, 7::Int64::Dynamic);

select * from empty_arrays order by id format Native;
"#;

// `a_v` has no elements, so no payload is ever read.
#[expect(dead_code)]
#[derive(FromVariant)]
enum Elem<'a> {
    Integer(i64),
    String(&'a str),
}

// Payload sizes follow the mirrored `Variant(Array(Int64), Int64)`.
#[expect(clippy::large_enum_variant)]
#[derive(FromVariant)]
enum Var<'a> {
    Array(ArrayIter<'a, I64<'a>>),
    Integer(i64),
}

#[derive(FromBlock, Copy, Clone)]
struct Pair<'a> {
    a: Str<'a>,
    b: U8<'a>,
}

#[derive(FromBlock, Copy, Clone)]
struct Child<'a> {
    x: U8<'a>,
    y: Str<'a>,
}

#[derive(FromBlock, Copy, Clone)]
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
