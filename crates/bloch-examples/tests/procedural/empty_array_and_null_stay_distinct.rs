use bloch::parse::block::parse_single;
use bloch::value::Value;
use bloch::zc;
use testresult::TestResult;

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
