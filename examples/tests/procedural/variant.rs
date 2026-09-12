use bloch::parse::block::parse_single;
use bloch::value::Value;
use bloch::zc;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
set allow_experimental_variant_type = 1;

drop table if exists variant_sample;

create table variant_sample
(
    id  Int64,
    var Variant(Int64, String, Array(Int64))
) engine = MergeTree order by tuple();

insert into variant_sample (id, var) values
    (0, 1),
    (1, 'a'),
    (2, [1, 2, 3]),
    (3, 2),
    (4, 'b'),
    (5, [4, 5, 6]),
    (6, 3);

optimize table variant_sample;

select * from variant_sample order by id format Native;
"#;

#[test]
fn variant() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("variant.native"))?;
    let (_, block) = parse_single(&buf)?;
    // Variant(Array(Int64), Int64, String)
    //    ┌─id─┬─var─────┐
    // 1. │  0 │ 1       │
    // 2. │  1 │ a       │
    // 3. │  2 │ [1,2,3] │
    // 4. │  3 │ 2       │
    // 5. │  4 │ b       │
    // 6. │  5 │ [4,5,6] │
    // 7. │  6 │ 3       │
    //    └────┴─────────┘

    let variant_marker = &block.markers[1];
    // `Value` has no `PartialEq`; compare through the converted Rust value instead.

    let expected_str_repr = ["1", "a", "1, 2, 3", "2", "b", "4, 5, 6", "3"];

    for (i, expected) in expected_str_repr.iter().enumerate() {
        let value = variant_marker.get(i)?.unwrap();
        if let Ok(value) = <Value<'_> as TryInto<i64>>::try_into(value.clone()) {
            assert_eq!(format!("{value}"), *expected, "Mismatch at index {i}");
            continue;
        }

        if let Ok(value) = <Value<'_> as TryInto<&str>>::try_into(value.clone()) {
            assert_eq!(value, *expected, "Mismatch at index {i}");
            continue;
        }

        if let Ok(value) = <Value<'_> as TryInto<&[zc::I64]>>::try_into(value.clone()) {
            let parts = value
                .iter()
                .map(|v| format!("{}", v.get()))
                .collect::<Vec<_>>()
                .join(", ");
            assert_eq!(parts, *expected, "Mismatch at index {i}");
            continue;
        }

        panic!("Unexpected value type at index {i}: {:?}", value);
    }

    Ok(())
}
