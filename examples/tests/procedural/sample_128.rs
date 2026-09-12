use bloch::parse::block::parse_single;
use bloch::zc;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists sample_128;

create table sample_128
(
    id          Int64,
    u128_single UInt128,
    u128_array  Array(UInt128),
    i128_single Int128,
    i128_array  Array(Int128)
) engine = MergeTree order by tuple();

insert into sample_128 (id, u128_single, u128_array, i128_single, i128_array) values
    (
        0,
        toUInt128('12345678901234567890123456789012'),
        [
            toUInt128('12345678901234567890123456789012'),
            toUInt128('98765432109876543210987654321098')
        ],
        toInt128('12345678901234567890123456789012'),
        [
            toInt128('12345678901234567890123456789012'),
            toInt128('-98765432109876543210987654321098')
        ]
    );

select * from sample_128 order by id format Native;
"#;

#[test]
fn sample_128() -> TestResult {
    let data = std::fs::read(crate::common::fixture("sample_128.native"))?;
    let (_, block) = parse_single(&data)?;

    //    ┌─id─┬──────────────────────u128_single─┬─u128_array──────────────────────────────────────────────────────────┬──────────────────────i128_single─┬─i128_array───────────────────────────────────────────────────────────┐
    // 1. │  0 │ 12345678901234567890123456789012 │ [12345678901234567890123456789012,98765432109876543210987654321098] │ 12345678901234567890123456789012 │ [12345678901234567890123456789012,-98765432109876543210987654321098] │
    //    └────┴──────────────────────────────────┴─────────────────────────────────────────────────────────────────────┴──────────────────────────────────┴──────────────────────────────────────────────────────────────────────┘

    let u128_marker = &block.markers[1];
    let expected_u128 = [12345678901234567890123456789012u128];
    for (i, expected) in expected_u128.iter().enumerate() {
        let value: u128 = u128_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let u128_array_marker = &block.markers[2];
    let expected_u128_array = [vec![
        12345678901234567890123456789012u128,
        98765432109876543210987654321098u128,
    ]];

    for (i, expected) in expected_u128_array.iter().enumerate() {
        let value: &[zc::U128] = u128_array_marker.get(i)?.unwrap().try_into()?;
        let mut actual = vec![];
        for item in value {
            actual.push(item.get());
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
