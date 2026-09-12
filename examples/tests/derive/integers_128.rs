use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, I64, I128, U128};

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

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    u128_single: U128<'a>,
    u128_array: Array<'a, U128<'a>>,
    i128_single: I128<'a>,
    i128_array: Array<'a, I128<'a>>,
}

#[test]
fn reads_128_bit_integers() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("sample_128.native"))?;
    let (_, block) = parse_single(&data)?;
    let row = Row::rows(&block)?.next().expect("one row")?;
    assert_eq!(row.id, 0);
    assert_eq!(row.u128_single, 12345678901234567890123456789012u128);
    assert_eq!(
        row.u128_array.try_collect_vec()?,
        [
            12345678901234567890123456789012u128,
            98765432109876543210987654321098u128
        ]
    );
    assert_eq!(row.i128_single, 12345678901234567890123456789012i128);
    assert_eq!(
        row.i128_array.try_collect_vec()?,
        [
            12345678901234567890123456789012i128,
            -98765432109876543210987654321098i128
        ]
    );
    Ok(())
}
