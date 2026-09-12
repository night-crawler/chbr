use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Decimal32, Decimal64, Decimal128, I64};
use rust_decimal::Decimal;

const _SQL: &str = r#"
drop table if exists decimal_sample;

create table decimal_sample
(
    id   Int64,
    d32  Decimal32(3),
    d64  Decimal64(6),
    d128 Decimal128(12),
    d256 Decimal256(24)
) engine = MergeTree order by tuple();

insert into decimal_sample (id, d32, d64, d128, d256) values
    (0, toDecimal32(1.234, 3), toDecimal64(1.234567, 6), toDecimal128(1.234567890123, 12), toDecimal256(1.2345678901234567890123456789, 24)),
    (1, toDecimal32(2.345, 3), toDecimal64(2.345678, 6), toDecimal128(2.345678901234, 12), toDecimal256(2.3456789012345678901234567890, 24)),
    (2, toDecimal32(3.456, 3), toDecimal64(3.456789, 6), toDecimal128(3.456789012345, 12), toDecimal256(3.4567890123456789012345678901, 24));

select * from decimal_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    d32: Decimal32<'a>,
    d64: Decimal64<'a>,
    d128: Decimal128<'a>,
}

#[test]
fn reads_decimal_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("decimal_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected32 = [
        Decimal::new(1234, 3),
        Decimal::new(2345, 3),
        Decimal::new(3456, 3),
    ];
    let expected64 = [
        Decimal::new(1234567, 6),
        Decimal::new(2345678, 6),
        Decimal::new(3456789, 6),
    ];
    let expected128 = [
        Decimal::new(1234567890123, 12),
        Decimal::new(2345678901234, 12),
        Decimal::new(3456789012345, 12),
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(
            (row.d32, row.d64, row.d128),
            (expected32[index], expected64[index], expected128[index])
        );
    }
    Ok(())
}
