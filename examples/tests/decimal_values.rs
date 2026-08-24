mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Decimal32, Decimal64, Decimal128, I64};
use rust_decimal::Decimal;

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    d32: Decimal32<'a>,
    d64: Decimal64<'a>,
    d128: Decimal128<'a>,
}

#[test]
fn reads_decimal_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("decimal_sample.native"))?;
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
