use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, I64, I128, U128};

#[derive(FromBlock)]
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
