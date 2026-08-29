mod common;

use chbr::FromBlock;
use chbr::error::Error;
use chbr::mark::{self, Mark, StringView, Tuple};
use chbr::parse::block::parse_single;
use chbr::reader::I64;
use chbr::slice::ByteView;

#[derive(FromBlock)]
struct MissingColumn<'a> {
    #[col(name = "no_such_column")]
    value: I64<'a>,
}

#[test]
fn reports_mark_accessor_errors() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("array.native"))?;
    let (_, block) = parse_single(&data)?;
    assert!(
        matches!(MissingColumn::from_block(&block), Err(Error::ColumnNotFound(name)) if name == "no_such_column")
    );

    let bytes = [1_u8, 2];
    let mark = Mark::UInt8(ByteView::try_from(bytes.as_slice())?);
    assert!(matches!(
        mark.slice(1..3),
        Err(Error::RangeOutOfBounds(_, "UInt8"))
    ));

    assert!(matches!(
        mark::lc::Indices::try_from(Mark::String(StringView { data: Vec::new() })),
        Err(Error::CorruptedData(_))
    ));

    let indices = [0_u8];
    let invalid_dictionary = Mark::LowCardinality(mark::lc::LowCardinality {
        is_nullable: false,
        indices: mark::lc::Indices::U8(&indices),
        global_dictionary: None,
        additional_keys: Some(Box::new(Mark::String(StringView { data: Vec::new() }))),
    });
    let mut values = invalid_dictionary.slice_lc_strs(0..1)?;
    assert!(matches!(
        values.next(),
        Some(Err(Error::IndexOutOfBounds(0, "LowCardinality dictionary")))
    ));

    let tuple = Mark::Tuple(Tuple { values: Vec::new() });
    let oversized_start = u32::MAX as usize + 1;
    assert!(matches!(
        tuple.slice(oversized_start..oversized_start),
        Err(Error::ValueOutOfRange("usize", "u32", _))
    ));

    Ok(())
}
