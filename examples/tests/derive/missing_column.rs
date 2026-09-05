use chbr::FromBlock;
use chbr::error::Error;
use chbr::parse::block::parse_single;
use chbr::reader::I64;

#[derive(FromBlock)]
struct MissingColumn<'a> {
    #[col(name = "no_such_column")]
    value: I64<'a>,
}

#[test]
fn reports_missing_column() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("array.native"))?;
    let (_, block) = parse_single(&data)?;
    assert!(
        matches!(MissingColumn::from_block(&block), Err(Error::ColumnNotFound(name)) if name == "no_such_column")
    );
    Ok(())
}
