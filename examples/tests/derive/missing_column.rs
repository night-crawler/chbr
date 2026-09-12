use bloch::FromBlock;
use bloch::error::Error;
use bloch::parse::block::parse_single;
use bloch::reader::I64;

#[derive(FromBlock, Copy, Clone)]
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
