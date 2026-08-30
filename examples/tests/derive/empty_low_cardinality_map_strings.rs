use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{LcStr, Map};

#[derive(FromBlock)]
struct Row<'a> {
    resource_attrs: Map<'a, LcStr<'a>, LcStr<'a>>,
}

#[test]
fn preserves_empty_strings_in_low_cardinality_maps() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("activity_hw.native"))?;
    let (remaining, block) = parse_single(&data)?;
    assert!(remaining.is_empty());
    for row in Row::rows(&block)? {
        row?.resource_attrs.collect::<chbr::Result<Vec<_>>>()?;
    }
    Ok(())
}
