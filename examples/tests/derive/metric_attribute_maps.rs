use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{LcStr, Map};

#[derive(FromBlock)]
struct Row<'a> {
    resource_attrs: Map<'a, LcStr<'a>, LcStr<'a>>,
    scope_attrs: Map<'a, LcStr<'a>, LcStr<'a>>,
    attrs: Map<'a, LcStr<'a>, LcStr<'a>>,
}

#[test]
fn reads_metric_attribute_maps() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("metric_activity.native"))?;
    let (_, block) = parse_single(&data)?;
    let mut rows = 0;
    for row in Row::rows(&block)? {
        let row = row?;
        for map in [row.resource_attrs, row.scope_attrs, row.attrs] {
            map.collect::<chbr::Result<Vec<_>>>()?;
        }
        rows += 1;
    }
    assert_eq!(rows, block.num_rows);
    Ok(())
}
