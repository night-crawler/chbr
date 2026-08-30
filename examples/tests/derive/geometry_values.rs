use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, F64, I64, Tuple};

type Point<'a> = Tuple<(F64<'a>, F64<'a>)>;
type Ring<'a> = Array<'a, Point<'a>>;
type Polygon<'a> = Array<'a, Ring<'a>>;
type MultiPolygon<'a> = Array<'a, Polygon<'a>>;

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    p: Point<'a>,
    r: Ring<'a>,
    poly: Polygon<'a>,
    mpoly: MultiPolygon<'a>,
    ls: Ring<'a>,
    mls: Polygon<'a>,
}

#[test]
fn reads_geometry_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("geo_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected_points = [(10.0, 10.0), (5.0, 5.0), (0.0, 0.0), (100.0, 100.0)];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index + 1)?);
        assert_eq!(row.p, expected_points[index]);
        assert!(!row.r.try_collect_vec()?.is_empty());
        assert!(!row.poly.try_collect_vec()?.is_empty());
        assert!(!row.mpoly.try_collect_vec()?.is_empty());
        assert!(!row.ls.try_collect_vec()?.is_empty());
        assert!(!row.mls.try_collect_vec()?.is_empty());
    }
    Ok(())
}
