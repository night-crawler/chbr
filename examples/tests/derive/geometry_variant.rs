use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, ArrayIter, Geometry, I64, Point, Ring, VariantNullable};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    // Variant is implicitly nullable, and so is Geometry.
    geo: VariantNullable<'a, Geometry<'a>>,
    mp: Ring<'a>,
    arr: Array<'a, VariantNullable<'a, Geometry<'a>>>,
}

fn ring<'a>(points: ArrayIter<'a, Point<'a>>) -> chbr::Result<Vec<(f64, f64)>> {
    points.try_collect_vec()
}

fn polygon<'a>(rings: ArrayIter<'a, Ring<'a>>) -> chbr::Result<Vec<Vec<(f64, f64)>>> {
    rings.map(|r| ring(r?)).collect()
}

#[test]
fn reads_geometry_by_discriminator_order() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("geometry_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let mut rows = 0;
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index + 1)?);
        match (row.id, row.geo) {
            (1, Some(Geometry::Point(p))) => assert_eq!(p, (1.0, 2.0)),
            (2, Some(Geometry::LineString(l))) => {
                assert_eq!(ring(l)?, [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]);
            }
            (3, Some(Geometry::MultiLineString(ml))) => assert_eq!(
                polygon(ml)?,
                [
                    vec![(0.0, 0.0), (1.0, 1.0)],
                    vec![(2.0, 2.0), (3.0, 3.0), (4.0, 2.0)],
                ]
            ),
            (4, Some(Geometry::Polygon(p))) => assert_eq!(
                polygon(p)?,
                [
                    vec![
                        (0.0, 0.0),
                        (10.0, 0.0),
                        (10.0, 10.0),
                        (0.0, 10.0),
                        (0.0, 0.0)
                    ],
                    vec![(4.0, 4.0), (5.0, 4.0), (5.0, 5.0), (4.0, 5.0), (4.0, 4.0)],
                ]
            ),
            (5, Some(Geometry::MultiPolygon(mp))) => {
                let polygons: Vec<_> = mp.map(|p| polygon(p?)).collect::<chbr::Result<_>>()?;
                assert_eq!(polygons.len(), 2);
                assert_eq!(
                    polygons[0],
                    [vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]]
                );
                assert_eq!(
                    polygons[1],
                    [
                        vec![(5.0, 5.0), (6.0, 5.0), (6.0, 6.0), (5.0, 5.0)],
                        vec![(5.2, 5.2), (5.5, 5.2), (5.5, 5.5), (5.2, 5.2)],
                    ]
                );
            }
            (6, Some(Geometry::Ring(r))) => {
                assert_eq!(ring(r)?, [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)])
            }
            (7, Some(Geometry::MultiPoint(mp))) => {
                assert_eq!(ring(mp)?, [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]);
            }
            (8, None) => {}
            (id, _) => panic!("unexpected geometry variant at id {id}"),
        }

        let mp = row.mp.try_collect_vec()?;
        match row.id {
            1 => assert_eq!(mp, [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]),
            3 => assert_eq!(mp, [(7.0, 7.0)]),
            _ => assert!(mp.is_empty()),
        }

        let mut arr = row.arr.try_collect_vec()?;
        match (row.id, arr.len()) {
            (1, 2) => {
                assert!(arr.pop().flatten().is_none());
                let Some(Some(Geometry::Point(p))) = arr.pop() else {
                    panic!("expected [Point, NULL] at id 1");
                };
                assert_eq!(p, (1.0, 2.0));
            }
            (3, 1) => {
                let Some(Some(Geometry::LineString(l))) = arr.pop() else {
                    panic!("expected [LineString] at id 3");
                };
                assert_eq!(ring(l)?, [(0.0, 0.0), (1.0, 1.0)]);
            }
            (4, 1) => assert!(arr.pop().flatten().is_none()),
            (7, 1) => {
                let Some(Some(Geometry::MultiPoint(mp))) = arr.pop() else {
                    panic!("expected [MultiPoint] at id 7");
                };
                assert_eq!(ring(mp)?, [(9.0, 9.0)]);
            }
            (_, 0) => {}
            (id, len) => panic!("unexpected arr of length {len} at id {id}"),
        }
        rows += 1;
    }
    assert_eq!(rows, 8);
    Ok(())
}
