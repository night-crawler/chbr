use bloch::parse::block::parse_single;
use bloch::value::{ArraySliceIterator, TupleSliceIterator};
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists geo_sample;

create table geo_sample
(
    id    Int64,
    p     Point,
    r     Ring,
    poly  Polygon,
    mpoly MultiPolygon,
    ls    LineString,
    mls   MultiLineString
) engine = MergeTree order by tuple();

insert into geo_sample (id, p, r, poly, mpoly, ls, mls) values
    (
        1,
        (10, 10),
        [(0, 0), (20, 0), (20, 20), (0, 20)],
        [[(0, 0), (20, 0), (20, 20), (0, 20)]],
        [
            [[(0, 0), (10, 0), (10, 10), (0, 10)]],
            [[(15, 15), (25, 15), (25, 25), (15, 25)]]
        ],
        [(0, 0), (20, 0), (20, 20), (0, 20)],
        [
            [(0, 0), (20, 0), (20, 20), (0, 20)],
            [(5, 5), (15, 5), (15, 15), (5, 15)]
        ]
    );

insert into geo_sample (id, p, r, poly, mpoly, ls, mls) values
    (
        2,
        (5, 5),
        [(0, 0), (10, 0), (5, 8)],
        [[(0, 0), (10, 0), (5, 8)]],
        [
            [
                [(0, 0), (10, 0), (5, 8)],
                [(4, 2), (6, 2), (5, 4)]
            ]
        ],
        [(0, 0), (10, 0), (5, 8)],
        [
            [(0, 0), (10, 10)],
            [(0, 10), (10, 0)]
        ]
    );

insert into geo_sample (id, p, r, poly, mpoly, ls, mls) values
    (
        3,
        (0, 0),
        [(0, 0), (3, 0), (3, 3), (0, 3)],
        [[(0, 0), (3, 0), (3, 3), (0, 3)]],
        [
            [[(0, 0), (3, 0), (3, 3), (0, 3)]],
            [
                [(5, 5), (9, 5), (9, 9), (5, 9)],
                [(6, 6), (8, 6), (8, 8), (6, 8)]
            ]
        ],
        [(0, 0), (3, 3), (6, 0)],
        [
            [(0, 0), (3, 0), (6, 0)],
            [(0, 0), (0, 3), (0, 6)]
        ]
    );

insert into geo_sample (id, p, r, poly, mpoly, ls, mls) values
    (
        4,
        (100, 100),
        [(100, 100), (110, 100), (110, 110), (100, 110)],
        [[(100, 100), (110, 100), (110, 110), (100, 110)]],
        [
            [[(100, 100), (105, 100), (105, 105), (100, 105)]],
            [[(108, 108), (112, 108), (112, 112), (108, 112)]]
        ],
        [(100, 100), (110, 110), (120, 100)],
        [
            [(100, 100), (105, 110), (110, 100)],
            [(120, 120), (130, 130), (140, 120)],
            [(150, 150), (160, 160)]
        ]
    );

optimize table geo_sample;

select * from geo_sample order by id format Native;
"#;

#[test]
fn geo_sample() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("geo_sample.native"))?;
    let (_, block) = parse_single(&buf)?;

    let expected_points = [(10.0, 10.0), (5.0, 5.0), (0.0, 0.0), (100.0, 100.0)];
    let points_marker = &block.markers[1];
    for (i, expected) in expected_points.iter().enumerate() {
        let value: (f64, f64) = points_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Point mismatch at index {i}");
    }

    let expected_rings: [Vec<(f64, f64)>; 4] = [
        vec![(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)],
        vec![(0.0, 0.0), (10.0, 0.0), (5.0, 8.0)],
        vec![(0.0, 0.0), (3.0, 0.0), (3.0, 3.0), (0.0, 3.0)],
        vec![
            (100.0, 100.0),
            (110.0, 100.0),
            (110.0, 110.0),
            (100.0, 110.0),
        ],
    ];
    let rings_marker = &block.markers[2];
    for (i, expected) in expected_rings.iter().enumerate() {
        let value: TupleSliceIterator = rings_marker.get(i)?.unwrap().try_into()?;
        let mut actual = Vec::with_capacity(expected.len());
        for point in value {
            let (x, y): (f64, f64) = point.try_into()?;
            actual.push((x, y));
        }
        assert_eq!(actual, *expected, "Ring mismatch at index {i}");
    }

    let expected_polygons = [
        vec![vec![(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]],
        vec![vec![(0.0, 0.0), (10.0, 0.0), (5.0, 8.0)]],
        vec![vec![(0.0, 0.0), (3.0, 0.0), (3.0, 3.0), (0.0, 3.0)]],
        vec![vec![
            (100.0, 100.0),
            (110.0, 100.0),
            (110.0, 110.0),
            (100.0, 110.0),
        ]],
    ];
    let polygons_marker = &block.markers[3];
    for (i, expected) in expected_polygons.iter().enumerate() {
        let value: ArraySliceIterator<TupleSliceIterator> =
            polygons_marker.get(i)?.unwrap().try_into()?;
        let mut actual = Vec::with_capacity(expected.len());
        for points in value.flatten() {
            let mut ring = Vec::with_capacity(expected[0].len());
            for p in points {
                let (x, y): (f64, f64) = p.try_into()?;
                ring.push((x, y));
            }
            actual.push(ring);
        }
        assert_eq!(actual, *expected, "Polygon mismatch at index {i}");
    }

    let expected_multipolygons = [
        vec![
            vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)],
            vec![(15.0, 15.0), (25.0, 15.0), (25.0, 25.0), (15.0, 25.0)],
        ],
        vec![
            vec![(0.0, 0.0), (10.0, 0.0), (5.0, 8.0)],
            vec![(4.0, 2.0), (6.0, 2.0), (5.0, 4.0)],
        ],
        vec![
            vec![(0.0, 0.0), (3.0, 0.0), (3.0, 3.0), (0.0, 3.0)],
            vec![(5.0, 5.0), (9.0, 5.0), (9.0, 9.0), (5.0, 9.0)],
            vec![(6.0, 6.0), (8.0, 6.0), (8.0, 8.0), (6.0, 8.0)],
        ],
        vec![
            vec![
                (100.0, 100.0),
                (105.0, 100.0),
                (105.0, 105.0),
                (100.0, 105.0),
            ],
            vec![
                (108.0, 108.0),
                (112.0, 108.0),
                (112.0, 112.0),
                (108.0, 112.0),
            ],
        ],
    ];
    let multipolygons_marker = &block.markers[4];
    for (i, expected) in expected_multipolygons.iter().enumerate() {
        let polygons: ArraySliceIterator<ArraySliceIterator<TupleSliceIterator>> =
            multipolygons_marker.get(i)?.unwrap().try_into()?;
        let mut actual = Vec::new();

        for polygon in polygons.flatten() {
            for ring in polygon.flatten() {
                let mut flat_ring = Vec::new();
                for pt in ring {
                    let (x, y): (f64, f64) = pt.try_into()?;
                    flat_ring.push((x, y));
                }
                actual.push(flat_ring);
            }
        }
        assert_eq!(actual, *expected, "Multi-polygon mismatch at index {i}");
    }

    let expected_linestrings = [
        vec![(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)],
        vec![(0.0, 0.0), (10.0, 0.0), (5.0, 8.0)],
        vec![(0.0, 0.0), (3.0, 3.0), (6.0, 0.0)],
        vec![(100.0, 100.0), (110.0, 110.0), (120.0, 100.0)],
    ];
    let linestrings_marker = &block.markers[5];
    for (i, expected) in expected_linestrings.iter().enumerate() {
        let value: TupleSliceIterator = linestrings_marker.get(i)?.unwrap().try_into()?;
        let mut actual = Vec::with_capacity(expected.len());
        for point in value {
            let (x, y): (f64, f64) = point.try_into()?;
            actual.push((x, y));
        }
        assert_eq!(actual, *expected, "LineString mismatch at index {i}");
    }

    let expected_multilinestrings = [
        vec![
            vec![(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)],
            vec![(5.0, 5.0), (15.0, 5.0), (15.0, 15.0), (5.0, 15.0)],
        ],
        vec![
            vec![(0.0, 0.0), (10.0, 10.0)],
            vec![(0.0, 10.0), (10.0, 0.0)],
        ],
        vec![
            vec![(0.0, 0.0), (3.0, 0.0), (6.0, 0.0)],
            vec![(0.0, 0.0), (0.0, 3.0), (0.0, 6.0)],
        ],
        vec![
            vec![(100.0, 100.0), (105.0, 110.0), (110.0, 100.0)],
            vec![(120.0, 120.0), (130.0, 130.0), (140.0, 120.0)],
            vec![(150.0, 150.0), (160.0, 160.0)],
        ],
    ];
    let multilinestrings_marker = &block.markers[6];
    for (i, expected) in expected_multilinestrings.iter().enumerate() {
        let lines: ArraySliceIterator<TupleSliceIterator> =
            multilinestrings_marker.get(i)?.unwrap().try_into()?;
        let mut actual = Vec::with_capacity(expected.len());

        for pts in lines.flatten() {
            let mut line = Vec::with_capacity(pts.len());
            for p in pts {
                let (x, y): (f64, f64) = p.try_into()?;
                line.push((x, y));
            }
            actual.push(line);
        }
        assert_eq!(actual, *expected, "Multi-lineString mismatch at index {i}");
    }

    Ok(())
}
