use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, MultiPolygon, Point, Polygon, Ring};

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

#[derive(FromBlock, Copy, Clone)]
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
