//! Type names as ClickHouse emits them in `Native` block headers.

use bloch::mark::Mark;
use bloch::parse::block::parse_single;
use bloch::value::{ArraySliceIterator, TupleSliceIterator, Value, VariantSliceIterator};
use testresult::TestResult;

const _SQL: &str = r#"
-- Geometry / MultiPoint: MultiPoint is unknown to ClickHouse 26.3 and older
-- (including 25.3), so both the server and the client must be 26.8+;
-- reproduced on ClickHouse 26.8.2.7.

drop table if exists geometry_sample;

create table geometry_sample
(
    id  Int64,
    geo Geometry,
    mp  MultiPoint,
    arr Array(Geometry)
) engine = MergeTree order by id;

insert into geometry_sample values
    (1, readWKT('POINT(1 2)'), readWKT('MULTIPOINT(1 1,2 2,3 3)'), [readWKT('POINT(1 2)'), NULL]),
    (2, readWKT('LINESTRING(0 0,1 1,2 0)'), [], []),
    (3, readWKT('MULTILINESTRING((0 0,1 1),(2 2,3 3,4 2))'), [(7, 7)], [readWKT('LINESTRING(0 0,1 1)')]),
    (4, readWKT('POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,5 4,5 5,4 5,4 4))'), [], [NULL]),
    (5, readWKT('MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((5 5,6 5,6 6,5 5),(5.2 5.2,5.5 5.2,5.5 5.5,5.2 5.2)))'), [], []),
    (6, CAST([(0, 0), (1, 0), (1, 1)], 'Ring'), [], []),
    (7, readWKT('MULTIPOINT(1 1,2 2,3 3)'), [], [readWKT('MULTIPOINT(9 9)')]),
    (8, NULL, [], []);

optimize table geometry_sample final;

select id, geo, mp, arr from geometry_sample order by id format Native;
"#;

fn points(points: TupleSliceIterator<'_>) -> bloch::Result<Vec<(f64, f64)>> {
    points.map(<(f64, f64)>::try_from).collect()
}

fn rings(value: Value<'_>) -> bloch::Result<Vec<Vec<(f64, f64)>>> {
    ArraySliceIterator::<TupleSliceIterator>::try_from(value)?
        .map(|ring| points(ring?))
        .collect()
}

#[test]
fn geometry_is_a_variant_in_discriminator_order() -> TestResult {
    let data = std::fs::read(crate::common::fixture("geometry_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.num_rows, 8);

    let geo = block.mark("geo")?;
    assert!(matches!(geo, Mark::Variant(_)), "{geo:?}");

    // Rows are inserted as Point, LineString, MultiLineString, Polygon, MultiPolygon, Ring,
    // MultiPoint, NULL: each resolves to its own shape only if the discriminators map to
    // LineString=0, MultiLineString=1, MultiPolygon=2, Point=3, Polygon=4, Ring=5, MultiPoint=6.
    assert_eq!(<(f64, f64)>::try_from(geo.get(0)?.unwrap())?, (1.0, 2.0));
    assert_eq!(
        points(geo.get(1)?.unwrap().try_into()?)?,
        [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)]
    );
    assert_eq!(
        rings(geo.get(2)?.unwrap())?,
        [
            vec![(0.0, 0.0), (1.0, 1.0)],
            vec![(2.0, 2.0), (3.0, 3.0), (4.0, 2.0)],
        ]
    );
    assert_eq!(
        rings(geo.get(3)?.unwrap())?,
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
    );
    let multi_polygon: ArraySliceIterator<ArraySliceIterator<TupleSliceIterator>> =
        geo.get(4)?.unwrap().try_into()?;
    assert_eq!(multi_polygon.len(), 2);
    assert_eq!(
        points(geo.get(5)?.unwrap().try_into()?)?,
        [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]
    );
    assert_eq!(
        points(geo.get(6)?.unwrap().try_into()?)?,
        [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]
    );
    assert!(matches!(geo.get(7)?, Some(Value::Empty)));

    let mp = block.mark("mp")?;
    assert_eq!(mp.as_str(), "Array");
    assert_eq!(
        points(mp.get(0)?.unwrap().try_into()?)?,
        [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]
    );
    assert!(points(mp.get(1)?.unwrap().try_into()?)?.is_empty());

    let arr: VariantSliceIterator = block.mark("arr")?.get(0)?.unwrap().try_into()?;
    let arr = arr.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(arr.len(), 2);
    assert_eq!(<(f64, f64)>::try_from(arr[0].clone())?, (1.0, 2.0));
    assert!(matches!(arr[1], Value::Empty));
    Ok(())
}
