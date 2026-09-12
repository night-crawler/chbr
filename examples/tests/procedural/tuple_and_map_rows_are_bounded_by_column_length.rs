use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use testresult::TestResult;

const _SQL: &str = r#"
-- Every array in `t` and every map in `m` is empty; a column length derived from the array
-- elements or the map entries would be 0 instead of 2.
select
    ([]::Array(UInt8), toUInt8(number))             as t,
    map()::Map(String, UInt8)                       as m,
    (toUInt8(number), '')::Tuple(a UInt8, b String) as nt
from numbers(2) format Native;
"#;

/// `Mark::get(row)` is `None` once `row` reaches the column length, for Tuple, Map, and named
/// Tuple columns alike.
#[test]
fn tuple_and_map_rows_are_bounded_by_column_length() -> TestResult {
    let data = std::fs::read(crate::common::fixture(
        "tuple_of_empty_array_and_empty_map.native",
    ))?;
    let (_, block) = parse_single(&data)?;

    // t Tuple(Array(UInt8), UInt8)    m Map(String, UInt8)    nt Tuple(a UInt8, b String)
    // ([],0)                          {}                      (0,'')
    // ([],1)                          {}                      (1,'')
    //
    // Every array in `t` and every map in `m` is empty, so a length derived from the array
    // elements or the map entries would be 0 instead of 2.
    assert_eq!(block.num_rows, 2);

    for name in ["t", "m", "nt"] {
        let mark = block.mark(name)?;
        assert_eq!(mark.len(), block.num_rows, "{name}");
        assert!(mark.get(block.num_rows - 1)?.is_some(), "{name}");
        assert!(mark.get(block.num_rows)?.is_none(), "{name}");
    }

    let t: (&[u8], u8) = block.mark("t")?.get(1)?.unwrap().try_into()?;
    assert_eq!(t, (&[][..], 1));
    let m: MapIterator<&str, u8> = block.mark("m")?.get(1)?.unwrap().try_into()?;
    assert_eq!(m.collect::<Result<Vec<_>, _>>()?, []);
    let nt: (u8, &str) = block.mark("nt")?.get(1)?.unwrap().try_into()?;
    assert_eq!(nt, (1, ""));
    Ok(())
}
