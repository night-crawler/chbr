use chbr::parse::block::parse_single;
use chbr::reader::{Array, LcNullableStr, TryRead as _};
use chbr::value::{LowCardinalitySliceIterator, MapIterator, Value};
use chbr::zc;
use testresult::TestResult;

fn load(name: &str) -> std::io::Result<Vec<u8>> {
    std::fs::read(crate::common::fixture(name))
}

#[test]
fn nullable_low_cardinality_null_inside_array_is_null_via_value_api() -> TestResult {
    let data = load("array_lc_nullable_string_null_slot.native")?;
    let (_, block) = parse_single(&data)?;
    let mark = &block.markers[0];

    let reader = Array::<LcNullableStr>::try_from(mark)?;
    let typed: Vec<Option<&str>> = reader.try_read(0)?.try_collect_vec()?;
    assert_eq!(typed, [Some("a"), None, Some("b")]);

    let row = mark.get(0)?.expect("row 0");
    let it = LowCardinalitySliceIterator::try_from(row)?;
    let values: Vec<Value> = it.collect::<Result<_, _>>()?;
    assert!(
        matches!(values[0], Value::String(s) if s == "a"),
        "{values:?}"
    );
    assert!(
        matches!(values[1], Value::Empty),
        "NULL element must be Value::Empty, got {:?}",
        values[1]
    );
    assert!(
        matches!(values[2], Value::String(s) if s == "b"),
        "{values:?}"
    );
    Ok(())
}

#[test]
fn uuid_and_ip_byte_order_matches_clickhouse_text_form() -> TestResult {
    let data = load("uuid_ip_order.native")?;
    let (_, block) = parse_single(&data)?;
    let uuid = block.mark("u")?.get_uuid(0)?.unwrap();
    assert_eq!(uuid.to_string(), "61f0c404-5cb3-11e7-907b-a6006ad3dba0");
    let Some(Value::Ipv6(ip6)) = block.mark("ip6")?.get(0)? else {
        panic!("ipv6");
    };
    assert_eq!(std::net::Ipv6Addr::from(*ip6).to_string(), "2001:db8::1");
    let Some(Value::Ipv4(ip4)) = block.mark("ip4")?.get(0)? else {
        panic!("ipv4");
    };
    assert_eq!(ip4.to_string(), "192.168.1.2");
    Ok(())
}

/// `SimpleAggregateFunction(f, T)` is read as a plain `T`.
#[test]
fn simple_aggregate_function_is_its_storage_type() -> TestResult {
    let data = load("simple_aggregate.native")?;
    let (_, block) = parse_single(&data)?;
    assert!(matches!(block.mark("x")?.get(0)?, Some(Value::UInt64(7))));
    let Some(Value::String(y)) = block.mark("y")?.get(0)? else {
        panic!("y");
    };
    assert_eq!(y, "a");
    Ok(())
}

/// `f` may carry parameters (`groupArrayArray(3)`), and `T` may be `LowCardinality(..)` or
/// `Map(..)`.
#[test]
fn simple_aggregate_function_with_parameters_and_composite_storage() -> TestResult {
    let data = load("simple_aggregate_parametric.native")?;
    let (_, block) = parse_single(&data)?;

    let a: &[zc::U64] = block.mark("a")?.get(0)?.unwrap().try_into()?;
    assert_eq!(a.iter().map(|v| v.get()).collect::<Vec<_>>(), [1, 2]);

    assert_eq!(
        block.mark("lc")?.get_str(0)?.map(|s| &**s),
        Some(b"a".as_slice())
    );

    let m: MapIterator<&str, u64> = block.mark("m")?.get(0)?.unwrap().try_into()?;
    assert_eq!(m.collect::<Result<Vec<_>, _>>()?, [("k", 5)]);
    Ok(())
}

/// `Mark::get(row)` is `None` once `row` reaches the column length, for Tuple, Map, and named
/// Tuple columns alike.
#[test]
fn tuple_and_map_rows_are_bounded_by_column_length() -> TestResult {
    let data = load("tuple_of_empty_array_and_empty_map.native")?;
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
