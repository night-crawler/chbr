use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use testresult::TestResult;

#[test]
fn lc_empty_string_bug() -> TestResult {
    let data = std::fs::read(crate::common::fixture("activity_hw.native"))?;
    let (rem, block) = parse_single(&data)?;
    assert!(rem.is_empty());

    let marker = block.mark("resource_attrs")?;
    for i in 0..block.num_rows {
        let map_it: MapIterator<&str, &str> = marker.get(i)?.unwrap().try_into()?;
        for kv in map_it {
            assert!(kv.is_ok(), "empty strings should not be Value::Empty");
        }
    }

    Ok(())
}
