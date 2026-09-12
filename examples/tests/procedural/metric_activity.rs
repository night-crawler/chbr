use std::collections::HashMap;

use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use testresult::TestResult;

#[test]
fn metric_activity() -> TestResult {
    let data = std::fs::read(crate::common::fixture("metric_activity.native"))?;
    let (_, block) = parse_single(&data)?;

    for index in 0..block.num_rows {
        for (col, name) in block.markers.iter().zip(block.col_names.iter()) {
            if !name.contains("attrs") {
                continue;
            }
            let value = col.get(index)?.unwrap();
            let value: MapIterator<&str, &str> = value.try_into()?;

            let mut map = HashMap::new();
            for (key, val) in value.flatten() {
                map.insert(key, val);
            }
        }
    }

    Ok(())
}
