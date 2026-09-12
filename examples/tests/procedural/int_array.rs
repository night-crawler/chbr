use bloch::parse::block::parse_single;
use bloch::zc;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn int_array() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("array.native"))?;
    // 0,[]
    // 128969003,[1]
    // 214500519,[1]
    // 301458964,[]
    // 475251162,[]
    // 1228122092,"[1, 2, 3, 4, 5]"
    // 1873422981,"[1, 2, 3, 4]"
    // 2172352370,"[1, 2, 3]"
    // 2181458171,"[1, 2]"
    // 2793473513,[]
    // 3697287021,"[1, 2, 3]"

    let (_, block) = parse_single(&buf)?;

    let index_marker = &block.markers[0];

    let indices = (0..block.num_rows)
        .map(|i| index_marker.get(i))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .map(i64::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let expected_ids = [
        0, 128969003, 214500519, 301458964, 475251162, 1228122092, 1873422981, 2172352370,
        2181458171, 2793473513, 3697287021,
    ];

    assert_eq!(indices, expected_ids);

    let expected_arrays = [
        vec![],
        vec![1],
        vec![1],
        vec![],
        vec![],
        vec![1, 2, 3, 4, 5],
        vec![1, 2, 3, 4],
        vec![1, 2, 3],
        vec![1, 2],
        vec![],
        vec![1, 2, 3],
    ];

    let arr_marker = &block.markers[1];

    let mut arrays = Vec::new();
    for index in 0..block.num_rows {
        let v: &[zc::I64] = arr_marker.get(index)?.unwrap().try_into()?;
        arrays.push(v);
    }

    assert_eq!(arrays, expected_arrays);

    Ok(())
}
