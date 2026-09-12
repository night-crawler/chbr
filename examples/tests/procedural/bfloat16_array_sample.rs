use bloch::Bf16Data;
use bloch::parse::block::parse_single;
use half::bf16;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn bfloat16_array_sample() -> TestResult {
    let data = std::fs::read(crate::common::fixture("bfloat16_array_sample.native"))?;
    let (_, block) = parse_single(&data)?;

    //    ┌─id─┬─arr_bf16─────────────────┐
    // 1. │  0 │ [3.125,2.703125,1.40625] │
    // 2. │  1 │ [0.57421875,1.6171875]   │
    // 3. │  2 │ [2.234375]               │
    // 4. │  3 │ []                       │
    // 5. │  4 │ [1.4140625,3.140625]     │
    //    └────┴──────────────────────────┘

    let expected = [
        vec![
            bf16::from_f32(3.125),
            bf16::from_f32(2.703125),
            bf16::from_f32(1.40625),
        ],
        vec![bf16::from_f32(0.57421875), bf16::from_f32(1.6171875)],
        vec![bf16::from_f32(2.234375)],
        vec![],
        vec![bf16::from_f32(1.4140625), bf16::from_f32(3.140625)],
    ];

    let bfloat16_array_marker = &block.markers[1];
    for (i, expected) in expected.iter().enumerate() {
        let value: &[Bf16Data] = bfloat16_array_marker.get(i)?.unwrap().try_into()?;
        let mut actual: Vec<bf16> = vec![];
        for item in value.iter().copied() {
            actual.push(item.into());
        }
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
