use bloch::parse::block::parse_single;
use half::bf16;
use pretty_assertions::assert_eq;
use testresult::TestResult;

//noinspection RsApproxConstant
#[expect(clippy::approx_constant)]
#[test]
fn float_sample() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("float_sample.native"))?;
    let (_, block) = parse_single(&buf)?;

    //    ┌─id─┬─────f32─┬────────────────f64─┬───────bf16─┐
    // 1. │  0 │    3.14 │  3.141592653589793 │      3.125 │
    // 2. │  1 │    2.71 │  2.718281828459045 │   2.703125 │
    // 3. │  2 │    1.41 │ 1.4142135623730951 │    1.40625 │
    // 4. │  3 │ 0.57721 │ 0.5772156649015329 │ 0.57421875 │
    //    └────┴─────────┴────────────────────┴────────────┘

    let f32_marker = &block.markers[1];
    let expected_f32 = [3.14f32, 2.71, 1.41, 0.57721];

    for (i, expected) in expected_f32.iter().enumerate() {
        let value: f32 = f32_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let f64_marker = &block.markers[2];
    let expected_f64 = [
        3.141592653589793,
        2.718281828459045,
        1.4142135623730951,
        0.5772156649015329,
    ];
    for (i, expected) in expected_f64.iter().enumerate() {
        let value: f64 = f64_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let bf16_marker = &block.markers[3];
    let expected_bf16 = [
        bf16::from_f32(3.125f32),
        bf16::from_f32(2.703125),
        bf16::from_f32(1.40625),
        bf16::from_f32(0.57421875),
    ];
    for (i, expected) in expected_bf16.iter().enumerate() {
        let value: bf16 = bf16_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
