use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Bf16, F32, F64, I64};
use half::bf16;

const _SQL: &str = r#"
drop table if exists float_sample;

create table float_sample
(
    id   Int64,
    f32  Float32,
    f64  Float64,
    bf16 BFloat16
) engine = MergeTree order by tuple();

insert into float_sample (id, f32, f64, bf16) values
    (0, 3.14, 3.141592653589793, 3.14),
    (1, 2.71, 2.718281828459045, 2.71),
    (2, 1.41, 1.4142135623730951, 1.41),
    (3, 0.57721, 0.5772156649015329, 0.57721);

select * from float_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    f32: F32<'a>,
    f64: F64<'a>,
    bf16: Bf16<'a>,
}

#[test]
#[expect(clippy::approx_constant)]
fn reads_floating_point_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("float_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected32 = [3.14, 2.71, 1.41, 0.57721];
    let expected64 = [
        3.141592653589793,
        2.718281828459045,
        1.4142135623730951,
        0.5772156649015329,
    ];
    let expected_bf16 = [3.125, 2.703125, 1.40625, 0.57421875].map(bf16::from_f32);
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(
            (row.f32, row.f64, row.bf16),
            (expected32[index], expected64[index], expected_bf16[index])
        );
    }
    Ok(())
}
