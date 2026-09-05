use std::hint::black_box;

use chbr::FromBlock as _;
use testresult::TestResult;

use crate::common::BenchmarkCols;

// Kept out of line so the row loop is one inspectable symbol (`scripts/asm_summary.py`,
// `scripts/perf_profile.py --annotate`); criterion already called it out of line.
#[inline(never)]
pub fn consume_blocks(blocks: &[chbr::ParsedBlock<'_>]) -> TestResult<()> {
    for row in BenchmarkCols::iter_blocks(blocks) {
        let row = row?;

        black_box(row.id);
        black_box(row.lc_string_cd10);
        black_box(row.timestamp);
        black_box(row.count);
        black_box(row.some_number);

        black_box(row.lc_nullable_string_cd1000);
        black_box(row.lc_nullable_string_cd5000);
        black_box(row.lc_nullable_string_cd3000);
        black_box(row.lc_nullable_string_cd4000);
        black_box(row.lc_nullable_string_cd50000);
        black_box(row.lc_nullable_string_cd100);
        black_box(row.lc_nullable_string_cd500);
        black_box(row.lc_nullable_string8);
        black_box(row.lc_nullable_string_cd_00000);

        black_box(row.some_ip_address);

        for s in row.lc_tags {
            black_box(s?);
        }
        for s in row.nested_lc_string_cd10 {
            black_box(s?);
        }
        for &flag in row.nested_flag.try_as_slice()? {
            black_box(flag);
        }
        for v in row.nested_some_id.try_as_slice()? {
            black_box(v.get());
        }
        for v in row.nested_some_other_id.try_as_slice()? {
            black_box(v.get());
        }
    }

    Ok(())
}
