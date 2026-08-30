use std::{fs, hint::black_box, time::Duration};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod common;

use chbr::FromBlock as _;
use chbr::parse::block::parse_many;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use testresult::TestResult;

use crate::common::BenchmarkCols;

fn consume_blocks(blocks: &[chbr::ParsedBlock<'_>]) -> TestResult<()> {
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

fn consume_all(input: &[u8]) -> TestResult<()> {
    let blocks = parse_many(input)?;
    consume_blocks(&blocks)
}

fn bench_throughput(c: &mut Criterion) {
    let native_data = fs::read("testdata/benchmark_sample.native")
        .expect("missing testdata/benchmark_sample.native");
    let bytes = u64::try_from(native_data.len()).expect("input size fits in u64");

    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Bytes(bytes));
    group.bench_function("parse_many", |b| {
        b.iter(|| black_box(parse_many(black_box(&native_data)).unwrap()))
    });

    let blocks = parse_many(&native_data).expect("input parses");
    group.bench_function("iterate_parsed", |b| {
        b.iter(|| consume_blocks(black_box(&blocks)).unwrap())
    });

    group.bench_function("chbr_derive_sum", |b| {
        b.iter(|| consume_all(black_box(&native_data)).unwrap())
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(20))
        .sample_size(200);
    targets = bench_throughput
}
criterion_main!(benches);
