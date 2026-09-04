use std::{fs, hint::black_box, time::Duration};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod common;
#[path = "common/consume.rs"]
mod consume;

use chbr::parse::block::parse_many;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use testresult::TestResult;

use crate::consume::consume_blocks;

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
