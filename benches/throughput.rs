use std::{fs, hint::black_box, time::Duration};

mod common;

use chbr::FromBlock as _;
use chbr::parse::block::parse_many;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use testresult::TestResult;

use crate::common::BenchmarkCols;

#[inline(always)]
fn opt_len(s: Option<&str>) -> u128 {
    s.map_or(0, str::len) as u128
}

fn sum_blocks(blocks: &[chbr::ParsedBlock<'_>]) -> TestResult<u128> {
    let mut acc: u128 = 0;

    for row in BenchmarkCols::iter_blocks(blocks) {
        let row = row?;

        acc = acc.wrapping_add(row.id.as_u128());
        acc = acc.wrapping_add(row.lc_string_cd10.len() as u128);
        acc = acc.wrapping_add_signed(i128::from(row.timestamp.timestamp()));
        acc = acc.wrapping_add(u128::from(row.count.to_bits()));
        acc = acc.wrapping_add(u128::from(row.some_number));

        acc = acc.wrapping_add(opt_len(row.lc_nullable_string_cd1000));
        acc = acc.wrapping_add(opt_len(row.lc_nullable_string_cd5000));
        acc = acc.wrapping_add(opt_len(row.lc_nullable_string_cd3000));
        acc = acc.wrapping_add(opt_len(row.lc_nullable_string_cd4000));
        acc = acc.wrapping_add(opt_len(row.lc_nullable_string_cd50000));
        acc = acc.wrapping_add(opt_len(row.lc_nullable_string_cd100));
        acc = acc.wrapping_add(opt_len(row.lc_nullable_string_cd500));
        acc = acc.wrapping_add(opt_len(row.lc_nullable_string8));
        acc = acc.wrapping_add(opt_len(row.lc_nullable_string_cd_00000));

        acc = acc.wrapping_add(row.some_ip_address.map_or(0, std::net::Ipv6Addr::to_bits));

        for s in row.lc_tags {
            acc = acc.wrapping_add(s?.len() as u128);
        }
        for s in row.nested_lc_string_cd10 {
            acc = acc.wrapping_add(s?.len() as u128);
        }
        for &flag in row.nested_flag.try_as_slice()? {
            acc = acc.wrapping_add(u128::from(flag));
        }
        for v in row.nested_some_id.try_as_slice()? {
            acc = acc.wrapping_add(v.get());
        }
        for v in row.nested_some_other_id.try_as_slice()? {
            acc = acc.wrapping_add(u128::from(v.get()));
        }
    }

    Ok(acc)
}

/// End-to-end: parse the input and read every value
fn sum_all(input: &[u8]) -> TestResult<u128> {
    let blocks = parse_many(input)?;
    sum_blocks(&blocks)
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
        b.iter(|| black_box(sum_blocks(black_box(&blocks)).unwrap()))
    });

    group.bench_function("chbr_derive_sum", |b| {
        b.iter(|| black_box(sum_all(black_box(&native_data)).unwrap()))
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
