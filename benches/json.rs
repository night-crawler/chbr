use std::{fs, hint::black_box};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use bloch::{
    parse::block::parse_single,
    reader::{Json, TryRead as _},
};
use criterion::{Criterion, criterion_group, criterion_main};
use serde::Deserialize;

#[derive(Deserialize)]
struct Borrowed<'a> {
    #[serde(borrow)]
    key: &'a str,
}

#[derive(Deserialize)]
struct Numbers {
    array: [u64; 3],
}

#[derive(Deserialize)]
struct Nested {
    nested: Pair,
}

#[derive(Deserialize)]
struct Pair {
    a: u64,
    b: u64,
}

fn deserialize_json(c: &mut Criterion) {
    let data = fs::read("testdata/json.native").unwrap();
    let (_, block) = parse_single(&data).unwrap();
    let reader = Json::try_from(block.mark("json").unwrap()).unwrap();

    c.bench_function("json/borrowed_scalar", |b| {
        b.iter(|| {
            let value: Borrowed<'_> = black_box(reader)
                .try_read(black_box(0))
                .unwrap()
                .deserialize()
                .unwrap();
            black_box(value.key);
        });
    });
    c.bench_function("json/fixed_array", |b| {
        b.iter(|| {
            let value: Numbers = black_box(reader)
                .try_read(black_box(1))
                .unwrap()
                .deserialize()
                .unwrap();
            black_box(value.array);
        });
    });
    c.bench_function("json/nested_scalars", |b| {
        b.iter(|| {
            let value: Nested = black_box(reader)
                .try_read(black_box(2))
                .unwrap()
                .deserialize()
                .unwrap();
            black_box((value.nested.a, value.nested.b));
        });
    });
}

fn deserialize_json_wide(c: &mut Criterion) {
    // Wide+deep JSON fixture pulled from a live ClickHouse server; see
    // scratch/gen_json_bench_fixture.sh for its exact shape. Stresses PathTree
    // construction (wide root-level sibling set) and the serde subtree-activity walk.
    let data = fs::read("testdata/json_wide.native").unwrap();

    c.bench_function("json/wide_parse_and_construct", |b| {
        b.iter(|| {
            let (_, block) = parse_single(black_box(&data)).unwrap();
            black_box(Json::try_from(block.mark("j").unwrap()).unwrap());
        });
    });

    let (_, block) = parse_single(&data).unwrap();
    let reader = Json::try_from(block.mark("j").unwrap()).unwrap();
    c.bench_function("json/wide_deserialize_all_rows", |b| {
        b.iter(|| {
            for row in 0..black_box(reader).len() {
                let value: serde_json::Value = reader.try_read(row).unwrap().deserialize().unwrap();
                black_box(value);
            }
        });
    });
}

criterion_group!(benches, deserialize_json, deserialize_json_wide);
criterion_main!(benches);
