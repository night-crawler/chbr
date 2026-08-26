use std::{fs, hint::black_box};

use chbr::{
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

criterion_group!(benches, deserialize_json);
criterion_main!(benches);
