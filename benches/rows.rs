use std::{env, fs, hint::black_box};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod common;
#[path = "common/consume.rs"]
mod consume;

use chbr::parse::block::parse_many;

use crate::consume::consume_blocks;

fn main() {
    let iters: usize = env::args()
        .nth(1)
        .map(|arg| arg.parse().expect("iterations must be an integer"))
        .unwrap_or(300);
    let data = fs::read("testdata/benchmark_sample.native")
        .expect("missing testdata/benchmark_sample.native");
    let blocks = parse_many(&data).expect("input parses");
    let rows: usize = blocks.iter().map(|block| block.num_rows).sum();
    println!("rows={rows} iters={iters}");

    for _ in 0..iters {
        consume_blocks(black_box(&blocks)).expect("rows read");
    }
}
