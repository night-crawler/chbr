use std::{fs, hint::black_box, net::Ipv6Addr, time::Duration};

use chbr::reader::{
    Array, Bool, DateTime, F64, Ipv6, LcNullableStr, LcStr, Nullable, U32, U64, U128, Uuid,
};
use chbr::{BlockRow, BlocksIterator, FromBlock, parse::block::parse_many};
use chrono::Utc;
use clickhouse::rowbinary::de::deserialize_from;
use criterion::{Criterion, criterion_group, criterion_main};
use testresult::TestResult;

#[derive(clickhouse::Row, serde::Deserialize, Debug)]
pub struct BenchmarkSample<'a> {
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: uuid::Uuid,

    pub lc_string_cd10: &'a str,

    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub timestamp: chrono::DateTime<Utc>,

    pub count: f64,
    pub some_number: u32,

    pub lc_nullable_string_cd1000: Option<&'a str>,
    pub lc_nullable_string_cd5000: Option<&'a str>,
    pub lc_nullable_string_cd3000: Option<&'a str>,
    pub lc_nullable_string_cd4000: Option<&'a str>,
    pub lc_nullable_string_cd50000: Option<&'a str>,
    pub lc_nullable_string_cd100: Option<&'a str>,
    pub lc_nullable_string_cd500: Option<&'a str>,

    pub some_ip_address: Option<Ipv6Addr>,

    pub lc_nullable_string8: Option<&'a str>,
    pub lc_tags: Vec<&'a str>,
    pub lc_nullable_string_cd_00000: Option<&'a str>,

    #[serde(rename = "nested_field.lc_string_cd10")]
    pub nested_lc_string_cd10: Vec<&'a str>,

    #[serde(rename = "nested_field.flag")]
    pub nested_flag: Vec<bool>,

    #[serde(rename = "nested_field.some_id")]
    pub nested_some_id: Vec<u128>,

    #[serde(rename = "nested_field.some_other_id")]
    pub nested_some_other_id: Vec<u64>,
}

impl<'a> TryFrom<BlockRow<'a>> for BenchmarkSample<'a> {
    type Error = chbr::error::Error;

    fn try_from(row: BlockRow<'a>) -> Result<Self, Self::Error> {
        let i = row.row_index();

        let [
            id,
            lc_string_cd10,
            timestamp,
            count,
            some_number,
            lc_nullable_string_cd1000,
            lc_nullable_string_cd5000,
            lc_nullable_string_cd3000,
            lc_nullable_string_cd4000,
            lc_nullable_string_cd50000,
            lc_nullable_string_cd100,
            lc_nullable_string_cd500,
            lc_nullable_string8,
            lc_nullable_string_cd_00000,
            some_ip_address,
            lc_tags,
            nested_field_lc_string_cd10,
            nested_field_flag,
            nested_field_some_id,
            nested_field_some_other_id,
            ..,
        ] = row.cols()
        else {
            unreachable!()
        };

        let tags = lc_tags
            .get_array_lc_strs(i)?
            .unwrap()
            .collect::<Result<Vec<_>, _>>()?;
        let nested_strs = nested_field_lc_string_cd10
            .get_array_lc_strs(i)?
            .unwrap()
            .collect::<Result<Vec<_>, _>>()?;

        let mut nested_some_id = Vec::with_capacity(nested_strs.len());
        let slice: &[zerocopy::little_endian::U128] =
            nested_field_some_id.get_arr_uint128_slice(i)?.unwrap();
        nested_some_id.extend(slice.iter().map(|v| v.get()));

        let mut nested_some_other_id = Vec::with_capacity(nested_strs.len());
        let slice: &[zerocopy::little_endian::U64] =
            nested_field_some_other_id.get_arr_uint64_slice(i)?.unwrap();
        nested_some_other_id.extend(slice.iter().map(|v| v.get()));

        let row = Self {
            id: id.get_uuid(i)?.unwrap(),
            lc_string_cd10: lc_string_cd10.get_str(i)?.unwrap(),
            timestamp: timestamp.get_datetime(i, Utc)?.unwrap(),
            count: count.get_f64(i)?.unwrap(),
            some_number: some_number.get_u32(i)?.unwrap(),
            lc_nullable_string_cd1000: lc_nullable_string_cd1000.get_opt_str(i)?.unwrap(),
            lc_nullable_string_cd5000: lc_nullable_string_cd5000.get_opt_str(i)?.unwrap(),
            lc_nullable_string_cd3000: lc_nullable_string_cd3000.get_opt_str(i)?.unwrap(),
            lc_nullable_string_cd4000: lc_nullable_string_cd4000.get_opt_str(i)?.unwrap(),
            lc_nullable_string_cd50000: lc_nullable_string_cd50000.get_opt_str(i)?.unwrap(),
            lc_nullable_string_cd100: lc_nullable_string_cd100.get_opt_str(i)?.unwrap(),
            lc_nullable_string_cd500: lc_nullable_string_cd500.get_opt_str(i)?.unwrap(),
            some_ip_address: some_ip_address.get_opt_ipv6(i)?.unwrap(),
            lc_nullable_string8: lc_nullable_string8.get_opt_str(i)?.unwrap(),
            lc_tags: tags,
            lc_nullable_string_cd_00000: lc_nullable_string_cd_00000.get_opt_str(i)?.unwrap(),
            nested_lc_string_cd10: nested_strs,
            nested_flag: nested_field_flag.get_arr_bool_iter(i)?.unwrap().collect(),
            nested_some_id,
            nested_some_other_id,
        };

        Ok(row)
    }
}

fn ch_rs_read(mut input: &[u8]) -> TestResult {
    while !input.is_empty() {
        let value: BenchmarkSample = deserialize_from(&mut input)?;
        black_box(value);
    }
    Ok(())
}

fn native_read(input: &[u8]) -> TestResult<()> {
    let mut blocks = parse_many(input)?;
    let it = BlocksIterator::new_ordered(
        &mut blocks,
        &[
            "id",
            "lc_string_cd10",
            "timestamp",
            "count",
            "some_number",
            "lc_nullable_string_cd1000",
            "lc_nullable_string_cd5000",
            "lc_nullable_string_cd3000",
            "lc_nullable_string_cd4000",
            "lc_nullable_string_cd50000",
            "lc_nullable_string_cd100",
            "lc_nullable_string_cd500",
            "lc_nullable_string8",
            "lc_nullable_string_cd_00000",
            "some_ip_address",
            "lc_tags",
            "nested_field.lc_string_cd10",
            "nested_field.flag",
            "nested_field.some_id",
            "nested_field.some_other_id",
        ],
    )?;

    for row in it {
        let row: BenchmarkSample = row.try_into()?;
        black_box(row);
    }

    Ok(())
}

#[derive(FromBlock)]
pub struct BenchmarkCols<'a> {
    id: Uuid<'a>,
    lc_string_cd10: LcStr<'a>,
    timestamp: DateTime<'a>,
    count: F64<'a>,
    some_number: U32<'a>,

    lc_nullable_string_cd1000: LcNullableStr<'a>,
    lc_nullable_string_cd5000: LcNullableStr<'a>,
    lc_nullable_string_cd3000: LcNullableStr<'a>,
    lc_nullable_string_cd4000: LcNullableStr<'a>,
    lc_nullable_string_cd50000: LcNullableStr<'a>,
    lc_nullable_string_cd100: LcNullableStr<'a>,
    lc_nullable_string_cd500: LcNullableStr<'a>,

    some_ip_address: Nullable<'a, Ipv6<'a>>,

    lc_nullable_string8: LcNullableStr<'a>,
    lc_tags: Array<'a, LcStr<'a>>,
    lc_nullable_string_cd_00000: LcNullableStr<'a>,

    #[col(name = "nested_field.lc_string_cd10")]
    nested_lc_string_cd10: Array<'a, LcStr<'a>>,

    #[col(name = "nested_field.flag")]
    nested_flag: Array<'a, Bool<'a>>,

    #[col(name = "nested_field.some_id")]
    nested_some_id: Array<'a, U128<'a>>,

    #[col(name = "nested_field.some_other_id")]
    nested_some_other_id: Array<'a, U64<'a>>,
}

// Both derived benchmarks parse the same Native input and construct the same
// `BenchmarkSample`. `BenchmarkCols::iter_blocks` calls the generated
// `BenchmarkCols::try_read`, which returns a `BenchmarkColsItem` containing
// every field before the five owned `Vec` conversions allocate.
fn native_derive_read(input: &[u8]) -> TestResult<()> {
    let blocks = parse_many(input)?;

    for row in BenchmarkCols::iter_blocks(&blocks) {
        let row = row?;
        let sample = BenchmarkSample {
            id: row.id,
            lc_string_cd10: row.lc_string_cd10,
            timestamp: row.timestamp.with_timezone(&Utc),
            count: row.count,
            some_number: row.some_number,
            lc_nullable_string_cd1000: row.lc_nullable_string_cd1000,
            lc_nullable_string_cd5000: row.lc_nullable_string_cd5000,
            lc_nullable_string_cd3000: row.lc_nullable_string_cd3000,
            lc_nullable_string_cd4000: row.lc_nullable_string_cd4000,
            lc_nullable_string_cd50000: row.lc_nullable_string_cd50000,
            lc_nullable_string_cd100: row.lc_nullable_string_cd100,
            lc_nullable_string_cd500: row.lc_nullable_string_cd500,
            some_ip_address: row.some_ip_address,
            lc_nullable_string8: row.lc_nullable_string8,
            lc_tags: row.lc_tags.try_collect_vec()?,
            lc_nullable_string_cd_00000: row.lc_nullable_string_cd_00000,
            nested_lc_string_cd10: row.nested_lc_string_cd10.try_collect_vec()?,
            nested_flag: row
                .nested_flag
                .try_as_slice()?
                .iter()
                .map(|&v| v == 1)
                .collect(),
            nested_some_id: row
                .nested_some_id
                .try_as_slice()?
                .iter()
                .map(|v| v.get())
                .collect(),
            nested_some_other_id: row
                .nested_some_other_id
                .try_as_slice()?
                .iter()
                .map(|v| v.get())
                .collect(),
        };
        black_box(sample);
    }

    Ok(())
}

// This variant bypasses `BlocksRows` and the generated `BenchmarkCols::try_read`,
// but still uses each field reader's `TryRead::try_read`. Rust evaluates the
// `BenchmarkSample` field initializers in order, so array collection and its
// allocations occur between reads of later `BenchmarkCols` fields. It measures
// this interleaved read-and-convert ordering rather than an allocation-free decoder.
fn native_derive_direct_read(input: &[u8]) -> TestResult<()> {
    use chbr::reader::TryRead as _;

    let blocks = parse_many(input)?;

    for block in &blocks {
        let cols = BenchmarkCols::from_block(block)?;
        for i in 0..block.num_rows {
            let sample = BenchmarkSample {
                id: cols.id.try_read(i)?,
                lc_string_cd10: cols.lc_string_cd10.try_read(i)?,
                timestamp: cols.timestamp.try_read(i)?.with_timezone(&Utc),
                count: cols.count.try_read(i)?,
                some_number: cols.some_number.try_read(i)?,
                lc_nullable_string_cd1000: cols.lc_nullable_string_cd1000.try_read(i)?,
                lc_nullable_string_cd5000: cols.lc_nullable_string_cd5000.try_read(i)?,
                lc_nullable_string_cd3000: cols.lc_nullable_string_cd3000.try_read(i)?,
                lc_nullable_string_cd4000: cols.lc_nullable_string_cd4000.try_read(i)?,
                lc_nullable_string_cd50000: cols.lc_nullable_string_cd50000.try_read(i)?,
                lc_nullable_string_cd100: cols.lc_nullable_string_cd100.try_read(i)?,
                lc_nullable_string_cd500: cols.lc_nullable_string_cd500.try_read(i)?,
                some_ip_address: cols.some_ip_address.try_read(i)?,
                lc_nullable_string8: cols.lc_nullable_string8.try_read(i)?,
                lc_tags: cols.lc_tags.try_read(i)?.try_collect_vec()?,
                lc_nullable_string_cd_00000: cols.lc_nullable_string_cd_00000.try_read(i)?,
                nested_lc_string_cd10: cols.nested_lc_string_cd10.try_read(i)?.try_collect_vec()?,
                nested_flag: cols
                    .nested_flag
                    .try_read(i)?
                    .try_as_slice()?
                    .iter()
                    .map(|&v| v == 1)
                    .collect(),
                nested_some_id: cols
                    .nested_some_id
                    .try_read(i)?
                    .try_as_slice()?
                    .iter()
                    .map(|v| v.get())
                    .collect(),
                nested_some_other_id: cols
                    .nested_some_other_id
                    .try_read(i)?
                    .try_as_slice()?
                    .iter()
                    .map(|v| v.get())
                    .collect(),
            };
            black_box(sample);
        }
    }

    Ok(())
}

fn bench_readers(c: &mut Criterion) {
    let rb_data =
        fs::read("testdata/benchmark_sample.rb").expect("missing testdata/benchmark_sample.rb");
    let native_data = fs::read("testdata/benchmark_sample.native")
        .expect("missing testdata/benchmark_sample.native");

    c.bench_function("serde", |b| {
        b.iter(|| ch_rs_read(black_box(&rb_data)).unwrap())
    });

    c.bench_function("chbr", |b| {
        b.iter(|| native_read(black_box(&native_data)).unwrap())
    });

    c.bench_function("chbr_derive", |b| {
        b.iter(|| native_derive_read(black_box(&native_data)).unwrap())
    });

    c.bench_function("chbr_derive_direct", |b| {
        b.iter(|| native_derive_direct_read(black_box(&native_data)).unwrap())
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(20))
        .sample_size(200);
    targets = bench_readers
}
criterion_main!(benches);
