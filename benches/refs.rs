use std::{fs, hint::black_box, net::Ipv6Addr};

use chbr::{BlockRow, BlocksIterator, parse::block::parse_many};
use chrono::Utc;
use clickhouse::rowbinary::de::deserialize_from;
use criterion::{Criterion, criterion_group, criterion_main};
use testresult::TestResult;
use zerocopy::little_endian::{U64, U128};

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

        let tags = lc_tags.get_array_lc_strs(i)?.unwrap().collect::<Vec<_>>();
        let nested_strs = nested_field_lc_string_cd10
            .get_array_lc_strs(i)?
            .unwrap()
            .collect::<Vec<_>>();

        let mut nested_some_id = Vec::with_capacity(nested_strs.len());
        let slice: &[U128] = nested_field_some_id.get_arr_uint128_slice(i)?.unwrap();
        nested_some_id.extend(slice.iter().map(|v| v.get()));

        let mut nested_some_other_id = Vec::with_capacity(nested_strs.len());
        let slice: &[U64] = nested_field_some_other_id.get_arr_uint64_slice(i)?.unwrap();
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
}

criterion_group!(benches, bench_readers);
criterion_main!(benches);
