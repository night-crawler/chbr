use chbr::parse::block::parse_blocks;
use chbr::value::{BoolSliceIterator, LowCardinalitySliceIterator};
use chbr::{BlockIterator, BlockRow};
use clickhouse::rowbinary::de::deserialize_from;
use criterion::{Criterion, criterion_group, criterion_main};
use std::fs;
use std::hint::black_box;
use std::net::Ipv6Addr;
use testresult::TestResult;
use zerocopy::little_endian::{U64, U128};

#[derive(clickhouse::Row, serde::Deserialize, Debug)]
pub struct BenchmarkSample<'a> {
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: uuid::Uuid,

    pub lc_string_cd10: &'a str,

    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub timestamp: chrono::DateTime<chrono::Utc>,

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

    fn try_from(mut row: BlockRow<'a>) -> Result<Self, Self::Error> {
        let mut id: Option<uuid::Uuid> = None;
        let mut lc_string_cd10: Option<&'a str> = None;
        let mut timestamp: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut count: Option<f64> = None;
        let mut some_number: Option<u32> = None;

        let mut lc_nullable_string_cd1000: Option<&'a str> = None;
        let mut lc_nullable_string_cd5000: Option<&'a str> = None;
        let mut lc_nullable_string_cd3000: Option<&'a str> = None;
        let mut lc_nullable_string_cd4000: Option<&'a str> = None;
        let mut lc_nullable_string_cd50000: Option<&'a str> = None;
        let mut lc_nullable_string_cd100: Option<&'a str> = None;
        let mut lc_nullable_string_cd500: Option<&'a str> = None;
        let mut lc_nullable_string8: Option<&'a str> = None;
        let mut lc_nullable_string_cd_00000: Option<&'a str> = None;
        let mut some_ip_address: Option<Ipv6Addr> = None;

        let mut lc_tags = Vec::<&'a str>::new();
        let mut nested_lc_string_cd10 = Vec::<&'a str>::new();
        let mut nested_flag = Vec::<bool>::new();
        let mut nested_some_id = Vec::<u128>::new();
        let mut nested_some_other_id = Vec::<u64>::new();

        for (name, value) in &mut row {
            match name {
                "id" => id = Some(value.try_into()?),
                "lc_string_cd10" => lc_string_cd10 = Some(value.try_into()?),
                "timestamp" => {
                    let ts: chrono::DateTime<chrono_tz::Tz> = value.try_into()?;
                    let ts = ts.with_timezone(&chrono::Utc);
                    timestamp = Some(ts)
                }
                "count" => count = Some(value.try_into()?),
                "some_number" => some_number = Some(value.try_into()?),

                "lc_nullable_string_cd1000" => lc_nullable_string_cd1000 = value.try_into()?,
                "lc_nullable_string_cd5000" => lc_nullable_string_cd5000 = value.try_into()?,
                "lc_nullable_string_cd3000" => lc_nullable_string_cd3000 = value.try_into()?,
                "lc_nullable_string_cd4000" => lc_nullable_string_cd4000 = value.try_into()?,
                "lc_nullable_string_cd50000" => lc_nullable_string_cd50000 = value.try_into()?,
                "lc_nullable_string_cd100" => lc_nullable_string_cd100 = value.try_into()?,
                "lc_nullable_string_cd500" => lc_nullable_string_cd500 = value.try_into()?,
                "lc_nullable_string8" => lc_nullable_string8 = value.try_into()?,
                "lc_nullable_string_cd_00000" => lc_nullable_string_cd_00000 = value.try_into()?,

                "some_ip_address" => some_ip_address = value.try_into()?,

                "lc_tags" => {
                    let it: LowCardinalitySliceIterator = value.try_into()?;
                    for value in it {
                        let value: &str = value.try_into()?;
                        lc_tags.push(value);
                    }
                }

                "nested_field.lc_string_cd10" => {
                    let it: LowCardinalitySliceIterator = value.try_into()?;
                    for value in it {
                        let value: &str = value.try_into()?;
                        nested_lc_string_cd10.push(value);
                    }
                }
                "nested_field.flag" => {
                    let it: BoolSliceIterator = value.try_into()?;
                    nested_flag.extend(it);
                }
                "nested_field.some_id" => {
                    let slice: &[U128] = value.try_into()?;
                    nested_some_id.extend(slice.iter().map(|v| v.get()));
                }
                "nested_field.some_other_id" => {
                    let slice: &[U64] = value.try_into()?;
                    nested_some_other_id.extend(slice.iter().map(|v| v.get()));
                }

                other => {
                    return Err(chbr::error::Error::Parse(other.to_owned()));
                }
            }
        }

        Ok(BenchmarkSample {
            id: id.expect("`id` column missing"),
            lc_string_cd10: lc_string_cd10.expect("`lc_string_cd10` column missing"),
            timestamp: timestamp.expect("`timestamp` column missing"),
            count: count.expect("`count` column missing"),
            some_number: some_number.expect("`some_number` column missing"),

            lc_nullable_string_cd1000,
            lc_nullable_string_cd5000,
            lc_nullable_string_cd3000,
            lc_nullable_string_cd4000,
            lc_nullable_string_cd50000,
            lc_nullable_string_cd100,
            lc_nullable_string_cd500,
            some_ip_address,
            lc_nullable_string8,
            lc_tags,
            lc_nullable_string_cd_00000,
            nested_lc_string_cd10,
            nested_flag,
            nested_some_id,
            nested_some_other_id,
        })
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
    let blocks = parse_blocks(input)?;
    let it = BlockIterator::new(&blocks);

    for row in it {
        let row: BenchmarkSample = row.try_into()?;
        black_box(row);
    }

    Ok(())
}

fn bench_readers(c: &mut Criterion) {
    let rb_data =
        fs::read("test_data/benchmark_sample.rb").expect("missing test_data/benchmark_sample.rb");
    let native_data = fs::read("test_data/benchmark_sample.native")
        .expect("missing test_data/benchmark_sample.native");

    c.bench_function("serde", |b| {
        b.iter(|| ch_rs_read(black_box(&rb_data)).unwrap())
    });

    c.bench_function("chbr", |b| {
        b.iter(|| native_read(black_box(&native_data)).unwrap())
    });
}

criterion_group!(benches, bench_readers);
criterion_main!(benches);
