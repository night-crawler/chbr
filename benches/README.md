```sql
drop table if exists benchmark_sample;
create table benchmark_sample
(
    id                            UUID codec (ZSTD(6)),
    lc_string_cd10                LowCardinality(String) codec (ZSTD(6)),
    timestamp                     DateTime default now() codec (DoubleDelta, ZSTD(6)),
    count                         Float64 codec (ZSTD(6)),
    some_number                   UInt32 codec (T64, ZSTD(6)),
    lc_nullable_string_cd1000     LowCardinality(Nullable(String)) codec (ZSTD(6)),
    lc_nullable_string_cd5000     LowCardinality(Nullable(String)) codec (ZSTD(6)),
    lc_nullable_string_cd3000     LowCardinality(Nullable(String)) codec (ZSTD(6)),
    lc_nullable_string_cd4000     LowCardinality(Nullable(String)) codec (ZSTD(6)),
    lc_nullable_string_cd50000    LowCardinality(Nullable(String)) codec (ZSTD(6)),
    lc_nullable_string_cd100      LowCardinality(Nullable(String)) codec (ZSTD(6)),
    lc_nullable_string_cd500      LowCardinality(Nullable(String)) codec (ZSTD(6)),
    some_ip_address               Nullable(IPv6) codec (ZSTD(6)),
    lc_nullable_string8           LowCardinality(Nullable(String)) codec (ZSTD(6)),
    lc_tags                       Array(LowCardinality(String)) codec (ZSTD(6)),
    lc_nullable_string_cd_00000   LowCardinality(Nullable(String)) codec (ZSTD(6)),
    `nested_field.lc_string_cd10` Array(LowCardinality(String)) codec (ZSTD(6)),
    `nested_field.flag`           Array(Bool) codec (T64, ZSTD(6)),
    `nested_field.some_id`        Array(UInt128) codec (ZSTD(6)),
    `nested_field.some_other_id`  Array(UInt64) codec (Delta(2), ZSTD(6))
) engine = MergeTree()
order by (
    id,
    lc_string_cd10,
    toStartOfHour(`timestamp`),
    arrayZip(`nested_field.some_id`, `nested_field.some_other_id`)
)
partition by toStartOfDay(`timestamp`)
settings async_insert = true;

INSERT INTO benchmark_sample
SELECT generateUUIDv4(),
       concat('val_', toString(number % 10)),
       now() - toIntervalSecond(number),
       rand64() / 1e9,
       rand32(),
       concat('ns1k_', toString(number % 1000)),
       concat('ns5k_', toString(number % 5000)),
       concat('ns3k_', toString(number % 3000)),
       concat('ns4k_', toString(number % 4000)),
       concat('ns50k_', toString(number % 50000)),
       concat('ns100_', toString(number % 100)),
       concat('ns500_', toString(number % 500)),
       if(rand32() % 7 = 0, NULL, toIPv6('2001:db8::1')),
       if(rand32() % 5 = 0, NULL, concat('opt8_', toString(number % 10))),
       arrayMap(i - > concat('tag_', toString(rand32() % 30)), range(1 + (rand32() % 3))),
       if(rand32() % 3 = 0, NULL, concat('free_', toString(rand32()))),
       arrayMap(i - > concat('arr10_', toString((number + i) % 10)), range(1)),
       arrayMap(i - > (rand32() % 2) = 1, range(1)),
       arrayMap(i - > toUInt128(generateUUIDv4()), range(1)),
       arrayMap(i - > rand64(), range(1))
FROM numbers(1000000);

SELECT uniq(lc_string_cd10)             AS cd10,
       uniq(lc_nullable_string_cd1000)  AS cd1000,
       uniq(lc_nullable_string_cd5000)  AS cd5000,
       uniq(lc_nullable_string_cd3000)  AS cd3000,
       uniq(lc_nullable_string_cd4000)  AS cd4000,
       uniq(lc_nullable_string_cd50000) AS cd50000,
       uniq(lc_nullable_string_cd100)   AS cd100,
       uniq(lc_nullable_string_cd500)   AS cd500
FROM benchmark_sample;
```

```bash

echo 'select * from benchmark_sample order by id limit 100000 FORMAT NATIVE' \
  | curl -v \
      -H 'X-ClickHouse-User: test_user' \
      -H 'X-ClickHouse-Key: test_user' \
      'http://100.64.0.2:8124/?database=default' \
      --data-binary @- \
      --output test_data/benchmark_sample.native
      
      
echo 'select * from benchmark_sample order by id limit 100000 FORMAT RowBinary' \
  | curl -v \
      -H 'X-ClickHouse-User: test_user' \
      -H 'X-ClickHouse-Key: test_user' \
      'http://100.64.0.2:8124/?database=default' \
      --data-binary @- \
      --output test_data/benchmark_sample.rb
```
