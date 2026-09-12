-- Provenance of the ClickHouse Native fixtures in this directory.
--
-- Every `.native` file here is byte-reproducible from SQL:
--
--   * fixtures read by the examples (examples/tests/**) carry their script next to the test
--     that reads them, as `const _SQL` -- grep for `const _SQL` to find one;
--   * fixtures read only by the crate's own tests and benches are kept in this file.
--
-- To regenerate one, pipe its section into a client and capture stdout:
--
--   clickhouse-client --multiquery < section.sql > testdata/<fixture>.native
--
-- `FORMAT Native` is serialised client side, so the client version matters as much as the
-- server one. Everything below was captured against ClickHouse 25.3.2.39 with a matching or
-- newer client, except the sections that say otherwise. All sections are timezone independent.


-- --------------------------------------------------------------------------
-- testdata/datetime_tz.native
-- --------------------------------------------------------------------------

-- Every DateTime type spelling the server emits.
-- Note: `format Native` via clickhouse-client (client revision 0) strips the
-- zone from a top-level DateTime('tz') -> `DateTime`; nested ones survive.
-- All values are epoch literals, so the bytes do not depend on the server or
-- session timezone.

drop table if exists datetime_tz;

create table datetime_tz
(
    a DateTime,
    b DateTime('Europe/Berlin'),
    c DateTime64(3),
    d DateTime64(6, 'Asia/Tokyo'),
    e DateTime64,
    f Nullable(DateTime64(9)),
    g Array(DateTime('UTC'))
) engine = Memory;

insert into datetime_tz values
    (1700000000, 1700000000, 1700000000.123, 1700000000.123456, 1700000000.5, NULL, [1700000000]);

select a, b, c, d, e, f, g, cast(b, 'Nullable(DateTime(''Europe/Berlin''))') as h
from datetime_tz format Native;


-- --------------------------------------------------------------------------
-- testdata/dynamic_shared_variant.native
-- --------------------------------------------------------------------------

-- max_types=0 forces every value into the SharedVariant sub-column.
select number as id, cast(number * 10, 'Dynamic(max_types=0)') as d, toString(number) as after
from numbers(3) format Native;


-- --------------------------------------------------------------------------
-- testdata/empty_tuple.native
-- --------------------------------------------------------------------------

-- requires the ClickHouse 26.8 server (port 9100): replaying this against 25.3 desynchronises
-- the modern native protocol client on the Array(Tuple()) column, so the stream cannot be captured
-- there even though the produced bytes are identical.
select
    tuple()            as t,
    [tuple(), tuple()] as at,
    number
from numbers(2) format Native;


-- --------------------------------------------------------------------------
-- testdata/failed_blocks.native
-- --------------------------------------------------------------------------

set allow_suspicious_low_cardinality_types = 1;

drop table if exists failed_blocks;

create table failed_blocks
(
    first_seen            SimpleAggregateFunction(min, DateTime),
    last_seen             SimpleAggregateFunction(max, DateTime),
    name                  LowCardinality(String),
    unit                  String,
    description           String,
    type                  String,
    temporality           String,
    is_monotonic          Bool,
    attrs                 Map(String, String),
    paginator_total_count LowCardinality(UInt64)
) engine = Memory;

-- the fixture holds six single-row blocks: the Memory engine keeps every insert as its own block
-- and returns them in insertion order when read single-threaded
-- first_seen = 2026-01-08 11:46:14 UTC, last_seen = 2026-01-08 21:36:17 UTC (epoch literals keep
-- the script independent of the session time zone)
insert into failed_blocks values
    (toDateTime(1767872774), toDateTime(1767908177), 'process.min_fault', '1/s',  'Minor page faults',    'gauge', 'unspecified', false, {'profiling.host.name': 'zymtrace-dev-worker-1'}, 6);

insert into failed_blocks values
    (toDateTime(1767872774), toDateTime(1767908177), 'process.virt',      'byte', 'Virtual Memory usage', 'gauge', 'unspecified', false, {'profiling.host.name': 'zymtrace-dev-worker-1'}, 6);

insert into failed_blocks values
    (toDateTime(1767872774), toDateTime(1767908177), 'process.stime',     '%',    'system time',          'gauge', 'unspecified', false, {'profiling.host.name': 'zymtrace-dev-worker-1'}, 6);

insert into failed_blocks values
    (toDateTime(1767872774), toDateTime(1767908177), 'process.utime',     '%',    'user time',            'gauge', 'unspecified', false, {'profiling.host.name': 'zymtrace-dev-worker-1'}, 6);

insert into failed_blocks values
    (toDateTime(1767872774), toDateTime(1767908177), 'process.maj_fault', '1/s',  'Major page faults',    'gauge', 'unspecified', false, {'profiling.host.name': 'zymtrace-dev-worker-1'}, 6);

insert into failed_blocks values
    (toDateTime(1767872774), toDateTime(1767908177), 'process.rss',       'byte', 'RSS usage',            'gauge', 'unspecified', false, {'profiling.host.name': 'zymtrace-dev-worker-1'}, 6);

select * from failed_blocks settings max_threads = 1 format Native;


-- --------------------------------------------------------------------------
-- testdata/json_array_dynamic_null.native
-- --------------------------------------------------------------------------

select CAST('{"a":[1,null,"x"]}', 'JSON(a Array(Dynamic))') as j format Native;


-- --------------------------------------------------------------------------
-- testdata/json_escaped_keys.native
-- --------------------------------------------------------------------------

drop table if exists json_escaped_keys;

create table json_escaped_keys
(
    j JSON(arr Array(JSON), map Map(String, JSON), tuple Tuple(doc JSON))
) engine = MergeTree order by tuple();

insert into json_escaped_keys (j) values
    ('{"a%2Eb": 1, "a%2eb": 3, "a": {"b": 2}, "arr": [{"c%2Ed": "v%2E"}], "dynamic": [{"e%2Ef": "d"}], "map": {"m%2Ek": {"c%2Ed": "m"}}, "percent%20key": 4, "tuple": {"doc": {"c%2Ed": "t"}}}');

select * from json_escaped_keys format Native;


-- --------------------------------------------------------------------------
-- testdata/json_literal_percent_keys.native
-- --------------------------------------------------------------------------

select CAST('{"a%2Eb":2,"a%2eb":1}', 'JSON') as j format Native;


-- --------------------------------------------------------------------------
-- testdata/json_shared_variant.native
-- --------------------------------------------------------------------------

-- max_dynamic_types=0 forces every JSON leaf into the SharedVariant sub-column.
select number as id, cast(concat('{"a":', toString(number * 10), '}'), 'JSON(max_dynamic_types=0)') as j,
       toString(number) as after
from numbers(3) format Native;


-- --------------------------------------------------------------------------
-- testdata/json_tuple_dynamic_null.native
-- --------------------------------------------------------------------------

select CAST('{"a":{"x":null}}', 'JSON(a Tuple(x Dynamic))') as j format Native;


-- --------------------------------------------------------------------------
-- testdata/json_typed_dynamic_null.native
-- --------------------------------------------------------------------------

select CAST('{"a":null}', 'JSON(a Dynamic)') as j format Native;


-- --------------------------------------------------------------------------
-- testdata/json_typed_nullable.native
-- --------------------------------------------------------------------------

drop table if exists json_typed_nullable;

create table json_typed_nullable
(
    id   UInt64,
    json JSON(k Nullable(Int64), n Nullable(String))
) engine = MergeTree order by id;

insert into json_typed_nullable (id, json) values
    (0, '{"k": 1, "n": "x", "free": true}'),
    (1, '{"k": null, "n": null}'),
    (2, '{"k": -7, "n": "", "free": "s"}');

select * from json_typed_nullable order by id format Native;


-- --------------------------------------------------------------------------
-- testdata/json_typed_variant.native
-- --------------------------------------------------------------------------

drop table if exists json_typed_variant;

create table json_typed_variant
(
    json JSON(n Nullable(String), v Variant(Int64, String))
) engine = MergeTree order by tuple();

insert into json_typed_variant (json) values
    ('{}'),
    ('{"n": null, "v": null}'),
    ('{"n": "s", "v": 1}'),
    ('{"n": null, "v": "x", "free": 2}');

select * from json_typed_variant format Native;


-- --------------------------------------------------------------------------
-- Scratch: statements that do not produce a committed fixture
-- --------------------------------------------------------------------------

-- The type zoo, handy when checking that a new reader covers every spelling ClickHouse emits.
set allow_experimental_dynamic_type = 1;
set allow_experimental_json_type = 1;
set allow_suspicious_low_cardinality_types = 1;
drop table if exists all_types_demo;
create table all_types_demo
(
    -- integers
    i8               Int8 default 8,
    i16              Int16 default 16,
    i32              Int32 default 32,
    i64              Int64 default 64,
    i128             Int128 default 128,
    i256             Int256 default 256,

    u8               UInt8 default 88,
    u16              UInt16 default 1616,
    u32              UInt32 default 3232,
    u64              UInt64 default 6464,
    u128             UInt128 default 128128,
    u256             UInt256 default 256256,

    -- floats
    f32              Float32 default 3.14,
    f64              Float64 default 3.141592653589793,
    bf16             BFloat16 default 3.14,

    -- decimals
    d32              Decimal32(9) default toDecimal32(1, 1),
    d64              Decimal64(18) default toDecimal64(2, 1),
    d128             Decimal128(38) default toDecimal128(3, 1),
    d256             Decimal256(76) default toDecimal256(4, 1),

    -- strings
    s                String default 'just a string',
    fs_fixed16       FixedString(16) default 'fixed string',

    -- UUID & stuff
    uid              UUID default generateUUIDv4(),
    ip4d             IPv4 default '127.0.0.1',
    ip6d             IPv6 default '::1',

    -- date & time family
    d                Date default toDate(now()),
    date32           Date32 default toDate32(now()),
    t                Time(3) default toTime(now()),
    ts               DateTime default now(),
    ts_utc           DateTime('UTC') default now(),
    ts64_ms          DateTime64(3, 'UTC') default now(),

    -- enumerations
    e8               Enum8 ('Red'=1, 'Green'=2, 'Blue'=3) default 1,
    e16              Enum16 ('Foo'=1000, 'Bar'=2000) default 1000,

    -- nullable
    n_str            Nullable(String) DEFAULT 'nullable string',
    n_i32            Nullable(Int32) DEFAULT 42,
    --
    bool_flag        Boolean default true,
    tup              Tuple(String, UInt64) default ('example', 42),

    -- composite / variant
    arr_u8           Array(UInt8) default [1, 2, 3, 4, 5],
    tup_arr Array(Tuple(String, UInt64)) default [
        ('first', 1),
        ('second', 2),
        ('third', 3)
    ],


    m_str_u64        Map(String, UInt64) default mapFromArrays(['a', 'b', 'c'], [1, 2, 3]),
    json_doc         JSON default '{"a" : {"b" : 42}, "c" : [1, 2, 3]}',

    -- Low Cardinality
    lc_datetime      LowCardinality(DateTime) DEFAULT now(),
    lc_date          LowCardinality(Date) DEFAULT toDate(now()),
    lc_nullable_date LowCardinality(Nullable(Date)) DEFAULT toDate(now()),

    lc_str           LowCardinality(String) DEFAULT 'low cardinality string',
    lc_nul_str       LowCardinality(Nullable(String)) DEFAULT 'nullable low cardinality string',
    lc_uuid          LowCardinality(UUID) DEFAULT generateUUIDv4(),
    lc_nul_uuid      LowCardinality(Nullable(UUID)) DEFAULT generateUUIDv4(),
    lc_u64           LowCardinality(UInt64) DEFAULT 1234567890,

    -- nested weirdness
    nested_type      Nested(child_id UInt64, child_name String, scores Array(UInt32)),

    array_of_nested  Array(Nested (
        child_id   UInt64,
        child_name String,
        scores     Array(UInt32)
    )),

    variant Variant(String, UInt64, Array(UInt64)) default arrayMap(i -> toUInt64(i), [1, 2, 3]),

    scope_attrs Map(
        LowCardinality(String),
        Variant(
            String,
            Bool,
            Float64,
            Array(UInt8),
        )
    ) DEFAULT CAST(map(), 'Map(String, String)') CODEC(ZSTD(1)),


    `nested.id` Array(UInt64) default [1, 2, 3, 4],
    `nested.name` Array(String) default ['Alice', 'Bob', 'Charlie', 'Diana'],

    -- geo types
    p                Point default (10, 10),
    r                Ring default [(0, 0), (10, 0), (10, 10), (0, 10)],
    poly             Polygon default [[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]],
    mpoly            MultiPolygon default [[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]],
    ls               LineString default [(0, 0), (10, 0), (10, 10), (0, 10)],
    mls              MultiLineString default [[(0, 0), (10, 0), (10, 10), (0, 10)], [(1, 1), (2, 2), (3, 3)]],

    dyn_any          Dynamic default 'dynamic value',
    var_mix          Variant(UInt8, String) default 'example'

)
ENGINE = MergeTree
ORDER BY tuple();


-- testdata/benchmark_sample.native and .rb are 1e6 rows of generateUUIDv4()/rand64() data, so they
-- are NOT byte-reproducible; only their shape is. See benches/README.md for the dump commands.
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
)
engine = MergeTree()
order by (
    id,
    lc_string_cd10,
    toStartOfHour(`timestamp`),
    arrayZip(`nested_field.some_id`, `nested_field.some_other_id`)
)
partition by toStartOfDay(`timestamp`)
settings async_insert = true;

insert into benchmark_sample
select
    generateUUIDv4(),
    concat('val_', toString(number % 10)),
    now() - toIntervalSecond(number),
    rand64() / 1e9,
    rand32(),
    concat('ns1k_',  toString(number % 1000)),
    concat('ns5k_',  toString(number % 5000)),
    concat('ns3k_',  toString(number % 3000)),
    concat('ns4k_',  toString(number % 4000)),
    concat('ns50k_', toString(number % 50000)),
    concat('ns100_', toString(number % 100)),
    concat('ns500_', toString(number % 500)),
    if(rand32() % 7 = 0, NULL, toIPv6('2001:db8::1')),
    if(rand32() % 5 = 0, NULL, concat('opt8_', toString(number % 10))),
    arrayMap(i -> concat('tag_', toString(rand32() % 30)), range(1 + (rand32() % 3))),
    if(rand32() % 3 = 0, NULL, concat('free_', toString(rand32()))),
    arrayMap(i -> concat('arr10_', toString((number + i) % 10)), range(1)),
    arrayMap(i -> (rand32() % 2) = 1, range(1)),
    arrayMap(i -> toUInt128(generateUUIDv4()), range(1)),
    arrayMap(i -> rand64(), range(1))
from numbers(1000000);

-- testdata/json_wide.native was captured from a live server (wide, deep JSON documents) and has no
-- generating statement; it is used only by benches/json.rs.
