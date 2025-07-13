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


create table array_sample (
    id Int64 default rand(),
    arr Array(Int64)
)
ENGINE = MergeTree
ORDER BY tuple();



insert into array_sample (id, arr) values ( 0, []);

select * from array_sample order by id;
optimize table array_sample;



create table tuple_sample
(
    id  Int64,
    tup Tuple(Int64, String)
) engine = MergeTree order by tuple();


insert into tuple_sample (id, tup)
values
    (0, (1, 'a')),
    (1, (3, 'ab')),
    (2, (7, 'ac')),
    (3, (9, 'ad')),
    (4, (11, 'ae')),
    (5, (2, 'af')),
    (6, (3, 'ag'))
;


optimize table tuple_sample;

select * from tuple_sample order by id;

drop table variant_sample;

create table variant_sample
(
    id Int64,
    var Variant(Int64, String, Array(Int64))
) engine = MergeTree order by tuple();

insert into variant_sample (id, var) values
        (0, 1),
        (1, 'a'),
        (2, [1, 2, 3]),
        (3, 2),
        (4, 'b'),
        (5, [4, 5, 6]),
        (6, 3);

optimize table variant_sample;



create table dynamic_sample
(
    id Int64,
    dyn Dynamic
) engine = MergeTree order by tuple();

insert into dynamic_sample (id, dyn) values
    (0, 'string value'),
    (1, 12345),
    (2, [1, 2, 3]),
    (3, {'key': 'value'}),
    (4, toDate('2023-01-01')),
    (5, null),
    (6, toDateTime('2023-01-01 12:00:00'));

insert into dynamic_sample (id, dyn) values
    (7, generateUUIDv4()),
    (8, toFloat64(3.14)),
    (9, toDecimal32(1.23, 2))
    ;


drop table json_sample;

create table json_sample
(
    id Int64,
    json JSON
) engine = MergeTree order by tuple();


insert into json_sample (id, json) values
    (0, '{"key": "value"}'),
    (1, '{"array": [1, 2, 3]}'),
    (2, '{"nested": {"a": 1, "b": 2}}'),
    (3, '{"boolean": true}'),
    (4, '{"null_value": null}'),
    (5, '{"date": "2023-01-01"}'),
    (6, '{"datetime": "2023-01-01T12:00:00Z"}');


insert into json_sample (id, json) values
    (7, '{"array": {"haha": true}}')
;

insert into json_sample (id, json) values
    (8, '{"complex": {"nested": {"array": [1, 2, 3], "value": "test"}}}'),
    (9, '{"empty_object": {}}'),
    (10, '{"empty_array": []}'),
    (11, '{"mixed_types": [1, "string", true, null]}'),
    (12, '{"uuid": "' || generateUUIDv4() || '"}');

insert into json_sample (id, json) values
    (13, '{"array": [1, 2, 3]}')
    (14, '{"array": [4, 5]}')
    (15, '{"array": [6, 7]}')
    (16, '{"array": [8, 9, 10]}')

    ;


insert into json_sample (id, json) values
    (13, '{"array": [4, 5, 6]}'),
    (14, '{"array": [7, 8, 9]}'),
    (15, '{"array": [10, 11, 12]}');

;
optimize table json_sample;


create table nullable_string
(
    id Int64,
    nstr Nullable(String)
) engine = MergeTree order by tuple();

insert into nullable_string (id, nstr) values
    (0, 'hello'),
    (1, null),
    (2, 'world'),
    (3, 'clickhouse'),
    (4, null),
    (5, 'test');



create table array_nullable_int64
(
    id Int64,
    arr Array(Nullable(Int64))
) engine = MergeTree order by tuple();

insert into array_nullable_int64 (id, arr) values
    (0, [1, 2, 3]),
    (1, [null, 4, 5]),
    (2, [6, null, 7]),
    (3, [8, 9, null]),
    (4, [null, null, null]),
    (5, [10]);




drop table array_lc_string;
create table array_lc_string (
        id Int64,
        arr Array(LowCardinality(String))
) engine = MergeTree order by tuple();


insert into array_lc_string (id, arr) values
    (0, ['apple', 'banana', 'cherry']),
    (1, ['date', 'elderberry']),
    (2, ['fig', 'grape', 'honeydew']),
    (3, ['kiwi']),
    (4, []),
    (5, ['lemon', 'mango']),
    (6, ['apple', 'banana', 'cherry', 'date']),
    (7, ['elderberry', 'fig', 'grape']),
    (8, ['honeydew', 'kiwi', 'lemon']),
    (9, ['mango', 'apple', 'banana']),
    (10, ['cherry', 'date', 'elderberry']),
    (11, ['fig', 'grape', 'honeydew', 'kiwi'])
;


create table array_lc_nullable_string (
        id Int64,
        arr Array(LowCardinality(Nullable(String)))
) engine = MergeTree order by tuple();


insert into array_lc_nullable_string (id, arr) values
    (0, ['apple', 'banana', null]),
    (1, [null, 'date', 'elderberry']),
    (2, ['fig', null, 'honeydew']),
    (3, [null]),
    (4, []),
    (5, ['lemon', null, 'mango']);



create table array_string (
    id Int64,
    arr Array(String)
) engine = MergeTree order by tuple();

insert into array_string (id, arr) values
    (0, ['apple', 'banana', 'cherry']),
    (1, ['date', 'elderberry']),
    (2, ['fig', 'grape', 'honeydew']),
    (3, ['kiwi']),
    (4, []),
    (5, ['lemon', 'mango']);


drop table map_nullable_lc_string;
create table map_nullable_lc_string
(
    id Int64,
    m  Map(
        String,
        Tuple(
            LowCardinality(Nullable(String)),
            LowCardinality(Nullable(String))
        )
    )
) engine = MergeTree order by tuple();

insert into map_nullable_lc_string (id, m) values
    (
     0,
     mapFromArrays(
         ['a', 'b', 'c'],
         [
             ('apple', 'fruit'),
             ('banana', 'fruit'),
             ('cherry', null)
         ]
     )
    );

insert into map_nullable_lc_string (id, m) values
    (
     1,
     mapFromArrays(
         ['d', 'e', 'f'],
         [
             ('date', 'fruit'),
             (null, 'aaaa'),
             ('fig', 'fruit')
         ]
     )
    );

select * from array_sample order by id;


create table plain_strings (
    id Int64,
    str String
) engine = MergeTree order by tuple();

insert into plain_strings (id, str) values
    (0, 'hello'),
    (1, 'world'),
    (2, 'clickhouse'),
    (3, 'test'),
    (4, 'example'),
    (5, 'data');

select * from plain_strings order by id;


create table plain_strings_array (
    id Int64,
    arr Array(String)
) engine = MergeTree order by tuple();

insert into plain_strings_array (id, arr) values
    (0, ['apple', 'banana', 'cherry']),
    (1, ['date', 'elderberry']),
    (2, ['fig', 'grape', 'honeydew']),
    (3, ['kiwi']),
    (4, []),
    (5, ['lemon', 'mango']);

select * from plain_strings_array;


create table plain_lc_string (
    id Int64,
    lc_str LowCardinality(String)
) engine = MergeTree order by tuple();

insert into plain_lc_string (id, lc_str) values
    (0, 'apple'),
    (1, 'banana'),
    (2, 'cherry'),
    (3, 'date'),
    (4, 'elderberry'),
    (5, 'fig');

select * from plain_lc_string order by id;


select * from array_lc_string order by id;

drop table if exists array_in_array_in64;
create table array_in_array_in64 (
    id Int64,
    arr Array(Array(Int64))
) engine = MergeTree order by tuple();

insert into array_in_array_in64 (id, arr) values
    (0, [[11, 22, 22, 77, 123], [333, 41]]),
    (1, [[11, 22], [7, 844, 12, 12, 0], [5, 5, 5]]),
    (2, [[9], [10, 11]]),
    (3, [[123, 134], [145]]),
    (4, [[156]]),
    (5, [[]]);


select * from array_in_array_in64;



select * from map_nullable_lc_string order by id;


create table map_sample
(
    id Int64,
    m  Map(String, String)
) engine = MergeTree order by tuple();
insert into map_sample (id, m) values
    (0, mapFromArrays(['a', 'b', 'c'], ['apple', 'banana', 'cherry'])),
    (1, mapFromArrays(['d', 'e'], ['date', 'elderberry'])),
    (2, mapFromArrays(['f', 'g', 'h'], ['fig', 'grape', 'honeydew'])),
    (3, mapFromArrays(['i'], ['kiwi'])),
    (4, mapFromArrays([], [])),
    (5, mapFromArrays(['j', 'k'], ['lemon', 'mango']));

select * from map_sample order by id;

create table array_map_sample
(
    id Int64,
    arr_map Array(Map(String, String))
) engine = MergeTree order by tuple();

insert into array_map_sample (id, arr_map) values
    (0, [mapFromArrays(['a', 'b'], ['apple', 'banana']), mapFromArrays(['c'], ['cherry'])]),
    (1, [mapFromArrays(['d'], ['date']), mapFromArrays(['e', 'f'], ['elderberry', 'fig'])]),
    (2, [mapFromArrays(['g', 'h'], ['grape', 'honeydew'])]),
    (3, [mapFromArrays(['i'], ['kiwi'])]),
    (4, []),
    (5, [mapFromArrays(['j', 'k'], ['lemon', 'mango'])]);

select * from array_map_sample order by id;


create table map_in_map
(
    id Int64,
    m  Map(String, Map(String, String))
) engine = MergeTree order by tuple();

insert into map_in_map (id, m) values
    (0, mapFromArrays(['a', 'b'], [mapFromArrays(['x', 'y'], ['apple', 'banana']), mapFromArrays(['z'], ['cherry'])])),
    (1, mapFromArrays(['c'], [mapFromArrays(['d'], ['date'])])),
    (2, mapFromArrays(['e', 'f'], [mapFromArrays(['g'], ['elderberry']), mapFromArrays(['h', 'i'], ['fig', 'grape'])])),
    (3, mapFromArrays(['j'], [mapFromArrays(['k'], ['kiwi'])])),
    (4, mapFromArrays([], [])),
    (5, mapFromArrays(['l', 'm'], [mapFromArrays(['n'], ['lemon']), mapFromArrays(['o', 'p'], ['mango', 'nectarine'])]));


select * from map_in_map order by id;


create table array_of_tuples
(
    id Int64,
    arr Array(Tuple(LowCardinality(String), Int64))
) engine = MergeTree order by tuple();

insert into array_of_tuples (id, arr) values
    (0, [('apple', 1), ('banana', 2), ('cherry', 3)]),
    (1, [('date', 4), ('elderberry', 5)]),
    (2, [('fig', 6), ('grape', 7), ('honeydew', 8)]),
    (3, [('kiwi', 9)]),
    (4, []),
    (5, [('lemon', 10), ('mango', 11)]);

select * from array_of_tuples order by id;


drop table uuid_and_dates;
create table uuid_and_dates (
        id UUID default generateUUIDv4(),
        date Date default toDate(now()),
        date32 Date32 default toDate32(now()),
        datetime DateTime default now(),
        datetime64 DateTime64(3, 'UTC') default now()
) engine = MergeTree order by tuple();

insert into uuid_and_dates (id, date, date32, datetime, datetime64) values
    ('00000000-0000-0000-0000-000000000001', '2023-01-01', '2023-01-01', '2023-01-01 12:00:00', '2023-01-01 12:00:00.123'),
    ('00000000-0000-0000-0000-000000000002', '2023-02-01', '2023-02-01', '2023-02-01 12:00:00', '2023-02-01 12:00:00.456'),
    ('00000000-0000-0000-0000-000000000003', '2023-03-01', '2023-03-01', '2023-03-01 12:00:00', '2023-03-01 12:00:00.789')
    ('00000000-0000-0000-0000-000000000004', '2023-03-01', -100, '2023-03-01 12:00:00', '2023-03-01 12:00:00.789')
;


select * from uuid_and_dates order by id;

create table lol (
                     datetime64 DateTime64(2, 'UTC') default now()

) engine = MergeTree order by tuple();


drop table if exists decimal_sample;
create table decimal_sample
(
    id Int64,
    d32 Decimal32(3),
    d64 Decimal64(6),
    d128 Decimal128(12),
    d256 Decimal256(24)
) engine = MergeTree order by tuple();

insert into decimal_sample (id, d32, d64, d128, d256) values
    (0, toDecimal32(1.234, 3), toDecimal64(1.234567, 6), toDecimal128(1.234567890123, 12), toDecimal256(1.2345678901234567890123456789, 24)),
    (1, toDecimal32(2.345, 3), toDecimal64(2.345678, 6), toDecimal128(2.345678901234, 12), toDecimal256(2.3456789012345678901234567890, 24)),
    (2, toDecimal32(3.456, 3), toDecimal64(3.456789, 6), toDecimal128(3.456789012345, 12), toDecimal256(3.4567890123456789012345678901, 24))
;

select * from decimal_sample order by id;


create table ip_sample
(
    id Int64,
    ip4 IPv4,
    ip6 IPv6
) engine = MergeTree order by tuple();

insert into ip_sample (id, ip4, ip6) values
     (0, '100.64.0.2', '2001:db8::ff00:42:8329'),
     (1, '127.0.0.1', '::1'),
     (2, '10.10.10.10', '2001:0db8:85a3:0000:0000:8a2e:0370:7334');

select * from ip_sample order by id;


create table geo_sample
(
    id Int64,
    p Point,
    r Ring,
    poly Polygon,
    mpoly MultiPolygon,
    ls LineString,
    mls MultiLineString
) engine = MergeTree order by tuple();



-- 1) Simple square
INSERT INTO geo_sample (id, p, r, poly, mpoly, ls, mls) VALUES
    (
        1,
        (10, 10),
        [(0, 0), (20, 0), (20, 20), (0, 20)],
        [[(0, 0), (20, 0), (20, 20), (0, 20)]],
        [  -- two simple squares as two polygons
            [[(0, 0), (10, 0), (10, 10), (0, 10)]],
            [[(15, 15), (25, 15), (25, 25), (15, 25)]]
            ],
        [(0, 0), (20, 0), (20, 20), (0, 20)],
        [  -- two concentric squares as two linestrings
            [(0, 0), (20, 0), (20, 20), (0, 20)],
            [(5, 5), (15, 5), (15, 15), (5, 15)]
            ]
    );

-- 2) Triangle and triangular multi-polygon with a hole
INSERT INTO geo_sample (id, p, r, poly, mpoly, ls, mls) VALUES
    (
        2,
        (5, 5),
        [(0, 0), (10, 0), (5, 8)],  -- open ring will implicitly close
        [[(0, 0), (10, 0), (5, 8)]],
        [  -- one triangle with a small triangular hole
            [
                [(0, 0), (10, 0), (5, 8)],        -- outer
                [(4, 2), (6, 2), (5, 4)]          -- hole
                ]
            ],
        [(0, 0), (10, 0), (5, 8)],
        [  -- two linestrings forming an X
            [(0, 0), (10, 10)],
            [(0, 10), (10, 0)]
            ]
    );

-- 3) Nested multi-polygon (one simple, one with hole)
INSERT INTO geo_sample (id, p, r, poly, mpoly, ls, mls) VALUES
    (
        3,
        (0, 0),
        [(0, 0), (3, 0), (3, 3), (0, 3)],
        [[(0, 0), (3, 0), (3, 3), (0, 3)]],
        [
            [[(0, 0), (3, 0), (3, 3), (0, 3)]],         -- simple square
            [                                           -- square with hole
                [(5, 5), (9, 5), (9, 9), (5, 9)],       -- outer
                [(6, 6), (8, 6), (8, 8), (6, 8)]        -- hole
                ]
            ],
        [(0, 0), (3, 3), (6, 0)],
        [
            [(0, 0), (3, 0), (6, 0)],                   -- horizontal
            [(0, 0), (0, 3), (0, 6)]                    -- vertical
            ]
    );

-- 4) Complex multi-linestring
INSERT INTO geo_sample (id, p, r, poly, mpoly, ls, mls) VALUES
    (
        4,
        (100, 100),
        [(100, 100), (110, 100), (110, 110), (100, 110)],
        [[(100, 100), (110, 100), (110, 110), (100, 110)]],
        [
            [[(100, 100), (105, 100), (105, 105), (100, 105)]],
            [[(108, 108), (112, 108), (112, 112), (108, 112)]]
            ],
        [(100, 100), (110, 110), (120, 100)],
        [
            [(100, 100), (105, 110), (110, 100)],
            [(120, 120), (130, 130), (140, 120)],
            [(150, 150), (160, 160)]
            ]
    );


optimize table geo_sample;

select * from geo_sample order by id;


create table float_sample
(
    id Int64,
    f32 Float32,
    f64 Float64,
    bf16 BFloat16
) engine = MergeTree order by tuple();

insert into float_sample (id, f32, f64, bf16) values
    (0, 3.14, 3.141592653589793, 3.14),
    (1, 2.71, 2.718281828459045, 2.71),
    (2, 1.41, 1.4142135623730951, 1.41),
    (3, 0.57721, 0.5772156649015329, 0.57721);

select * from float_sample order by id;


create table bool_array_sample
(
    id Int64,
    arr Array(Boolean)
) engine = MergeTree order by tuple();

insert into bool_array_sample (id, arr) values
    (0, [true, false, true]),
    (1, [false, false, true]),
    (2, [true, true, false]),
    (3, [false, true, false]),
    (4, []),
    (5, [true]);

select * from bool_array_sample order by id;


create table nullable_string_array
(
    id Int64,
    arr Array(Nullable(String))
) engine = MergeTree order by tuple();

insert into nullable_string_array (id, arr) values
    (0, ['apple', 'banana', null]),
    (1, [null, 'date', 'elderberry']),
    (2, ['fig', null, 'honeydew']),
    (3, [null]),
    (4, []),
    (5, ['lemon', null, 'mango']);


select * from nullable_string_array order by id;


create table empty_sample (
    id Int64,
    empty_arr Array(Int64) default []
) engine = MergeTree order by tuple();


drop table array_of_nested;
create table array_of_nested (
        id Int64,
        arr Array(Nested(child_id UInt64, child_name String))
) engine = MergeTree order by tuple();

INSERT INTO array_of_nested (id, arr) VALUES
      (0, [[(1, 'Alice'), (2, 'Bob')]]),
      (1, [[(3, 'Charlie'), (4, 'Diana')]]),
      (2, [[(5, 'Eve')]]),
      (3, [[]]),
      (4, [[(6, 'Frank'), (7, 'Grace')]]),
      (5, [[(8, 'Heidi')]]);

optimize table array_of_nested;
select * from array_of_nested;


set flatten_nested=0;
drop table simple_nested;
create table simple_nested (
    id Int64,
    nes Nested(child_id UInt64, child_name String)
) engine = MergeTree order by tuple();


INSERT INTO simple_nested (id, nes) VALUES
                                           (0, [(1, 'Alice'),   (2, 'Bob')]),
                                           (1, [(3, 'Charlie'), (4, 'Diana')]),
                                           (2, [(5, 'Eve')]),
                                           (3, []),
                                           (4, [(6, 'Frank'),  (7, 'Grace')]),
                                           (5, [(8, 'Heidi')]);

select * from simple_nested order by id;


create table fixed_string_sample (
    id Int64,
    fs FixedString(16)
) engine = MergeTree order by tuple();

insert into fixed_string_sample (id, fs) values
    (0, 'fixed string 1'),
    (1, 'fixed string 2'),
    (2, 'fixed string 3'),
    (3, 'fixed string 4'),
    (4, 'fixed string 5 q');

select * from fixed_string_sample order by id;


create table fixed_string_array (
    id Int64,
    arr Array(FixedString(16))
) engine = MergeTree order by tuple();

insert into fixed_string_array (id, arr) values
    (0, ['fixed string 1', 'fixed string 2']),
    (1, ['fixed string 3', 'fixed string 4']),
    (2, ['fixed string 5', 'fixed string 6']),
    (3, ['fixed string 7']),
    (4, []),
    (5, ['fixed string 8', 'fixed string 9']);

select * from fixed_string_array order by id;


drop table enums_sample;

create table enums_sample (
    id Int64,
    e8 Enum8('Red' = 11, 'Green' = 2, 'Blue' = -23) default 'Red',
    e16 Enum16('Foo' = 2000, 'Bar' = 200) default 'Foo'
) engine = MergeTree order by tuple();

insert into enums_sample (id, e8, e16) values
    (0, 'Red', 'Foo'),
    (1, 'Green', 'Bar'),
    (2, 'Blue', 'Foo'),
    (3, 'Red', 'Bar'),
    (4, 'Green', 'Foo'),
    (5, 'Blue', 'Bar');

select * from enums_sample order by id;


create table enums_array_sample (
    id Int64,
    arr_e8 Array(Enum8('Red' = 11, 'Green' = 2, 'Blue' = -23)) default ['Red', 'Green'],
    arr_e16 Array(Enum16('Foo' = 2000, 'Bar' = 200)) default ['Foo']
) engine = MergeTree order by tuple();

insert into enums_array_sample (id, arr_e8, arr_e16) values
    (0, ['Red', 'Green'], ['Foo']),
    (1, ['Blue', 'Red'], ['Bar']),
    (2, ['Green'], ['Foo', 'Bar']),
    (3, [], ['Foo']),
    (4, ['Red', 'Blue'], []),
    (5, ['Green', 'Red', 'Blue'], ['Bar']);

select * from enums_array_sample order by id;


create table bfloat16_array_sample (
    id Int64,
    arr_bf16 Array(BFloat16)
) engine = MergeTree order by tuple();

insert into bfloat16_array_sample (id, arr_bf16) values
    (0, [3.14, 2.71, 1.41]),
    (1, [0.57721, 1.61803]),
    (2, [2.23607]),
    (3, []),
    (4, [1.41421, 3.14159]);

select * from bfloat16_array_sample order by id;

select * from bfloat16_array_sample;

select * from plain_strings;


select * from generateRandom('a LowCardinality(String)', 100) limit 10;

create table array_lc_string_empty (
    id Int64,
    arr Array(LowCardinality(String)) default []
) engine = MergeTree order by tuple();

insert into array_lc_string_empty (id, arr) values
    (0, []),
    (1, []),
    (2, []),
    (3, []),
    (4, []);

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

INSERT INTO benchmark_sample
SELECT
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
FROM numbers(1000000);

SELECT
    uniq(lc_string_cd10)               AS cd10,
    uniq(lc_nullable_string_cd1000)    AS cd1000,
    uniq(lc_nullable_string_cd5000)    AS cd5000,
    uniq(lc_nullable_string_cd3000)    AS cd3000,
    uniq(lc_nullable_string_cd4000)    AS cd4000,
    uniq(lc_nullable_string_cd50000)   AS cd50000,
    uniq(lc_nullable_string_cd100)     AS cd100,
    uniq(lc_nullable_string_cd500)     AS cd500,
    uniq(lc_nullable_string_cd_00000)     AS qwe

FROM benchmark_sample;


select * from benchmark_sample;


create table nullable_lc_str (
    id Int64,
    nlc_str LowCardinality(Nullable(String))
) engine = MergeTree order by tuple();

insert into nullable_lc_str (id, nlc_str) values
    (0, 'apple'),
    (1, null),
    (2, 'banana'),
    (3, 'cherry'),
    (4, null),
    (5, 'date');


create table sample_128 (
    id Int64,
    u128_single UInt128,
    u128_array Array(UInt128),

    i128_single Int128,
    i128_array Array(Int128),
) engine = MergeTree order by tuple();

insert into sample_128 (id, u128_single, u128_array, i128_single, i128_array) values
    (
        0,
        toUInt128('12345678901234567890123456789012'),
        [
            toUInt128('12345678901234567890123456789012'),
            toUInt128('98765432109876543210987654321098')
        ],

        toInt128('12345678901234567890123456789012'),
        [
            toInt128('12345678901234567890123456789012'),
            toInt128('-98765432109876543210987654321098')
        ]
    );


SET enable_json_type = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;
drop table if exists variant_arr;
create table variant_arr
(
    id Int64,
    variant Array(Variant(String, UInt64, Array(UInt64), JSON))
) engine = MergeTree order by tuple();

INSERT INTO variant_arr (id, variant)
VALUES (0,
        array(
                CAST('string value', 'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST(toUInt64(12345), 'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST(array(toUInt64(1), toUInt64(2), toUInt64(3)),
                     'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST('{"key":"value"}'::JSON,
                     'Variant(String, UInt64, Array(UInt64), JSON)')
        )),
       (1,
        array(
                CAST('another string', 'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST(toUInt64(1232), 'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST(array(toUInt64(4), toUInt64(5)),
                     'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST('{"array":[6,7]}'::JSON,
                     'Variant(String, UInt64, Array(UInt64), JSON)')
        )),
       (2,
        array(
                CAST('more strings', 'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST(toUInt64(3333), 'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST(CAST(array(), 'Array(UInt64)'),
                     'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST('{"nested":{"a":1}}'::JSON,
                     'Variant(String, UInt64, Array(UInt64), JSON)')
        )),
       (3,
        array(
                CAST('test json', 'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST(toUInt64(44), 'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST(array(toUInt64(8), toUInt64(9)),
                     'Variant(String, UInt64, Array(UInt64), JSON)'),
                CAST('{"boolean":true}'::JSON,
                     'Variant(String, UInt64, Array(UInt64), JSON)')
        ));

