# chbr (ClickHouseBlockReader)

A zero-copy-ish parser for the ClickHouse `Native` format. It does not talk to ClickHouse itself, so you need to get the
bytes using some existing client, i.e., `clickhouse-client` a dump, whatever.

## Why this exists

Quite often, perf problems come from strings/interning and all the related topics. When you request a ton of data from
CH and need to handle it, you'll likely be allocating for strings at the very least. But CH already returns
LowCardinality strings, and there's no point in allocating them. Non-owning deserializers can help with that, but they
can't help when you want to iterate over the same data multiple times, because, for example, you can't handle everything
in a single pass.

This crate is an attempt to implement a random access iterator over CH blocks.

## Types

It supposedly supports all CH types, supposedly correctly, including stuff like `Dynamic`,
`JSON`, `LowCardinality`, etc.

## Perf

Keep in mind that it still needs the whole block in memory, and, for example, if you use the official `clickhouse-rs`,
you will need to allocate memory for all blocks and only then parse/process it. Since the original `clickhouse-rs`
does not expose the RowBinary reader, I had to hack it to have some sort of apples-to-apples comparison.

```bash
❯ cargo bench --bench refs
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.10s
     Running benches/refs.rs (target/release/deps/refs-ed3c098f1a6966ff)
serde                   time:   [25.949 ms 26.063 ms 26.183 ms]
                        change: [−3.7579% −2.3750% −1.2509%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild

chbr                    time:   [15.681 ms 15.793 ms 15.911 ms]
                        change: [−3.0130% +0.0803% +2.8665%] (p = 0.96 > 0.05)
                        No change in performance detected.
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high mild

chbr_derive             time:   [13.643 ms 13.879 ms 14.149 ms]
                        change: [+3.2850% +5.3937% +7.6390%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 9 outliers among 100 measurements (9.00%)
  5 (5.00%) high mild
  4 (4.00%) high severe

chbr_derive_direct      time:   [11.030 ms 11.171 ms 11.320 ms]
                        change: [−3.6447% −1.6280% +0.3017%] (p = 0.11 > 0.05)
                        No change in performance detected.
Found 3 outliers among 100 measurements (3.00%)
  3 (3.00%) high mild
```

## Quick start

Create a table and populate:

```sh
clickhouse-client --host 127.0.0.1 --port 9000 \
    --database qweqwe --user lol --password wut \
    --multiquery "
CREATE TABLE chbr_example
(
    id      UInt32,
    tags    Array(String),
    attrs   Map(String, String),
    payload Variant(Array(Int64), Int64, String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO chbr_example VALUES
    (1, ['fast', 'cpu'], {'region': 'eu', 'host': 'a1'}, 'hello'),
    (2, [], {'region': 'us'}, 42),
    (3, ['gpu'], {}, [1, 2, 3]);
"
```

Dump the table in `Native` format:

```sh
clickhouse-client --host 127.0.0.1 --port 9001 \
    --database qweqwe --user lol --password wut \
    --query "SELECT * FROM chbr_example ORDER BY id FORMAT Native" \
    > testdata/example.native
```

Parse it:

```rust
use chbr::{BlocksIterator, parse::block::parse_many, value::Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read("testdata/example.native")?;

    // Now we know the schema in blocks.
    let mut blocks = parse_many(&data)?;

    // Reorder columns so we know what to expect during parsing.
    let it = BlocksIterator::new_ordered(&mut blocks, &["id", "tags", "attrs", "payload"])?;

    for row in it {
        // You can be the one implementing a proc macro to avoid doing this
        let [id, tags, attrs, payload] = row.cols() else {
            return Err("unexpected column count".into());
        };
        let i = row.row_index();

        // Parse u32 using accessor method avoiding fat Value creation 
        let id = id.get_u32(i)?.expect("valid row index");

        // Extract a str arr
        let tags: &[&str] = tags.get(i).expect("valid row index").try_into()?;

        // Convenience method for maps
        let mut attrs_vec = vec![];
        if let Some(map) = attrs.get_map::<&str, &str>(i)? {
            for kv in map {
                let (key, value) = kv?;
                attrs_vec.push((key, value));
            }
        }

        // Fat Value TryFrom path for Variant(Array(Int64), Int64, String)
        let payload = match payload.get(i).expect("valid row index") {
            Value::String(s) => format!("string: {s}"),
            Value::Int64(n) => format!("int: {n}"),
            Value::Int64Slice(xs) => {
                let xs = xs.iter().map(|x| x.get()).collect::<Vec<i64>>();
                format!("array: {xs:?}")
            }
            other => format!("unexpected: {other:?}"),
        };

        println!("id={id} tags={tags:?} attrs={attrs_vec:?} payload={payload}");
    }

    Ok(())
}
```

See same example in [`examples/basic.rs`](examples/basic.rs), and `testdata/example.native`.

```sh
cargo run --example basic
```

Output:

```text
id=1 tags=["fast", "cpu"] attrs=[("region", "eu"), ("host", "a1")] payload=string: hello
id=2 tags=[] attrs=[("region", "us")] payload=int: 42
id=3 tags=["gpu"] attrs=[] payload=array: [1, 2, 3]
```

