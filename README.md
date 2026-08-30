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

- `serde` means `clickhouse-rs` deserializer from a pinned version I need to update some day
- `chbr` means manual read with column sorting
- `chbr_derive` - derive a struct and use the generated `*Item` column to access
- `chbr_derive_direct` read with try_read.

```bash
cargo bench --bench refs
serde                   time:   [26.363 ms 26.447 ms 26.536 ms]
chbr                    time:   [13.660 ms 13.760 ms 13.868 ms]
chbr_derive             time:   [10.669 ms 10.767 ms 10.877 ms]
chbr_derive_direct      time:   [9.6742 ms 9.7298 ms 9.7872 ms]
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

The standalone examples crate keeps both access styles as executable tests:

- [`examples/tests/procedural`](examples/tests/procedural) drives blocks through `Mark`/`Value`
  accessors directly, without any derive.
- [`examples/tests/derive`](examples/tests/derive) exercises one schema apiece through
  `#[derive(FromBlock)]` (and `#[derive(FromVariant)]` for variant schemas).

```sh
cargo test -p chbr-examples
```

