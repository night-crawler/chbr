Every `.native` sample here is byte-reproducible from SQL, and the SQL lives next to whoever reads
the sample:

* samples read by the examples (`examples/tests/**`) carry their script in the test file itself, as
  `const _SQL` — `rg 'const _SQL' examples/tests` lists them;
* samples read only by the crate's own tests and benches are kept in [`sql_data.sql`](./sql_data.sql).

Regenerate one by piping its script into a client and capturing stdout:

```shell
clickhouse-client --multiquery < script.sql > testdata/<sample>.native
```

`FORMAT Native` is serialised client side, so the client version matters as much as the server one.
Every sample was captured against ClickHouse 25.3.2.39, except `time.native`, `geometry_sample.native`
and `empty_tuple.native`, which need 26.8 (their scripts say so). No script depends on the server or
session timezone.

`benchmark_sample.native`/`.rb` (1e6 random rows) and `json_wide.native` (captured from a live
server) are the only samples that are not byte-reproducible; see `sql_data.sql` and
`benches/README.md`.

In the future we'll need a proper script that generates the data for different ClickHouse versions.
