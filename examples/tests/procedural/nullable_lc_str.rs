use bloch::BStr;
use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists nullable_lc_str;

create table nullable_lc_str
(
    id      Int64,
    nlc_str LowCardinality(Nullable(String))
) engine = MergeTree order by tuple();

insert into nullable_lc_str (id, nlc_str) values
    (0, 'apple'),
    (1, null),
    (2, 'banana'),
    (3, 'cherry'),
    (4, null),
    (5, 'date');

select * from nullable_lc_str order by id format Native;
"#;

#[test]
fn nullable_lc_str() -> TestResult {
    let data = std::fs::read(crate::common::fixture("nullable_lc_str.native"))?;
    let (_, block) = parse_single(&data)?;

    //    ┌─id─┬─nlc_str─┐
    // 1. │  0 │ apple   │
    // 2. │  1 │ ᴺᵁᴸᴸ    │
    // 3. │  2 │ banana  │
    // 4. │  3 │ cherry  │
    // 5. │  4 │ ᴺᵁᴸᴸ    │
    // 6. │  5 │ date    │
    //    └────┴─────────┘

    let expected = [
        Some("apple"),
        None,
        Some("banana"),
        Some("cherry"),
        None,
        Some("date"),
    ];

    let nlc_str_marker = &block.markers[1];
    for (i, expected) in expected.iter().enumerate() {
        let value: Option<&str> = nlc_str_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");

        let value = nlc_str_marker.get_opt_str(i)?.unwrap();
        assert_eq!(
            value,
            expected.map(BStr::new),
            "Mismatch at index {i} (get_opt_str)"
        );
    }

    // Out of range is the outer None; NULL (index 1) is the inner None.
    assert_eq!(nlc_str_marker.get_opt_str(expected.len())?, None);
    assert_eq!(nlc_str_marker.get_opt_str(1)?, Some(None));

    Ok(())
}
