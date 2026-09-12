use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, LcNullableStr};

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

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    nlc_str: LcNullableStr<'a>,
}

#[test]
fn reads_nullable_low_cardinality_strings() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("nullable_lc_str.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        Some("apple"),
        None,
        Some("banana"),
        Some("cherry"),
        None,
        Some("date"),
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.nlc_str, expected[index]);
    }
    Ok(())
}
