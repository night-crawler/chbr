use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{FixedStr, I64};

const _SQL: &str = r#"
drop table if exists fixed_string_sample;

create table fixed_string_sample
(
    id Int64,
    fs FixedString(16)
) engine = MergeTree order by tuple();

insert into fixed_string_sample (id, fs) values
    (0, 'fixed string 1'),
    (1, 'fixed string 2'),
    (2, 'fixed string 3'),
    (3, 'fixed string 4'),
    (4, 'fixed string 5 q');

select * from fixed_string_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "fs")]
    value: FixedStr<'a>,
}

#[test]
fn reads_fixed_strings() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("fixed_string_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = [
        "fixed string 1",
        "fixed string 2",
        "fixed string 3",
        "fixed string 4",
        "fixed string 5 q",
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!(row.value, expected[index]);
    }
    Ok(())
}
