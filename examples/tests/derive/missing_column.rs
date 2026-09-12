use bloch::FromBlock;
use bloch::error::Error;
use bloch::parse::block::parse_single;
use bloch::reader::I64;

const _SQL: &str = r#"
-- The authoring table used `id Int64 default rand()`; the ids below are the
-- values the committed fixture was generated with.
drop table if exists array_sample;

create table array_sample
(
    id  Int64,
    arr Array(Int64)
) engine = MergeTree order by tuple();

insert into array_sample (id, arr) values
    (0, []),
    (128969003, [1]),
    (214500519, [1]),
    (301458964, []),
    (475251162, []),
    (1228122092, [1, 2, 3, 4, 5]),
    (1873422981, [1, 2, 3, 4]),
    (2172352370, [1, 2, 3]),
    (2181458171, [1, 2]),
    (2793473513, []),
    (3697287021, [1, 2, 3]);

select * from array_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct MissingColumn<'a> {
    #[col(name = "no_such_column")]
    value: I64<'a>,
}

#[test]
fn reports_missing_column() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("array.native"))?;
    let (_, block) = parse_single(&data)?;
    assert!(
        matches!(MissingColumn::from_block(&block), Err(Error::ColumnNotFound(name)) if name == "no_such_column")
    );
    Ok(())
}
