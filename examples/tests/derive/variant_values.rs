use bloch::parse::block::parse_single;
use bloch::reader::{ArrayIter, I64, Variant};
use bloch::{FromBlock, FromVariant};

const _SQL: &str = r#"
set allow_experimental_variant_type = 1;

drop table if exists variant_sample;

create table variant_sample
(
    id  Int64,
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

select * from variant_sample order by id format Native;
"#;

#[derive(FromVariant)]
enum Value<'a> {
    Array(ArrayIter<'a, I64<'a>>),
    Integer(i64),
    String(&'a str),
}
#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    #[col(name = "var")]
    value: Variant<'a, Value<'a>>,
}

#[test]
fn reads_variant_values() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("variant.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected = ["1", "a", "[1, 2, 3]", "2", "b", "[4, 5, 6]", "3"];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let actual = match row.value {
            Value::Array(values) => format!("{:?}", values.try_collect_vec()?),
            Value::Integer(value) => value.to_string(),
            Value::String(value) => value.to_owned(),
        };
        assert_eq!(actual, expected[index]);
    }
    Ok(())
}
