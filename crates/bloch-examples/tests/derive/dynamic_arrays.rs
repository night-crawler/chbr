use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::JsonIterator;
use bloch::reader::{Array, I64, Value};

const _SQL: &str = r#"
set session_timezone = 'UTC';
set allow_experimental_dynamic_type = 1;
set enable_json_type = 1;

drop table if exists dynamic_arr;

create table dynamic_arr
(
    id  Int64,
    arr Array(Dynamic)
) engine = MergeTree order by tuple();

insert into dynamic_arr (id, arr) values
    (0, [1, 2, 3]),
    (1, ['a', 'b', 'c']),
    (2, [true, false, true]),
    (3, [1.23, 4.56, 7.89]),
    (4, [toDate('2023-01-01'), toDate('2023-01-02')]),
    (5, [toDateTime('2023-01-01 12:00:00'), toDateTime('2023-01-02 12:00:00')]),
    (6, ['{"sample": true}'::JSON]);

select * from dynamic_arr order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Value<'a>>,
}

#[test]
fn reads_dynamic_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("dynamic_arr.native"))?;
    let (_, block) = parse_single(&data)?;
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let values = row.arr.collect::<bloch::Result<Vec<_>>>()?;
        match index {
            0 => assert_eq!(
                values
                    .into_iter()
                    .map(i64::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
                [1, 2, 3]
            ),
            1 => assert_eq!(
                values
                    .into_iter()
                    .map(<&str>::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
                ["a", "b", "c"]
            ),
            2 => assert_eq!(
                values
                    .into_iter()
                    .map(bool::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
                [true, false, true]
            ),
            3 => assert_eq!(
                values
                    .into_iter()
                    .map(f64::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
                [1.23, 4.5600000000000005, 7.89]
            ),
            4 => assert_eq!(
                values
                    .into_iter()
                    .map(chrono::NaiveDate::try_from)
                    .collect::<Result<Vec<_>, _>>()?
                    .len(),
                2
            ),
            5 => assert_eq!(
                values
                    .into_iter()
                    .map(chrono::DateTime::<chrono_tz::Tz>::try_from)
                    .collect::<Result<Vec<_>, _>>()?
                    .len(),
                2
            ),
            6 => {
                let json: JsonIterator = values
                    .into_iter()
                    .next()
                    .expect("one JSON value")
                    .try_into()?;
                assert_eq!(json.count(), 1);
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}
