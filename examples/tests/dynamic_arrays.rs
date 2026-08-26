mod common;

use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::JsonIterator;
use chbr::reader::{Array, I64, Value};

#[derive(FromBlock)]
struct Row<'a> {
    id: I64<'a>,
    arr: Array<'a, Value<'a>>,
}

#[test]
fn reads_dynamic_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(common::fixture("dynamic_arr.native"))?;
    let (_, block) = parse_single(&data)?;
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        let values = row.arr.collect::<chbr::Result<Vec<_>>>()?;
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
