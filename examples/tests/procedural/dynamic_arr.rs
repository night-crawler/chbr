use bloch::parse::block::parse_single;
use bloch::reader::JsonIterator;
use bloch::value::DynamicSliceIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

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

#[test]
fn dynamic_arr() -> TestResult {
    let data = std::fs::read(crate::common::fixture("dynamic_arr.native"))?;
    let (_, block) = parse_single(&data)?;

    // │  0 │ [1,2,3]                                       │
    // │  1 │ ['a','b','c']                                 │
    // │  2 │ [true,false,true]                             │
    // │  3 │ [1.23,4.5600000000000005,7.89]                │
    // │  4 │ ['2023-01-01','2023-01-02']                   │
    // │  5 │ ['2023-01-01 12:00:00','2023-01-02 12:00:00'] │
    // │  6 │ ['{"sample":true}']                           │

    let marker = &block.markers[1];

    assert_eq!(block.num_rows, 7, "Expected 7 rows in dynamic_arr");

    {
        let arr: DynamicSliceIterator = marker.get(0)?.unwrap().try_into()?;
        let actual: Vec<i64> = arr
            .map(|value| value.and_then(TryFrom::try_from))
            .collect::<Result<_, _>>()?;
        assert_eq!(actual, [1, 2, 3], "Row 0 mismatch");
    }

    {
        let arr: DynamicSliceIterator = marker.get(1)?.unwrap().try_into()?;
        let actual: Vec<&str> = arr
            .map(|value| value.and_then(TryFrom::try_from))
            .collect::<Result<_, _>>()?;
        assert_eq!(actual, ["a", "b", "c"], "Row 1 mismatch");
    }

    {
        let arr: DynamicSliceIterator = marker.get(2)?.unwrap().try_into()?;
        let actual: Vec<bool> = arr
            .map(|value| value.and_then(TryFrom::try_from))
            .collect::<Result<_, _>>()?;
        assert_eq!(actual, [true, false, true], "Row 2 mismatch");
    }

    {
        let arr: DynamicSliceIterator = marker.get(3)?.unwrap().try_into()?;
        let actual: Vec<f64> = arr
            .map(|value| value.and_then(TryFrom::try_from))
            .collect::<Result<_, _>>()?;
        let expected = [1.23, 4.5600000000000005, 7.89];
        assert_eq!(actual, expected, "Row 3 mismatch");
    }

    {
        let arr: DynamicSliceIterator = marker.get(4)?.unwrap().try_into()?;
        let actual: Vec<chrono::NaiveDate> = arr
            .map(|value| value.and_then(TryFrom::try_from))
            .collect::<Result<_, _>>()?;
        let expected = [
            chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
        ];
        assert_eq!(actual, expected, "Row 4 mismatch");
    }

    {
        let arr: DynamicSliceIterator = marker.get(5)?.unwrap().try_into()?;
        let actual: Vec<chrono::DateTime<chrono_tz::Tz>> = arr
            .map(|value| value.and_then(TryFrom::try_from))
            .collect::<Result<_, _>>()?;
        let expected = [
            chrono::DateTime::parse_from_rfc3339("2023-01-01T12:00:00+00:00")?
                .with_timezone(&chrono_tz::UTC),
            chrono::DateTime::parse_from_rfc3339("2023-01-02T12:00:00+00:00")?
                .with_timezone(&chrono_tz::UTC),
        ];
        assert_eq!(actual, expected, "Row 5 mismatch");
    }

    {
        let mut it: DynamicSliceIterator = marker.get(6)?.unwrap().try_into()?;
        let json_it: JsonIterator = it.next().unwrap()?.try_into()?;

        let mut paths: Vec<&str> = json_it
            .map(|item| item.map(|(path, _)| path))
            .collect::<Result<_, _>>()?;
        paths.sort_unstable();
        assert_eq!(paths, ["sample"], "Row 6 mismatch");
    }

    Ok(())
}
