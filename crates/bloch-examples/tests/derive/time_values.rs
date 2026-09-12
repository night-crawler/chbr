use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{Array, Nullable, Time, Time64};
use chrono::TimeDelta;

const _SQL: &str = r#"
-- Time / Time64 are not available in ClickHouse 25.3 (the setting
-- enable_time_time64_type is unknown there); reproduced on ClickHouse 26.8.2.7.

set enable_time_time64_type = 1;

select
    toTime('12:34:56') as t,
    toTime('-01:02:03') as neg,
    toTime64('12:34:56.789', 3) as t3,
    toTime64('-00:00:01.5', 6) as neg6,
    toTime64('999:59:59.999999999', 9) as t9,
    toTime64('00:00:07', 0) as t0,
    [toTime('00:00:01'), toTime('-00:00:02')] as arr,
    if(number = 0, NULL, toTime64('01:00:00', 3)) as n
from numbers(2) format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    t: Time<'a>,
    neg: Time<'a>,
    t3: Time64<'a>,
    neg6: Time64<'a>,
    t9: Time64<'a>,
    t0: Time64<'a>,
    arr: Array<'a, Time<'a>>,
    n: Nullable<'a, Time64<'a>>,
}

#[test]
fn reads_time_and_time64_as_time_delta() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("time.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected_n = [None, Some(TimeDelta::hours(1))];
    let mut rows = 0;
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.t, TimeDelta::seconds(12 * 3600 + 34 * 60 + 56));
        assert_eq!(row.neg, -TimeDelta::seconds(3600 + 2 * 60 + 3));
        assert_eq!(
            row.t3,
            TimeDelta::seconds(12 * 3600 + 34 * 60 + 56) + TimeDelta::milliseconds(789)
        );
        assert_eq!(row.neg6, -TimeDelta::milliseconds(1500));
        assert_eq!(
            row.t9,
            TimeDelta::seconds(999 * 3600 + 59 * 60 + 59) + TimeDelta::nanoseconds(999_999_999)
        );
        assert_eq!(row.t0, TimeDelta::seconds(7));
        let arr = row.arr.try_collect_vec()?;
        assert_eq!(arr, [TimeDelta::seconds(1), TimeDelta::seconds(-2)]);
        assert_eq!(row.n, expected_n[index]);
        rows += 1;
    }
    assert_eq!(rows, 2);
    Ok(())
}
