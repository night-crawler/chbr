use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{Array, Nullable, Time, Time64};
use chrono::TimeDelta;

#[derive(FromBlock)]
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
