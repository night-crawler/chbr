//! Type names as ClickHouse emits them in `Native` block headers.

use bloch::parse::block::parse_single;
use bloch::value::{Time64SliceIterator, TimeSliceIterator, Value};
use chrono::TimeDelta;
use testresult::TestResult;

#[test]
fn time_types_parse() -> TestResult {
    let data = std::fs::read(crate::common::fixture("time.native"))?;
    let (_, block) = parse_single(&data)?;
    assert_eq!(block.num_rows, 2);

    let expected = [
        ("t", "Time", TimeDelta::seconds(12 * 3600 + 34 * 60 + 56)),
        ("neg", "Time", -TimeDelta::seconds(3600 + 2 * 60 + 3)),
        (
            "t3",
            "Time64",
            TimeDelta::seconds(12 * 3600 + 34 * 60 + 56) + TimeDelta::milliseconds(789),
        ),
        ("neg6", "Time64", -TimeDelta::milliseconds(1500)),
        (
            "t9",
            "Time64",
            TimeDelta::seconds(999 * 3600 + 59 * 60 + 59) + TimeDelta::nanoseconds(999_999_999),
        ),
        ("t0", "Time64", TimeDelta::seconds(7)),
    ];
    for (name, mark_name, td) in expected {
        let mark = block.mark(name)?;
        assert_eq!(mark.as_str(), mark_name, "{name}");
        assert_eq!(TimeDelta::try_from(mark.get(1)?.unwrap())?, td, "{name}");
    }

    let arr: TimeSliceIterator = block.mark("arr")?.get(0)?.unwrap().try_into()?;
    assert_eq!(
        arr.collect::<Vec<_>>(),
        [TimeDelta::seconds(1), TimeDelta::seconds(-2)]
    );

    let n = block.mark("n")?;
    assert!(matches!(n.get(0)?, Some(Value::Empty)));
    assert_eq!(
        TimeDelta::try_from(n.get(1)?.unwrap())?,
        TimeDelta::hours(1)
    );
    let t3: Time64SliceIterator = block.mark("t3")?.slice(0..2)?.try_into()?;
    let t3 = t3.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(t3.len(), 2);
    assert_eq!(t3[0], t3[1]);
    Ok(())
}
