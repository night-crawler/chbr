use bloch::BStr;
use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

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

#[test]
fn fixed_string_sample() -> TestResult {
    let data = std::fs::read(crate::common::fixture("fixed_string_sample.native"))?;
    let (_, block) = parse_single(&data)?;

    // 0,fixed string 1
    // 1,fixed string 2
    // 2,fixed string 3
    // 3,fixed string 4
    // 4,fixed string 5 q

    let expected = [
        "fixed string 1",
        "fixed string 2",
        "fixed string 3",
        "fixed string 4",
        "fixed string 5 q",
    ];

    let fixed_string_marker = &block.markers[1];
    for (i, expected) in expected.iter().enumerate() {
        let value = fixed_string_marker.get(i)?.unwrap();
        // `&str` trims the zero padding; `&BStr` is the raw 16-byte record.
        let text: &str = value.clone().try_into()?;
        assert_eq!(text, *expected, "Mismatch at index {i}");
        let raw: &BStr = value.try_into()?;
        assert_eq!(raw.len(), 16, "Mismatch at index {i}");
        assert_eq!(
            &raw[..expected.len()],
            expected.as_bytes(),
            "Mismatch at index {i}"
        );
        assert!(
            raw[expected.len()..].iter().all(|byte| *byte == 0),
            "Mismatch at index {i}"
        );
    }

    Ok(())
}
