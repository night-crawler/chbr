use bloch::BStr;
use bloch::error::Error;
use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists fixed_string_binary;

create table fixed_string_binary
(
    id Int64,
    fb FixedString(4)
) engine = MergeTree order by tuple();

insert into fixed_string_binary (id, fb) values
    (0, unhex('01000000')),
    (1, unhex('00000000')),
    (2, unhex('deadbeef')),
    (3, 'ab');

select * from fixed_string_binary order by id format Native;
"#;

#[test]
fn fixed_string_binary() -> TestResult {
    let data = std::fs::read(crate::common::fixture("fixed_string_binary.native"))?;
    let (_, block) = parse_single(&data)?;

    // 0,\x01\0\0\0
    // 1,\0\0\0\0
    // 2,\xde\xad\xbe\xef
    // 3,ab\0\0

    let expected: [&[u8]; 4] = [
        b"\x01\x00\x00\x00",
        b"\x00\x00\x00\x00",
        b"\xde\xad\xbe\xef",
        b"ab\x00\x00",
    ];

    let marker = &block.markers[1];
    for (i, expected) in expected.iter().enumerate() {
        let raw: &BStr = marker.get(i)?.unwrap().try_into()?;
        assert_eq!(raw, *expected, "Mismatch at index {i}");
        assert_eq!(
            marker.get_str(i)?.unwrap(),
            *expected,
            "Mismatch at index {i}"
        );
    }

    // `&str` trims the padding and validates only what is left.
    let text: &str = marker.get(0)?.unwrap().try_into()?;
    assert_eq!(text, "\u{1}");
    let text: &str = marker.get(1)?.unwrap().try_into()?;
    assert_eq!(text, "");
    let text: &str = marker.get(3)?.unwrap().try_into()?;
    assert_eq!(text, "ab");
    let invalid: Result<&str, _> = marker.get(2)?.unwrap().try_into();
    assert!(matches!(invalid, Err(Error::Utf8Decode(_, _))));

    Ok(())
}
