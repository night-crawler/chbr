use bloch::parse::block::parse_single;
use bloch::value::FixedStringSliceIterator;
use pretty_assertions::assert_eq;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists fixed_string_array;

create table fixed_string_array
(
    id  Int64,
    arr Array(FixedString(16))
) engine = MergeTree order by tuple();

insert into fixed_string_array (id, arr) values
    (0, ['fixed string 1', 'fixed string 2']),
    (1, ['fixed string 3', 'fixed string 4']),
    (2, ['fixed string 5', 'fixed string 6']),
    (3, ['fixed string 7']),
    (4, []),
    (5, ['fixed string 8', 'fixed string 9']);

select * from fixed_string_array order by id format Native;
"#;

#[test]
fn fixed_string_array() -> TestResult {
    let data = std::fs::read(crate::common::fixture("fixed_string_array.native"))?;
    let (_, block) = parse_single(&data)?;

    // 0,"['fixed string 1\u0000\u0000', 'fixed string 2\u0000\u0000']"
    // 1,"['fixed string 3\u0000\u0000', 'fixed string 4\u0000\u0000']"
    // 2,"['fixed string 5\u0000\u0000', 'fixed string 6\u0000\u0000']"
    // 3,['fixed string 7\u0000\u0000']
    // 4,[]
    // 5,"['fixed string 8\u0000\u0000', 'fixed string 9\u0000\u0000']"

    let expected: [&[&[u8]]; 6] = [
        &[b"fixed string 1\0\0", b"fixed string 2\0\0"],
        &[b"fixed string 3\0\0", b"fixed string 4\0\0"],
        &[b"fixed string 5\0\0", b"fixed string 6\0\0"],
        &[b"fixed string 7\0\0"],
        &[],
        &[b"fixed string 8\0\0", b"fixed string 9\0\0"],
    ];

    let fixed_string_array_marker = &block.markers[1];
    for (i, expected) in expected.iter().enumerate() {
        let value: FixedStringSliceIterator =
            fixed_string_array_marker.get(i)?.unwrap().try_into()?;
        let actual = value.map(AsRef::as_ref).collect::<Vec<&[u8]>>();
        assert_eq!(actual, *expected, "Mismatch at index {i}");
    }

    Ok(())
}
