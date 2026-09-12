use database::reader::{Array, ArrayIter, I64, TryRead, U8, VariantNullable};
use database::{BlocksIterator, FromBlock, FromVariant, ParsedBlock};

#[derive(database::FromVariant)]
enum Payload<'a> {
    #[col(reader = Array<'a, I64<'a>>)]
    Array(ArrayIter<'a, I64<'a>>),
    Integer(i64),
    Text(&'a str),
}

#[derive(database::FromBlock)]
struct GenericRow<'a, T: FromVariant<'a>> {
    id: I64<'a>,
    var: VariantNullable<'a, T>,
}

// T is a produced value, not a reader: neither Copy nor Clone is required.
impl<'a, T: FromVariant<'a>> Copy for GenericRow<'a, T> {}
impl<'a, T: FromVariant<'a>> Clone for GenericRow<'a, T> {
    fn clone(&self) -> Self {
        *self
    }
}

#[test]
fn renamed_derives_read_noncopy_payloads_with_manual_copy() -> database::Result<()> {
    let data = include_bytes!("../../testdata/variant.native");
    let (_, block) = database::parse::block::parse_single(data)?;
    let rows = GenericRow::<Payload<'_>>::rows(&block)?
        .map(|row| {
            let row = row?;
            let value = match row.var {
                Some(Payload::Array(values)) => format!("{:?}", values.try_collect_vec()?),
                Some(Payload::Integer(value)) => value.to_string(),
                Some(Payload::Text(value)) => value.to_owned(),
                None => "null".to_owned(),
            };
            Ok((row.id, value))
        })
        .collect::<database::Result<Vec<_>>>()?;
    assert_eq!(
        rows,
        [
            (0, "1"),
            (1, "a"),
            (2, "[1, 2, 3]"),
            (3, "2"),
            (4, "b"),
            (5, "[4, 5, 6]"),
            (6, "3")
        ]
        .map(|(id, value)| (id, value.to_owned()))
    );
    Ok(())
}

#[derive(database::FromBlock, Copy, Clone)]
struct RepeatedName<'a> {
    #[col(name = "x")]
    first: U8<'a>,
    #[col(name = "x")]
    again: U8<'a>,
}

#[test]
fn name_lookup_repeats_first_match_but_ordering_consumes_occurrences() -> database::Result<()> {
    // Exercise both the small in-place and large allocated reorder paths.
    for width in [4, 64] {
        let values = (10..10 + width).collect::<Vec<u8>>();
        let mut names = vec!["tail"; usize::from(width)];
        names[..3].copy_from_slice(&["x", "y", "x"]);
        let mut blocks = [ParsedBlock {
            markers: values
                .chunks_exact(1)
                .map(|bytes| database::mark::Mark::UInt8(bytes.try_into().unwrap()))
                .collect(),
            col_names: names.into(),
            num_rows: 1,
        }];
        let row = RepeatedName::from_block(&blocks[0])?.try_read(0)?;
        assert_eq!((row.first, row.again), (10, 10));

        BlocksIterator::new_ordered(&mut blocks, &["x", "x"])?;
        let actual = blocks[0]
            .markers
            .iter()
            .map(|mark| U8::try_from(mark)?.try_read(0))
            .collect::<database::Result<Vec<_>>>()?;
        let mut expected = values.clone();
        expected.swap(1, 2);
        assert_eq!(actual, expected);
        let row = RepeatedName::from_block(&blocks[0])?.try_read(0)?;
        assert_eq!((row.first, row.again), (10, 10));
    }
    Ok(())
}
