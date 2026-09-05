use std::collections::HashSet;
use std::hint::cold_path;

use log::debug;

use crate::{
    ParsedBlock,
    parse::{
        IResult,
        consts::{MAX_NUM_COLUMNS, MAX_NUM_ROWS},
        parse_var_str, parse_varuint,
    },
    types::Type,
};

#[derive(Debug, Clone)]
pub(crate) struct ParseContext<'a> {
    pub(crate) initial: &'a [u8],

    pub(crate) input: &'a [u8],
    pub(crate) num_columns: usize,
    pub(crate) num_rows: usize,
    pub(crate) col_id: usize,

    pub(crate) column_name: &'a str,
}

impl<'a> ParseContext<'a> {
    pub(crate) const fn fork(&self, input: &'a [u8]) -> ParseContext<'a> {
        ParseContext {
            initial: self.initial,
            input,
            num_columns: self.num_columns,
            num_rows: self.num_rows,
            col_id: self.col_id,
            column_name: self.column_name,
        }
    }

    pub(crate) const fn with_num_rows(self, num_rows: usize) -> ParseContext<'a> {
        ParseContext { num_rows, ..self }
    }
}

pub fn parse_single(input: &[u8]) -> IResult<&[u8], ParsedBlock<'_>> {
    if input.is_empty() {
        return Ok((
            input,
            ParsedBlock {
                markers: Box::new([]),
                col_names: Box::new([]),
                num_rows: 0,
            },
        ));
    }

    let mut parse_context = ParseContext {
        initial: input,
        input,
        num_columns: 0,
        num_rows: 0,
        col_id: 0,
        column_name: "",
    };

    let (input, num_columns) = parse_varuint::<usize>(input)?;
    let (mut input, num_rows) = parse_varuint::<usize>(input)?;

    debug!("num_columns={num_columns} num_rows={num_rows}");

    if num_columns > MAX_NUM_COLUMNS {
        cold_path();
        return Err(crate::parse::Error::CorruptedData(format!(
            "suspiciously many columns in Native block: {num_columns}"
        )));
    }
    if num_rows > MAX_NUM_ROWS {
        cold_path();
        return Err(crate::parse::Error::CorruptedData(format!(
            "suspiciously many rows in Native block: {num_rows}"
        )));
    }

    parse_context.num_columns = num_columns;
    parse_context.num_rows = num_rows;

    let cap = num_columns.min(input.len());
    let mut markers = Vec::with_capacity(cap);
    let mut col_names = Vec::with_capacity(cap);
    let mut seen_names = HashSet::with_capacity(cap);

    for index in 0..num_columns {
        debug!("Parsing column {} of {num_columns}", index + 1);
        parse_context.col_id = index;

        let column_name;
        (input, column_name) = parse_var_str(input)?;
        debug!("column name: {column_name}");
        if !seen_names.insert(column_name) {
            cold_path();
            return Err(crate::parse::Error::CorruptedData(format!(
                "duplicate column name in Native block: {column_name:?}"
            )));
        }
        parse_context.column_name = column_name;
        col_names.push(column_name);

        let column_type;
        (input, column_type) = parse_var_str(input)?;
        debug!("{column_name}: column type: {column_type}");

        let typ = Type::from_bytes(column_type.as_bytes())?;
        debug!("column type parsed: {:?}", typ);

        let ctx = parse_context.fork(input);
        let header;
        (input, header) = typ.decode_header(&ctx)?;
        debug!("Decoded header: `{header:?}` for column `{column_name}`");

        let marker;
        (input, marker) = typ.decode(ctx.fork(input), header)?;
        debug!("Decoded, remaining bytes: {}", input.len());

        markers.push(marker);
    }

    Ok((
        input,
        ParsedBlock {
            markers: markers.into_boxed_slice(),
            col_names: col_names.into_boxed_slice(),
            num_rows,
        },
    ))
}

pub fn parse_many(mut input: &[u8]) -> Result<Vec<ParsedBlock<'_>>, crate::parse::Error> {
    let mut blocks = Vec::new();
    while !input.is_empty() {
        let block;
        (input, block) = parse_single(input)?;
        blocks.push(block);
    }

    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use testresult::TestResult;

    use super::*;
    use crate::common::load;

    fn var_str(out: &mut Vec<u8>, s: &str) {
        out.push(u8::try_from(s.len()).unwrap());
        out.extend_from_slice(s.as_bytes());
    }

    /// Block: 1 column "d" of type Dynamic, 2 rows: [Int64(42), NULL].
    #[test]
    fn dynamic_null_discriminator() -> TestResult {
        let mut b = Vec::new();
        b.push(1); // num_columns
        b.push(2); // num_rows
        var_str(&mut b, "d");
        var_str(&mut b, "Dynamic");
        // -- prefix --
        b.extend_from_slice(&1u64.to_le_bytes()); // structure version V1
        b.push(32); // legacy max_dynamic_types (varint, V1 only)
        b.push(1); // num dynamic types
        var_str(&mut b, "Int64");
        b.extend_from_slice(&0u64.to_le_bytes()); // Variant discriminators mode BASIC
        // -- data --
        // sorted variants: ["Int64", "SharedVariant"] -> Int64 = 0
        b.push(0x00); // row 0 -> Int64
        b.push(0xFF); // row 1 -> NULL_DISCRIMINATOR
        b.extend_from_slice(&42i64.to_le_bytes()); // Int64 variant, 1 row

        let (rest, block) = parse_single(&b)?;
        assert!(rest.is_empty());

        let chbr::mark::Mark::Dynamic(dynamic) = &block.markers[0] else {
            panic!("expected Dynamic mark, got {:?}", block.markers[0]);
        };
        assert!(matches!(
            dynamic.get(0)?,
            Some(chbr::value::Value::Int64(42))
        ));
        assert!(matches!(dynamic.get(1)?, Some(chbr::value::Value::Empty)));
        assert!(dynamic.get(2)?.is_none());
        Ok(())
    }

    #[test]
    fn populated_shared_variant_rejected() -> TestResult {
        for (file, expected) in [
            (
                "./testdata/dynamic_shared_variant.native",
                "Dynamic with 3 values in SharedVariant",
            ),
            (
                "./testdata/json_shared_variant.native",
                "Dynamic with 3 values in SharedVariant",
            ),
        ] {
            let buf = load(file)?;
            match parse_many(&buf) {
                Err(crate::Error::NotImplemented(message)) => assert_eq!(message, expected),
                Err(other) => panic!("{file}: expected NotImplemented, got {other:?}"),
                Ok(_) => panic!("{file}: expected NotImplemented, parsed successfully"),
            }
        }
        Ok(())
    }

    #[test]
    fn zero_row_blocks_have_no_prefixes() -> TestResult {
        for typ in [
            "LowCardinality(String)",
            "Variant(Int64, String)",
            "Dynamic",
            "JSON",
            "Array(LowCardinality(String))",
            "Map(LowCardinality(String), Variant(Int64, String))",
            "Tuple(LowCardinality(String), Dynamic)",
        ] {
            let mut b = Vec::new();
            b.push(1); // num_columns
            b.push(0); // num_rows
            var_str(&mut b, "x");
            var_str(&mut b, typ);

            let (rest, block) = parse_single(&b)
                .unwrap_or_else(|e| panic!("zero-row block of {typ} must parse: {e:?}"));
            assert!(rest.is_empty(), "{typ}: trailing bytes");
            assert_eq!(block.num_rows, 0);
            assert_eq!(*block.col_names, ["x"]);
        }
        Ok(())
    }

    #[test]
    fn duplicate_column_names_rejected() {
        let mut b = vec![2u8, 0]; // 2 columns, 0 rows
        var_str(&mut b, "x");
        var_str(&mut b, "UInt8");
        var_str(&mut b, "x");
        var_str(&mut b, "UInt8");

        match parse_single(&b) {
            Err(crate::Error::CorruptedData(msg)) => {
                assert!(msg.contains("duplicate column name"), "{msg}");
            }
            Err(other) => panic!("expected CorruptedData, got {other:?}"),
            Ok(_) => panic!("duplicate column names must error"),
        }
    }

    #[test]
    fn hostile_counts_rejected_without_allocation() {
        // u64::MAX varint (10 bytes).
        let huge = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];

        let mut b = huge.to_vec();
        b.push(0); // num_rows
        assert!(parse_single(&b).is_err(), "huge column count must error");

        // Row count beyond the cap.
        let mut b = vec![1u8];
        b.extend_from_slice(&huge);
        var_str(&mut b, "x");
        var_str(&mut b, "String");
        assert!(parse_single(&b).is_err(), "huge row count must error");

        // Plausible row count (within caps) but no data behind it: must fail on input length
        let mut b = vec![1u8];
        b.extend_from_slice(&[0xA0, 0x8D, 0x06]); // varint 100_000
        var_str(&mut b, "x");
        var_str(&mut b, "String");
        b.push(0); // one empty string, then EOF
        assert!(parse_single(&b).is_err(), "truncated rows must error");

        // Dynamic type count beyond MAX_DYNAMIC_TYPES (254).
        let mut b = vec![1u8, 1];
        var_str(&mut b, "d");
        var_str(&mut b, "Dynamic");
        b.extend_from_slice(&1u64.to_le_bytes()); // structure version V1
        b.push(32); // legacy max_dynamic_types
        b.extend_from_slice(&huge); // num dynamic types
        match parse_single(&b) {
            Err(err) => assert!(
                err.to_string().contains("too many types"),
                "unexpected error: {err}"
            ),
            Ok(_) => panic!("huge dynamic type count must error"),
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    fn non_monotonic_offsets_rejected() {
        let mut b = vec![1u8, 2]; // 1 column, 2 rows
        var_str(&mut b, "a");
        var_str(&mut b, "Array(UInt8)");
        b.extend_from_slice(&5u64.to_le_bytes());
        b.extend_from_slice(&2u64.to_le_bytes());
        b.extend_from_slice(&[0, 1]); // last offset = 2 -> inner decoded with 2 rows

        match parse_single(&b) {
            Err(err) => assert!(
                err.to_string().contains("offsets not monotonic"),
                "unexpected error: {err}"
            ),
            Ok(_) => panic!("non-monotonic offsets must error"),
        }
    }
}
