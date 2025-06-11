use crate::mark::Mark;
use crate::parse::parse_var_str;
use crate::types::OffsetIndexPair as _;
use crate::value::Value;
use std::ops::Range;

#[derive(Debug)]
pub enum IndexableColumn<'a> {
    Stateless(Mark<'a>),
    Stateful { marker: Mark<'a> },
}

impl<'a> From<Mark<'a>> for IndexableColumn<'a> {
    fn from(marker: Mark<'a>) -> Self {
        if marker.size().is_some() {
            return IndexableColumn::Stateless(marker);
        }
        IndexableColumn::Stateful { marker }
    }
}

impl<'a> Mark<'a> {
    pub fn get(&'a self, index: usize) -> Option<Value<'a>> {
        match self {
            Mark::Empty => None,
            Mark::Bool(is_null) => is_null.get(index).map(|&null| Value::Bool(null == 0)),
            Mark::Int8(bc) => bc.get(index).copied().map(Value::Int8),
            Mark::Int16(bv) => bv.get(index).map(|v| v.get()).map(Value::Int16),
            Mark::Int32(bv) => bv.get(index).map(|v| v.get()).map(Value::Int32),
            Mark::Int64(bv) => bv.get(index).map(|v| v.get()).map(Value::Int64),
            Mark::Int128(bv) => bv.get(index).map(|v| v.get()).map(Value::Int128),
            Mark::Int256(bv) => bv.get(index).copied().map(Value::Int256),
            Mark::UInt8(bv) => bv.get(index).copied().map(Value::UInt8),
            Mark::UInt16(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt16),
            Mark::UInt32(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt32),
            Mark::UInt64(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt64),
            Mark::UInt128(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt128),
            Mark::UInt256(bv) => bv.get(index).copied().map(Value::UInt256),
            Mark::Float32(bv) => bv.get(index).map(|v| v.get()).map(Value::Float32),
            Mark::Float64(bv) => bv.get(index).map(|v| v.get()).map(Value::Float64),
            Mark::BFloat16(bv) => {
                let value = *bv.get(index)?;
                let value = half::bf16::from_le_bytes(value);
                Some(Value::BFloat16(value))
            }
            Mark::Decimal32(precision, data) => {
                let value = data.get(index)?.get();
                let value = rust_decimal::Decimal::new(i64::from(value), u32::from(*precision));
                Some(Value::Decimal32(value))
            }
            Mark::Decimal64(precision, data) => {
                let value = data.get(index)?.get();
                let value = rust_decimal::Decimal::new(value, u32::from(*precision));
                Some(Value::Decimal32(value))
            }
            Mark::Decimal128(precision, data) => {
                let value = data.get(index)?.get();
                let value =
                    rust_decimal::Decimal::try_from_i128_with_scale(value, u32::from(*precision))
                        .unwrap();
                Some(Value::Decimal128(value))
            }
            Mark::Decimal256(_, _) => todo!(),
            Mark::String(offsets, buf) => {
                let start = if index == 0 {
                    0
                } else {
                    offsets.get(index.saturating_sub(1)).copied()?
                };

                let end = offsets.get(index).copied().unwrap_or(buf.len());
                let slice = &buf[start..end];

                let (_, s) = parse_var_str(slice).unwrap();

                Some(Value::String(s))
            }
            Mark::FixedString(_, _) => todo!(),
            Mark::Uuid(bv) => {
                let value = bv.get(index)?;
                Some(Value::Uuid(uuid::Uuid::from(*value)))
            }
            Mark::Date(bv) => {
                let value = *bv.get(index)?;
                Some(Value::Date(value.into()))
            }
            Mark::Date32(bv) => {
                let value = *bv.get(index)?;
                Some(Value::Date32(value.into()))
            }
            Mark::DateTime { tz, data } => {
                let value = data.get(index)?.with_tz(*tz);
                Some(Value::DateTime(value))
            }
            Mark::DateTime64 {
                precision,
                tz,
                data,
            } => {
                let value = data.get(index)?.with_tz_and_precision(*tz, *precision)?;
                Some(Value::DateTime64(value))
            }
            Mark::Ipv4(data) => {
                let value = *data.get(index)?;
                Some(Value::Ipv4(value.into()))
            }
            Mark::Ipv6(data) => {
                let value = *data.get(index)?;
                Some(Value::Ipv6(value.into()))
            }
            Mark::Point(_) => unreachable!("Point should be covered by Tuple(f64, f64)"),
            Mark::Ring(_)
            | Mark::Polygon(_)
            | Mark::MultiPolygon(_)
            | Mark::LineString(_)
            | Mark::MultiLineString(_) => {
                unreachable!("Geometric types should be covered by arrays)")
            }
            Mark::Enum8(_, _) => todo!(),
            Mark::Enum16(_, _) => todo!(),
            Mark::LowCardinality {
                indices,
                // https://github.com/ClickHouse/clickhouse-go/blob/main/lib/column/lowcardinality.go#L191
                global_dictionary: _unused,
                additional_keys,
            } => {
                let value_index: usize = indices.get(index)?.try_into().unwrap();
                let Some(keys) = additional_keys else {
                    return None;
                };

                Some(keys.get(value_index)?)
            }
            Mark::Array(offsets, marker) => {
                let (start, end) = offsets.offset_indices(index).unwrap()?;
                Some(marker.slice(start..end))
            }

            Mark::Tuple(inner) => Some(Value::Tuple(index, inner)),
            Mark::Nullable(is_null, data) => {
                if is_null.get(index) == Some(&1) {
                    return Some(Value::Empty);
                }

                data.get(index)
            }
            Mark::Map {
                offsets,
                keys,
                values,
            } => Some(Value::Map {
                offsets,
                keys,
                values,
                index,
            }),
            Mark::Variant {
                offsets,
                discriminators,
                types,
            } => {
                let discriminator = (*discriminators.get(index)?) as usize;
                let in_type_index = *offsets.get(index)?;
                types[discriminator].get(in_type_index)
            }
            Mark::Nested(_, _) => todo!(),
            Mark::Dynamic(_, _) => todo!(),
            Mark::Json { .. } => todo!(),
        }
    }

    pub fn slice(&'a self, idx: Range<usize>) -> Value<'a> {
        match self {
            Mark::Empty => {
                if !idx.is_empty() {
                    panic!("Index out of bounds for empty marker");
                }
                Value::Empty
            }
            Mark::Bool(bv) => Value::BoolSlice(&bv[idx]),
            Mark::Int8(bv) => Value::Int8Slice(&bv[idx]),
            Mark::Int16(bv) => Value::Int16Slice(&bv[idx]),
            Mark::Int32(bv) => Value::Int32Slice(&bv[idx]),
            Mark::Int64(bv) => Value::Int64Slice(&bv[idx]),
            Mark::Int128(bv) => Value::Int128Slice(&bv[idx]),
            Mark::Int256(bv) => Value::Int256Slice(&bv[idx]),
            Mark::UInt8(bv) => Value::UInt8Slice(&bv[idx]),
            Mark::UInt16(bv) => Value::UInt16Slice(&bv[idx]),
            Mark::UInt32(bv) => Value::UInt32Slice(&bv[idx]),
            Mark::UInt64(bv) => Value::UInt64Slice(&bv[idx]),
            Mark::UInt128(bv) => Value::UInt128Slice(&bv[idx]),
            Mark::UInt256(bv) => Value::UInt256Slice(&bv[idx]),
            Mark::Float32(bv) => Value::Float32Slice(&bv[idx]),
            Mark::Float64(bv) => Value::Float64Slice(&bv[idx]),
            Mark::BFloat16(_) => todo!(),
            Mark::Decimal32(_, _) => todo!(),
            Mark::Decimal64(_, _) => todo!(),
            Mark::Decimal128(_, _) => todo!(),
            Mark::Decimal256(_, _) => todo!(),
            Mark::String(offsets, data) => {
                let count = idx.len();
                let Range { start, .. } = idx;

                let data_start = if start == 0 {
                    data
                } else {
                    &data[offsets[start - 1]..]
                };

                Value::StringSlice(count, data_start)
            }
            Mark::FixedString(_, _) => todo!(),
            Mark::Uuid(bv) => Value::UuidSlice(&bv[idx]),
            Mark::Date(bv) => Value::Date16Slice(&bv[idx]),
            Mark::Date32(bv) => Value::Date32Slice(&bv[idx]),
            Mark::DateTime { tz, data } => Value::DateTime32Slice {
                tz: *tz,
                slice: &data[idx],
            },
            Mark::DateTime64 { precision, tz, data } => Value::DateTime64Slice {
                precision: *precision,
                tz: *tz,
                slice: &data[idx],
            },
            Mark::Ipv4(bv) => Value::Ipv4Slice(&bv[idx]),
            Mark::Ipv6(bv) => Value::Ipv6Slice(&bv[idx]),
            Mark::Point(_)
            | Mark::Ring(_)
            | Mark::Polygon(_)
            | Mark::MultiPolygon(_)
            | Mark::LineString(_)
            | Mark::MultiLineString(_) => unreachable!("must be covered by array marker already"),
            Mark::Enum8(_, _) => todo!(),
            Mark::Enum16(_, _) => todo!(),
            Mark::LowCardinality {
                indices,
                global_dictionary: _unused,
                additional_keys,
            } => {
                let Some(additional_keys) = additional_keys else {
                    panic!("LowCardinality marker without additional keys");
                };
                let sliced = indices.slice(idx);
                Value::LowCardinalitySlice {
                    indices: sliced.into(),
                    additional_keys,
                }
            }
            Mark::Array(offsets, data) => Value::ArraySlice {
                offsets,
                data,
                slice_indices: idx,
            },
            Mark::Tuple(inner) => Value::TupleSlice {
                inner,
                slice_indices: idx,
            },
            Mark::Nullable(_, _) => todo!(),
            Mark::Map {
                offsets,
                keys,
                values,
            } => Value::MapSlice {
                offsets,
                keys,
                values,
                slice_indices: idx,
            },
            Mark::Variant { .. } => todo!(),
            Mark::Nested(_, _) => todo!(),
            Mark::Dynamic(_, _) => todo!(),
            Mark::Json { .. } => todo!(),
        }
    }
}

// TODO: ditch the struct if we can use Mark directly and everything is stateless
impl<'a> IndexableColumn<'a> {
    pub fn get(&'a self, index: usize) -> Option<Value<'a>> {
        match self {
            IndexableColumn::Stateless(m) => m.get(index),
            IndexableColumn::Stateful { marker } => match marker {
                Mark::Array(_, _)
                | Mark::String(_, _)
                | Mark::LowCardinality { .. }
                | Mark::Tuple(_)
                | Mark::Map { .. }
                | Mark::Variant { .. }
                | Mark::Nullable(_, _) => marker.get(index),
                _ => todo!(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::load;
    use crate::parse::block::parse_block;
    use crate::value::{
        ArraySliceIterator, BoolSliceIterator, LowCardinalitySliceIterator, MapIterator,
        MapSliceIterator, StringSliceIterator, TupleSliceIterator, Value,
    };
    use half::bf16;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use std::str::FromStr as _;
    use testresult::TestResult;
    use zerocopy::little_endian::I64;

    #[test]
    fn int_array() -> TestResult {
        let buf = load("./test_data/array.native")?;
        // random() for id was a bad idea, it looks like parser is broken
        // 0,[]
        // 128969003,[1]
        // 214500519,[1]
        // 301458964,[]
        // 475251162,[]
        // 1228122092,"[1, 2, 3, 4, 5]"
        // 1873422981,"[1, 2, 3, 4]"
        // 2172352370,"[1, 2, 3]"
        // 2181458171,"[1, 2]"
        // 2793473513,[]
        // 3697287021,"[1, 2, 3]"

        let (_, block) = parse_block(&buf)?;

        let index_marker = &block.cols[0];

        let indices = (0..block.num_rows)
            .filter_map(|i| index_marker.get(i))
            .map(|v| i64::try_from(v).unwrap())
            .collect::<Vec<_>>();

        let expected_ids = [
            0, 128969003, 214500519, 301458964, 475251162, 1228122092, 1873422981, 2172352370,
            2181458171, 2793473513, 3697287021,
        ];

        assert_eq!(indices, expected_ids);

        let expected_arrays = [
            vec![],
            vec![1],
            vec![1],
            vec![],
            vec![],
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 3, 4],
            vec![1, 2, 3],
            vec![1, 2],
            vec![],
            vec![1, 2, 3],
        ];

        let arr_marker = &block.cols[1];

        let mut arrays = Vec::new();
        for index in 0..block.num_rows {
            let v: &[I64] = arr_marker.get(index).unwrap().try_into()?;
            arrays.push(v);
        }

        assert_eq!(arrays, expected_arrays);

        Ok(())
    }

    #[test]
    fn plain_strings() -> TestResult {
        let buf = load("./test_data/plain_strings.native")?;
        // 0,hello
        // 1,world
        // 2,clickhouse
        // 3,test
        // 4,example
        // 5,data

        let (_, block) = parse_block(&buf)?;

        let expected_strings = ["hello", "world", "clickhouse", "test", "example", "data"];

        let strings_marker = &block.cols[1];
        for (i, expected) in expected_strings.iter().enumerate() {
            let value: &str = strings_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn plain_strings_array() -> TestResult {
        let buf = load("./test_data/plain_strings_array.native")?;

        // 0,"['apple', 'banana', 'cherry']"
        // 1,"['date', 'elderberry']"
        // 2,"['fig', 'grape', 'honeydew']"
        // 3,['kiwi']
        // 4,[]
        // 5,"['lemon', 'mango']"

        let (_, block) = parse_block(&buf)?;

        let expected_arrays = [
            vec!["apple", "banana", "cherry"],
            vec!["date", "elderberry"],
            vec!["fig", "grape", "honeydew"],
            vec!["kiwi"],
            vec![],
            vec!["lemon", "mango"],
        ];

        let strings_marker = &block.cols[1];

        for (i, expected) in expected_arrays.iter().enumerate() {
            let it: StringSliceIterator = strings_marker.get(i).unwrap().try_into()?;
            let actual = it.collect::<Vec<_>>();

            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn lc_string() -> TestResult {
        let buf = load("./test_data/plain_lc_string.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,apple
        // 1,banana
        // 2,cherry
        // 3,date
        // 4,elderberry
        // 5,fig

        let expected_strings = ["apple", "banana", "cherry", "date", "elderberry", "fig"];

        let strings_marker = &block.cols[1];
        for (i, expected) in expected_strings.iter().enumerate() {
            let value: &str = strings_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn lc_array_string() -> TestResult {
        let buf = load("./test_data/array_lc_string.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,"['apple', 'banana', 'cherry']"
        // 1,"['date', 'elderberry']"
        // 2,"['fig', 'grape', 'honeydew']"
        // 3,['kiwi']
        // 4,[]
        // 5,"['lemon', 'mango']"
        // 6,"['apple', 'banana', 'cherry', 'date']"
        // 7,"['elderberry', 'fig', 'grape']"
        // 8,"['honeydew', 'kiwi', 'lemon']"
        // 9,"['mango', 'apple', 'banana']"
        // 10,"['cherry', 'date', 'elderberry']"
        // 11,"['fig', 'grape', 'honeydew', 'kiwi']"

        let expected_arrays = [
            vec!["apple", "banana", "cherry"],
            vec!["date", "elderberry"],
            vec!["fig", "grape", "honeydew"],
            vec!["kiwi"],
            vec![],
            vec!["lemon", "mango"],
            vec!["apple", "banana", "cherry", "date"],
            vec!["elderberry", "fig", "grape"],
            vec!["honeydew", "kiwi", "lemon"],
            vec!["mango", "apple", "banana"],
            vec!["cherry", "date", "elderberry"],
            vec!["fig", "grape", "honeydew", "kiwi"],
        ];

        let strings_marker = &block.cols[1];
        for (i, expected) in expected_arrays.iter().enumerate() {
            let it: LowCardinalitySliceIterator = strings_marker.get(i).unwrap().try_into()?;
            let mut actual = vec![];
            for value in it {
                let value: &str = value.try_into()?;
                actual.push(value);
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn array_in_array_in64() -> TestResult {
        let buf = load("./test_data/array_in_array_in64.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,"[[11, 22, 22, 77, 123], [333, 41]]"
        // 1,"[[11, 22], [7, 844, 12, 12, 0], [5, 5, 5]]"
        // 2,"[[9], [10, 11]]"
        // 3,"[[123, 134], [145]]"
        // 4,[[156]]
        // 5,[[]]

        let expected_arrays = [
            vec![vec![11, 22, 22, 77, 123], vec![333, 41]],
            vec![vec![11, 22], vec![7, 844, 12, 12, 0], vec![5, 5, 5]],
            vec![vec![9], vec![10, 11]],
            vec![vec![123, 134], vec![145]],
            vec![vec![156]],
            vec![vec![]],
        ];

        let arrs_marker = &block.cols[1];

        for (i, expected) in expected_arrays.iter().enumerate() {
            let v = arrs_marker.get(i).unwrap();
            let outer: ArraySliceIterator = v.try_into()?;
            let mut actual_outer = vec![];
            for slice in outer {
                let slice: &[I64] = slice.try_into()?;
                let inner = slice.iter().map(|&v| v.get()).collect::<Vec<_>>();
                actual_outer.push(inner);
            }

            assert_eq!(actual_outer, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn nullable_string() -> TestResult {
        let buf = load("./test_data/nullable_string.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,hello
        // 1,
        // 2,world
        // 3,clickhouse
        // 4,
        // 5,test
        let expected_col = [
            Some("hello"),
            None,
            Some("world"),
            Some("clickhouse"),
            None,
            Some("test"),
        ];

        let strings_marker = &block.cols[1];
        for (i, expected) in expected_col.iter().enumerate() {
            let value: Option<&str> = strings_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn tuple_sample() -> TestResult {
        let buf = load("./test_data/tuple.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,"(1, 'a')"
        // 1,"(3, 'ab')"
        // 2,"(7, 'ac')"
        // 3,"(9, 'ad')"
        // 4,"(11, 'ae')"
        // 5,"(2, 'af')"
        // 6,"(3, 'ag')"

        let expected_tuples = [
            (1, "a"),
            (3, "ab"),
            (7, "ac"),
            (9, "ad"),
            (11, "ae"),
            (2, "af"),
            (3, "ag"),
        ];

        let tuples_marker = &block.cols[1];

        for (i, expected) in expected_tuples.iter().enumerate() {
            let value: (i64, &str) = tuples_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn map_sample() -> TestResult {
        let buf = load("./test_data/map_sample.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,"{'a': 'apple', 'b': 'banana', 'c': 'cherry'}"
        // 1,"{'d': 'date', 'e': 'elderberry'}"
        // 2,"{'f': 'fig', 'g': 'grape', 'h': 'honeydew'}"
        // 3,{'i': 'kiwi'}
        // 4,{}
        // 5,"{'j': 'lemon', 'k': 'mango'}"

        let expected = [
            HashMap::from([("a", "apple"), ("b", "banana"), ("c", "cherry")]),
            HashMap::from([("d", "date"), ("e", "elderberry")]),
            HashMap::from([("f", "fig"), ("g", "grape"), ("h", "honeydew")]),
            HashMap::from([("i", "kiwi")]),
            HashMap::new(),
            HashMap::from([("j", "lemon"), ("k", "mango")]),
        ];

        let map_marker = &block.cols[1];
        for (i, expected) in expected.iter().enumerate() {
            let map_value = map_marker.get(i).unwrap();
            let map_iter: MapIterator<&str, &str> = map_value.try_into()?;
            let map = map_iter.flatten().collect::<HashMap<&str, &str>>();
            assert_eq!(map, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn array_map_sample() -> TestResult {
        let buf = load("./test_data/array_map_sample.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,"[{'a': 'apple', 'b': 'banana'}, {'c': 'cherry'}]"
        // 1,"[{'d': 'date'}, {'e': 'elderberry', 'f': 'fig'}]"
        // 2,"[{'g': 'grape', 'h': 'honeydew'}]"
        // 3,[{'i': 'kiwi'}]
        // 4,[]
        // 5,"[{'j': 'lemon', 'k': 'mango'}]"

        let expected = [
            vec![
                HashMap::from([("a", "apple"), ("b", "banana")]),
                HashMap::from([("c", "cherry")]),
            ],
            vec![
                HashMap::from([("d", "date")]),
                HashMap::from([("e", "elderberry"), ("f", "fig")]),
            ],
            vec![HashMap::from([("g", "grape"), ("h", "honeydew")])],
            vec![HashMap::from([("i", "kiwi")])],
            vec![],
            vec![HashMap::from([("j", "lemon"), ("k", "mango")])],
        ];

        let map_marker = &block.cols[1];
        for (i, expected) in expected.iter().enumerate() {
            let map_slice_iterator: MapSliceIterator<&str, &str> =
                map_marker.get(i).unwrap().try_into()?;
            let mut actual = vec![];

            for map in map_slice_iterator.flatten() {
                let map = map.flatten().collect::<HashMap<&str, &str>>();
                actual.push(map);
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn map_in_map() -> TestResult {
        let buf = load("./test_data/map_in_map.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,"{'a': {'x': 'apple', 'y': 'banana'}, 'b': {'z': 'cherry'}}"
        // 1,{'c': {'d': 'date'}}
        // 2,"{'e': {'g': 'elderberry'}, 'f': {'h': 'fig', 'i': 'grape'}}"
        // 3,{'j': {'k': 'kiwi'}}
        // 4,{}
        // 5,"{'l': {'n': 'lemon'}, 'm': {'o': 'mango', 'p': 'nectarine'}}"

        let expected = [
            HashMap::from([
                ("a", HashMap::from([("x", "apple"), ("y", "banana")])),
                ("b", HashMap::from([("z", "cherry")])),
            ]),
            HashMap::from([("c", HashMap::from([("d", "date")]))]),
            HashMap::from([
                ("e", HashMap::from([("g", "elderberry")])),
                ("f", HashMap::from([("h", "fig"), ("i", "grape")])),
            ]),
            HashMap::from([("j", HashMap::from([("k", "kiwi")]))]),
            HashMap::new(),
            HashMap::from([
                ("l", HashMap::from([("n", "lemon")])),
                ("m", HashMap::from([("o", "mango"), ("p", "nectarine")])),
            ]),
        ];

        let map_marker = &block.cols[1];

        for (i, expected) in expected.iter().enumerate() {
            let map_value = map_marker.get(i).unwrap();
            let map_iter: MapIterator<&str, MapIterator<&str, &str>> = map_value.try_into()?;

            let mut actual = HashMap::new();

            for (map_key, map_value) in map_iter.flatten() {
                let inner_map = map_value.flatten().collect::<HashMap<&str, &str>>();
                actual.insert(map_key, inner_map);
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn array_of_tuples() -> TestResult {
        let buf = load("./test_data/array_of_tuples.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,"[('apple', 1), ('banana', 2), ('cherry', 3)]"
        // 1,"[('date', 4), ('elderberry', 5)]"
        // 2,"[('fig', 6), ('grape', 7), ('honeydew', 8)]"
        // 3,"[('kiwi', 9)]"
        // 4,[]
        // 5,"[('lemon', 10), ('mango', 11)]"

        let expected_arrays = [
            vec![("apple", 1), ("banana", 2), ("cherry", 3)],
            vec![("date", 4), ("elderberry", 5)],
            vec![("fig", 6), ("grape", 7), ("honeydew", 8)],
            vec![("kiwi", 9)],
            vec![],
            vec![("lemon", 10), ("mango", 11)],
        ];

        let tuples_marker = &block.cols[1];

        for (i, expected) in expected_arrays.iter().enumerate() {
            let slice: TupleSliceIterator = tuples_marker.get(i).unwrap().try_into()?;
            let mut actual = vec![];
            for tup in slice {
                let (s, n): (&str, i64) = tup.try_into()?;
                actual.push((s, n));
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn variant() -> TestResult {
        let buf = load("./test_data/variant.native")?;
        let (_, block) = parse_block(&buf)?;
        // Variant(Array(Int64), Int64, String)
        //    ┌─id─┬─var─────┐
        // 1. │  0 │ 1       │
        // 2. │  1 │ a       │
        // 3. │  2 │ [1,2,3] │
        // 4. │  3 │ 2       │
        // 5. │  4 │ b       │
        // 6. │  5 │ [4,5,6] │
        // 7. │  6 │ 3       │
        //    └────┴─────────┘

        let variant_marker = &block.cols[1];
        // it's hard to write a test for this because Value does not implement equals yet

        let expected_str_repr = ["1", "a", "1, 2, 3", "2", "b", "4, 5, 6", "3"];

        for (i, expected) in expected_str_repr.iter().enumerate() {
            let value = variant_marker.get(i).unwrap();
            if let Ok(value) = <Value<'_> as TryInto<i64>>::try_into(value.clone()) {
                assert_eq!(format!("{value}"), *expected, "Mismatch at index {i}");
                continue;
            }

            if let Ok(value) = <Value<'_> as TryInto<&str>>::try_into(value.clone()) {
                assert_eq!(value, *expected, "Mismatch at index {i}");
                continue;
            }

            if let Ok(value) = <Value<'_> as TryInto<&[I64]>>::try_into(value.clone()) {
                let parts = value
                    .iter()
                    .map(|v| format!("{}", v.get()))
                    .collect::<Vec<_>>()
                    .join(", ");
                assert_eq!(parts, *expected, "Mismatch at index {i}");
                continue;
            }

            panic!("Unexpected value type at index {i}: {:?}", value);
        }

        Ok(())
    }

    #[test]
    fn uuid_and_dates() -> TestResult {
        let buf = load("./test_data/uuid_and_dates.native")?;
        let (_, block) = parse_block(&buf)?;
        // UUID, Date, Date32, DateTime, DateTime64
        // 00000000-0000-0000-0000-000000000001,2023-01-01,2023-01-01,2023-01-01 12:00:00,2023-01-01T12:00:00.123Z
        // 00000000-0000-0000-0000-000000000002,2023-02-01,2023-02-01,2023-02-01 12:00:00,2023-02-01T12:00:00.456Z
        // 00000000-0000-0000-0000-000000000003,2023-03-01,2023-03-01,2023-03-01 12:00:00,2023-03-01T12:00:00.789Z
        // 00000000-0000-0000-0000-000000000004,2023-03-01,1969-09-23,2023-03-01 12:00:00,2023-03-01T12:00:00.789Z

        let uuid_marker = &block.cols[0];
        let expected_uuids = [
            uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001")?,
            uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000002")?,
            uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000003")?,
            uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000004")?,
        ];
        for (i, expected) in expected_uuids.iter().enumerate() {
            let value: uuid::Uuid = uuid_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let date_marker = &block.cols[1];
        let expected_dates = [
            chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2023, 2, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
        ];
        for (i, expected) in expected_dates.iter().enumerate() {
            let value: chrono::NaiveDate = date_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let date32_marker = &block.cols[2];
        let expected_date32 = [
            chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2023, 2, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2023, 3, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(1969, 9, 23).unwrap(),
        ];
        for (i, expected) in expected_date32.iter().enumerate() {
            let value: chrono::NaiveDate = date32_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let datetime_marker = &block.cols[3];
        let expected_datetimes = [
            chrono::DateTime::parse_from_rfc3339("2023-01-01T12:00:00+00:00")?
                .with_timezone(&chrono_tz::UTC),
            chrono::DateTime::parse_from_rfc3339("2023-02-01T12:00:00+00:00")?
                .with_timezone(&chrono_tz::UTC),
            chrono::DateTime::parse_from_rfc3339("2023-03-01T12:00:00+00:00")?
                .with_timezone(&chrono_tz::UTC),
            chrono::DateTime::parse_from_rfc3339("2023-03-01T12:00:00+00:00")?
                .with_timezone(&chrono_tz::UTC),
        ];
        for (i, expected) in expected_datetimes.iter().enumerate() {
            let value: chrono::DateTime<chrono_tz::Tz> =
                datetime_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let datetime64_marker = &block.cols[4];
        let expected_datetime64 = [
            chrono::DateTime::parse_from_rfc3339("2023-01-01T12:00:00.123+00:00")?
                .with_timezone(&chrono_tz::UTC),
            chrono::DateTime::parse_from_rfc3339("2023-02-01T12:00:00.456+00:00")?
                .with_timezone(&chrono_tz::UTC),
            chrono::DateTime::parse_from_rfc3339("2023-03-01T12:00:00.789+00:00")?
                .with_timezone(&chrono_tz::UTC),
            chrono::DateTime::parse_from_rfc3339("2023-03-01T12:00:00.789+00:00")?
                .with_timezone(&chrono_tz::UTC),
        ];

        for (i, expected) in expected_datetime64.iter().enumerate() {
            let value: chrono::DateTime<chrono_tz::Tz> =
                datetime64_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn decimal_sample() -> TestResult {
        let buf = load("./test_data/decimal_sample.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,1.234,1.234567,1.234567890123,1.234567890123456556104338
        // 1,2.345,2.345678,2.345678901234,2.345678901234567875661641
        // 2,3.456,3.456789,3.456789012345,3.456789012345678555325440

        let expected_d32 = [
            rust_decimal::Decimal::new(1234, 3),
            rust_decimal::Decimal::new(2345, 3),
            rust_decimal::Decimal::new(3456, 3),
        ];
        let decimal32_marker = &block.cols[1];
        for (i, expected) in expected_d32.iter().enumerate() {
            let value: rust_decimal::Decimal = decimal32_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let expected_d64 = [
            rust_decimal::Decimal::new(1234567, 6),
            rust_decimal::Decimal::new(2345678, 6),
            rust_decimal::Decimal::new(3456789, 6),
        ];

        let decimal64_marker = &block.cols[2];
        for (i, expected) in expected_d64.iter().enumerate() {
            let value: rust_decimal::Decimal = decimal64_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let expected_d128 = [
            rust_decimal::Decimal::new(1234567890123, 12),
            rust_decimal::Decimal::new(2345678901234, 12),
            rust_decimal::Decimal::new(3456789012345, 12),
        ];
        let decimal128_marker = &block.cols[3];
        for (i, expected) in expected_d128.iter().enumerate() {
            let value: rust_decimal::Decimal = decimal128_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        // expect panic for decimal256, it's not implemented

        Ok(())
    }

    #[test]
    fn ip_sample() -> TestResult {
        let buf = load("./test_data/ip_sample.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,100.64.0.2,2001:db8:0:0:0:ff00:42:8329
        // 1,127.0.0.1,0:0:0:0:0:0:0:1
        // 2,10.10.10.10,2001:db8:85a3:0:0:8a2e:370:7334

        let ipv4_marker = &block.cols[1];
        let expected_ipv4 = [
            std::net::Ipv4Addr::new(100, 64, 0, 2),
            std::net::Ipv4Addr::new(127, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 10, 10, 10),
        ];

        for (i, expected) in expected_ipv4.iter().enumerate() {
            let value: std::net::Ipv4Addr = ipv4_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let ipv6_marker = &block.cols[2];
        let expected_ipv6 = [
            std::net::Ipv6Addr::from_str("2001:db8:0:0:0:ff00:42:8329")?,
            std::net::Ipv6Addr::from_str("0:0:0:0:0:0:0:1")?,
            std::net::Ipv6Addr::from_str("2001:db8:85a3:0:0:8a2e:370:7334")?,
        ];
        for (i, expected) in expected_ipv6.iter().enumerate() {
            let value: std::net::Ipv6Addr = ipv6_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn geo_sample() -> TestResult {
        let buf = load("./test_data/geo_sample.native")?;
        let (_, block) = parse_block(&buf)?;

        let expected_points = [(10.0, 10.0), (5.0, 5.0), (0.0, 0.0), (100.0, 100.0)];
        let points_marker = &block.cols[1];
        for (i, expected) in expected_points.iter().enumerate() {
            let value: (f64, f64) = points_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Point mismatch at index {i}");
        }

        let expected_rings: [Vec<(f64, f64)>; 4] = [
            vec![(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)],
            vec![(0.0, 0.0), (10.0, 0.0), (5.0, 8.0)],
            vec![(0.0, 0.0), (3.0, 0.0), (3.0, 3.0), (0.0, 3.0)],
            vec![
                (100.0, 100.0),
                (110.0, 100.0),
                (110.0, 110.0),
                (100.0, 110.0),
            ],
        ];
        let rings_marker = &block.cols[2];
        for (i, expected) in expected_rings.iter().enumerate() {
            let value: TupleSliceIterator = rings_marker.get(i).unwrap().try_into()?;
            let mut actual = Vec::with_capacity(expected.len());
            for point in value {
                let (x, y): (f64, f64) = point.try_into()?;
                actual.push((x, y));
            }
            assert_eq!(actual, *expected, "Ring mismatch at index {i}");
        }

        let expected_polygons = [
            vec![vec![(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)]],
            vec![vec![(0.0, 0.0), (10.0, 0.0), (5.0, 8.0)]],
            vec![vec![(0.0, 0.0), (3.0, 0.0), (3.0, 3.0), (0.0, 3.0)]],
            vec![vec![
                (100.0, 100.0),
                (110.0, 100.0),
                (110.0, 110.0),
                (100.0, 110.0),
            ]],
        ];
        let polygons_marker = &block.cols[3];
        for (i, expected) in expected_polygons.iter().enumerate() {
            let value: ArraySliceIterator = polygons_marker.get(i).unwrap().try_into()?;
            let mut actual = Vec::with_capacity(expected.len());
            for slice in value {
                let points: TupleSliceIterator = slice.try_into()?;
                let mut ring = Vec::with_capacity(expected[0].len());
                for p in points {
                    let (x, y): (f64, f64) = p.try_into()?;
                    ring.push((x, y));
                }
                actual.push(ring);
            }
            assert_eq!(actual, *expected, "Polygon mismatch at index {i}");
        }

        let expected_multipolygons = [
            vec![
                vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)],
                vec![(15.0, 15.0), (25.0, 15.0), (25.0, 25.0), (15.0, 25.0)],
            ],
            vec![
                vec![(0.0, 0.0), (10.0, 0.0), (5.0, 8.0)],
                vec![(4.0, 2.0), (6.0, 2.0), (5.0, 4.0)],
            ],
            vec![
                vec![(0.0, 0.0), (3.0, 0.0), (3.0, 3.0), (0.0, 3.0)],
                vec![(5.0, 5.0), (9.0, 5.0), (9.0, 9.0), (5.0, 9.0)],
                vec![(6.0, 6.0), (8.0, 6.0), (8.0, 8.0), (6.0, 8.0)],
            ],
            vec![
                vec![
                    (100.0, 100.0),
                    (105.0, 100.0),
                    (105.0, 105.0),
                    (100.0, 105.0),
                ],
                vec![
                    (108.0, 108.0),
                    (112.0, 108.0),
                    (112.0, 112.0),
                    (108.0, 112.0),
                ],
            ],
        ];
        let multipolygons_marker = &block.cols[4];
        for (i, expected) in expected_multipolygons.iter().enumerate() {
            let polygons: ArraySliceIterator = multipolygons_marker.get(i).unwrap().try_into()?;
            let mut actual = Vec::new();

            for polygon in polygons {
                let rings: ArraySliceIterator = polygon.try_into()?;
                for ring in rings {
                    let pts: TupleSliceIterator = ring.try_into()?;
                    let mut flat_ring = Vec::new();
                    for pt in pts {
                        let (x, y): (f64, f64) = pt.try_into()?;
                        flat_ring.push((x, y));
                    }
                    actual.push(flat_ring);
                }
            }
            assert_eq!(actual, *expected, "Multi-polygon mismatch at index {i}");
        }

        let expected_linestrings = [
            vec![(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)],
            vec![(0.0, 0.0), (10.0, 0.0), (5.0, 8.0)],
            vec![(0.0, 0.0), (3.0, 3.0), (6.0, 0.0)],
            vec![(100.0, 100.0), (110.0, 110.0), (120.0, 100.0)],
        ];
        let linestrings_marker = &block.cols[5];
        for (i, expected) in expected_linestrings.iter().enumerate() {
            let value: TupleSliceIterator = linestrings_marker.get(i).unwrap().try_into()?;
            let mut actual = Vec::with_capacity(expected.len());
            for point in value {
                let (x, y): (f64, f64) = point.try_into()?;
                actual.push((x, y));
            }
            assert_eq!(actual, *expected, "LineString mismatch at index {i}");
        }

        let expected_multilinestrings = [
            vec![
                vec![(0.0, 0.0), (20.0, 0.0), (20.0, 20.0), (0.0, 20.0)],
                vec![(5.0, 5.0), (15.0, 5.0), (15.0, 15.0), (5.0, 15.0)],
            ],
            vec![
                vec![(0.0, 0.0), (10.0, 10.0)],
                vec![(0.0, 10.0), (10.0, 0.0)],
            ],
            vec![
                vec![(0.0, 0.0), (3.0, 0.0), (6.0, 0.0)],
                vec![(0.0, 0.0), (0.0, 3.0), (0.0, 6.0)],
            ],
            vec![
                vec![(100.0, 100.0), (105.0, 110.0), (110.0, 100.0)],
                vec![(120.0, 120.0), (130.0, 130.0), (140.0, 120.0)],
                vec![(150.0, 150.0), (160.0, 160.0)],
            ],
        ];
        let multilinestrings_marker = &block.cols[6];
        for (i, expected) in expected_multilinestrings.iter().enumerate() {
            let lines: ArraySliceIterator = multilinestrings_marker.get(i).unwrap().try_into()?;
            let mut actual = Vec::with_capacity(expected.len());

            for slice in lines {
                let pts: TupleSliceIterator = slice.try_into()?;
                let mut line = Vec::with_capacity(pts.len());
                for p in pts {
                    let (x, y): (f64, f64) = p.try_into()?;
                    line.push((x, y));
                }
                actual.push(line);
            }
            assert_eq!(actual, *expected, "Multi-lineString mismatch at index {i}");
        }

        Ok(())
    }

    //noinspection RsApproxConstant
    #[allow(clippy::approx_constant)]
    #[test]
    fn float_sample() -> TestResult {
        let buf = load("./test_data/float_sample.native")?;
        let (_, block) = parse_block(&buf)?;

        //    ┌─id─┬─────f32─┬────────────────f64─┬───────bf16─┐
        // 1. │  0 │    3.14 │  3.141592653589793 │      3.125 │
        // 2. │  1 │    2.71 │  2.718281828459045 │   2.703125 │
        // 3. │  2 │    1.41 │ 1.4142135623730951 │    1.40625 │
        // 4. │  3 │ 0.57721 │ 0.5772156649015329 │ 0.57421875 │
        //    └────┴─────────┴────────────────────┴────────────┘

        let f32_marker = &block.cols[1];
        let expected_f32 = [3.14f32, 2.71, 1.41, 0.57721];

        for (i, expected) in expected_f32.iter().enumerate() {
            let value: f32 = f32_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let f64_marker = &block.cols[2];
        let expected_f64 = [
            3.141592653589793,
            2.718281828459045,
            1.4142135623730951,
            0.5772156649015329,
        ];
        for (i, expected) in expected_f64.iter().enumerate() {
            let value: f64 = f64_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let bf16_marker = &block.cols[3];
        let expected_bf16 = [
            bf16::from_f32(3.125f32),
            bf16::from_f32(2.703125),
            bf16::from_f32(1.40625),
            bf16::from_f32(0.57421875),
        ];
        for (i, expected) in expected_bf16.iter().enumerate() {
            let value: bf16 = bf16_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn bool_array_sample() -> TestResult {
        let buf = load("./test_data/bool_array_sample.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,"[true, false, true]"
        // 1,"[false, false, true]"
        // 2,"[true, true, false]"
        // 3,"[false, true, false]"
        // 4,[]
        // 5,[true]

        let expected = [
            vec![true, false, true],
            vec![false, false, true],
            vec![true, true, false],
            vec![false, true, false],
            vec![],
            vec![true],
        ];
        let bool_array_marker = &block.cols[1];
        for (i, expected) in expected.iter().enumerate() {
            let value: BoolSliceIterator = bool_array_marker.get(i).unwrap().try_into()?;
            let mut actual = vec![];
            for b in value {
                actual.push(b);
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }
}
