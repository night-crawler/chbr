use std::{marker::PhantomData, ops::Range};

use chrono::{DateTime, TimeZone};
use zerocopy::little_endian::{F32, F64, I16, I32, I64, I128, U16, U32, U64, U128};

use crate::value::Value::JsonSlice;
use crate::{
    Bf16Data, ByteExt as _, Date16Data, Date32Data, I256, Ipv4Data, Ipv6Data, U256, UuidData,
    macros::define_slice_fns,
    mark::{Mark, Nullable},
    types::OffsetIndexPair as _,
    value::{MapIterator, Value},
};

impl<'a> Mark<'a> {
    pub fn get(&'a self, index: usize) -> Option<Value<'a>> {
        match self {
            Mark::Empty => None,
            Mark::Bool(is_null) => is_null.get(index).map(|&null| Value::Bool(null == 1)),
            Mark::Int8(bv) => bv.get(index).copied().map(Value::Int8),
            Mark::Int16(bv) => bv.get(index).map(|v| v.get()).map(Value::Int16),
            Mark::Int32(bv) => bv.get(index).map(|v| v.get()).map(Value::Int32),
            Mark::Int64(bv) => bv.get(index).map(|v| v.get()).map(Value::Int64),
            Mark::Int128(bv) => bv.get(index).map(Value::Int128),
            Mark::Int256(bv) => bv.get(index).map(Value::Int256),
            Mark::UInt8(bv) => bv.get(index).copied().map(Value::UInt8),
            Mark::UInt16(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt16),
            Mark::UInt32(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt32),
            Mark::UInt64(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt64),
            Mark::UInt128(bv) => bv.get(index).map(Value::UInt128),
            Mark::UInt256(bv) => bv.get(index).map(Value::UInt256),
            Mark::Float32(bv) => bv.get(index).map(|v| v.get()).map(Value::Float32),
            Mark::Float64(bv) => bv.get(index).map(|v| v.get()).map(Value::Float64),
            Mark::BFloat16(bv) => {
                let value = *bv.get(index)?;
                Some(Value::BFloat16(value.into()))
            }
            Mark::Decimal32(d) => d.get(index),
            Mark::Decimal64(d) => d.get(index),
            Mark::Decimal128(d) => d.get(index),
            Mark::Decimal256(d) => d.get(index),
            Mark::String(strings) => Some(Value::String(strings.get(index)?)),
            Mark::FixedString(fs) => fs.get(index),
            Mark::Uuid(bv) => {
                let value = bv.get(index)?;
                Some(Value::Uuid(value))
            }
            Mark::Date(bv) => {
                let value = *bv.get(index)?;
                Some(Value::Date(value.into()))
            }
            Mark::Date32(bv) => {
                let value = *bv.get(index)?;
                Some(Value::Date32(value.into()))
            }
            Mark::DateTime(d) => d.get(index),
            Mark::DateTime64(d) => d.get(index),
            Mark::Ipv4(data) => {
                let value = *data.get(index)?;
                Some(Value::Ipv4(value.into()))
            }
            Mark::Ipv6(data) => {
                let value = data.get(index)?;
                Some(Value::Ipv6(value))
            }
            Mark::Point(_) => unreachable!("Point should be covered by Tuple(f64, f64)"),
            Mark::Ring(_)
            | Mark::Polygon(_)
            | Mark::MultiPolygon(_)
            | Mark::LineString(_)
            | Mark::MultiLineString(_) => {
                unreachable!("Geometric types should be covered by arrays")
            }
            Mark::Enum8(v) => {
                let variant = *v.data.get(index)?;
                if let Ok(index) = v.variants.binary_search_by_key(&variant, |(_, id)| *id) {
                    return Some(Value::String(v.variants[index].0));
                }
                // actually, at this point it's broken, but we trust clickhouse!
                None
            }
            Mark::Enum16(v) => {
                let variant = v.data.get(index)?.get();
                if let Ok(index) = v.variants.binary_search_by_key(&variant, |(_, id)| *id) {
                    return Some(Value::String(v.variants[index].0));
                }
                None
            }
            Mark::LowCardinality(lc) => lc.get(index),
            Mark::Array(a) => {
                let (start, end) = a.offsets.offset_indices(index).unwrap()?;
                Some(a.values.slice(start..end))
            }

            Mark::Tuple(inner) => Some(Value::Tuple { mark: inner, index }),
            Mark::Nullable(n) => n.get(index),
            Mark::Map(mark_map) => Some(Value::Map {
                mark: mark_map,
                index,
            }),
            Mark::Variant(v) => v.get(index),
            Mark::Nested(n) => {
                // verify the index is present
                let _ = n.array_of_tuples.get(index)?;
                Some(Value::Nested { mark: n, index })
            }
            Mark::Dynamic(d) => d.get(index),
            Mark::Json(j) => Some(Value::Json { mark: j, index }),
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
            Mark::BFloat16(bv) => Value::BFloat16Slice(&bv[idx]),
            Mark::Uuid(bv) => Value::UuidSlice(&bv[idx]),
            Mark::Date(bv) => Value::Date16Slice(&bv[idx]),
            Mark::Date32(bv) => Value::Date32Slice(&bv[idx]),
            Mark::Ipv4(bv) => Value::Ipv4Slice(&bv[idx]),
            Mark::Ipv6(bv) => Value::Ipv6Slice(&bv[idx]),
            Mark::String(data) => Value::StringSlice(&data[idx]),

            Mark::Decimal32(d) => Value::Decimal32Slice {
                precision: d.precision,
                slice: &d.data[idx],
            },
            Mark::Decimal64(d) => Value::Decimal64Slice {
                precision: d.precision,
                slice: &d.data[idx],
            },
            Mark::Decimal128(d) => Value::Decimal128Slice {
                precision: d.precision,
                slice: &d.data[idx],
            },
            Mark::Decimal256(d) => Value::Decimal256Slice {
                precision: d.precision,
                slice: &d.data[idx],
            },
            Mark::FixedString(mark) => Value::FixedStringSlice {
                mark,
                slice_indices: idx.try_into().unwrap(),
            },

            Mark::DateTime(d) => Value::DateTime32Slice {
                tz: d.tz,
                slice: &d.data[idx],
            },
            Mark::DateTime64(d) => Value::DateTime64Slice {
                precision: d.precision,
                tz: d.tz,
                slice: &d.data[idx],
            },

            Mark::Point(_)
            | Mark::Ring(_)
            | Mark::Polygon(_)
            | Mark::MultiPolygon(_)
            | Mark::LineString(_)
            | Mark::MultiLineString(_) => unreachable!("must be covered by array marker already"),
            Mark::Enum8(mark) => Value::Enum8Slice {
                mark,
                slice_indices: idx.try_into().unwrap(),
            },
            Mark::Enum16(mark) => Value::Enum16Slice {
                mark,
                slice_indices: idx.try_into().unwrap(),
            },
            Mark::LowCardinality(mark) => Value::LowCardinalitySlice {
                slice_indices: idx.try_into().unwrap(),
                mark,
            },
            Mark::Array(mark) => Value::ArraySlice {
                mark,
                range: idx.try_into().unwrap(),
            },
            Mark::Tuple(mark) => Value::TupleSlice {
                mark,
                slice_indices: idx.try_into().unwrap(),
            },
            Mark::Nullable(mark) => Value::NullableSlice {
                mark,
                slice_indices: idx.try_into().unwrap(),
            },
            Mark::Map(mark) => Value::MapSlice {
                mark,
                slice_indices: idx.try_into().unwrap(),
            },
            Mark::Nested(mark) => Value::NestedSlice {
                mark,
                slice_indices: idx.try_into().unwrap(),
            },
            Mark::Variant(mark) => Value::VariantSlice {
                mark,
                slice_indices: idx.try_into().unwrap(),
            },
            Mark::Dynamic(mark) => Value::DynamicSlice {
                mark,
                slice_indices: idx.try_into().unwrap(),
            },
            Mark::Json(mark) => JsonSlice {
                mark,
                slice_indices: idx.try_into().unwrap(),
            },
        }
    }

    #[inline]
    pub fn get_str(&'a self, index: usize) -> crate::Result<Option<&'a str>> {
        match self {
            Mark::String(strings) => Ok(strings.get(index).copied()),
            Mark::FixedString(fs) => {
                let offset = fs.size * index;
                let slice = fs.data[offset..offset + fs.size].rtrim_zeros();
                let slice = unsafe { std::str::from_utf8_unchecked(slice) };
                Ok(Some(slice))
            }
            Mark::LowCardinality(lc) => {
                let Some(keys) = &lc.additional_keys else {
                    return Err(crate::Error::CorruptedData(
                        "LowCardinality marker without additional keys".to_owned(),
                    ));
                };

                let Some(value_index) = lc.value_index(index) else {
                    return Ok(None);
                };

                if value_index == 0 && lc.is_nullable {
                    return Ok(None);
                }

                let Mark::String(keys) = keys.as_ref() else {
                    return Err(crate::Error::MismatchedType(keys.as_str(), "&str"));
                };

                Ok(keys.get(value_index).copied())
            }
            mark => Err(crate::Error::MismatchedType(mark.as_str(), "&str")),
        }
    }

    #[inline]
    pub fn get_opt_str(&'a self, index: usize) -> crate::Result<Option<Option<&'a str>>> {
        let Mark::Nullable(Nullable { mask, data }) = self else {
            // convenience wrapper
            let value = self.get_str(index)?;
            return Ok(Some(value));
        };

        if mask.get(index) == Some(&1) {
            return Ok(Some(None));
        }

        Ok(Some(data.get_str(index)?))
    }

    #[inline]
    pub fn get_u128(&'a self, index: usize) -> crate::Result<Option<u128>> {
        let Mark::UInt128(bv) = self else {
            return Err(crate::Error::MismatchedType(self.as_str(), "UInt128"));
        };

        let value = bv.get(index).copied().map(U128::get);
        Ok(value)
    }

    #[expect(clippy::needless_pass_by_value)]
    #[inline]
    pub fn get_datetime<T: TimeZone>(
        &'a self,
        index: usize,
        tz: T,
    ) -> crate::Result<Option<DateTime<T>>> {
        match self {
            Mark::DateTime(d) => {
                let value = d
                    .data
                    .get(index)
                    .map(|dt| dt.with_tz(d.tz))
                    .map(|dt| dt.with_timezone(&tz));
                Ok(value)
            }
            Mark::DateTime64(d) => {
                let value = d
                    .data
                    .get(index)
                    .map(|dt| {
                        dt.with_tz_and_precision(d.tz, d.precision).ok_or_else(|| {
                            crate::Error::Overflow("DateTime64 value out of range".to_owned())
                        })
                    })
                    .transpose()?
                    .map(|dt| dt.with_timezone(&tz));

                Ok(value)
            }
            _ => Err(crate::Error::MismatchedType(self.as_str(), "DateTime")),
        }
    }

    #[inline]
    pub fn get_uuid(&'a self, index: usize) -> crate::Result<Option<uuid::Uuid>> {
        match self {
            Mark::Uuid(bv) => {
                let value = bv.get(index).map(|data| uuid::Uuid::from(*data));
                Ok(value)
            }
            _ => Err(crate::Error::MismatchedType(self.as_str(), "Uuid")),
        }
    }

    #[inline]
    pub fn get_ipv6(&'a self, index: usize) -> crate::Result<Option<std::net::Ipv6Addr>> {
        match self {
            Mark::Ipv6(bv) => {
                let value = bv.get(index).copied().map(Into::into);
                Ok(value)
            }
            _ => Err(crate::Error::MismatchedType(self.as_str(), "Ipv6")),
        }
    }

    #[inline]
    pub fn get_opt_ipv6(
        &'a self,
        index: usize,
    ) -> crate::Result<Option<Option<std::net::Ipv6Addr>>> {
        let Mark::Nullable(Nullable { mask, data }) = self else {
            let value = self.get_ipv6(index)?;
            return Ok(Some(value));
        };

        if mask.get(index) == Some(&1) {
            return Ok(Some(None));
        }

        let value = data.get_ipv6(index)?;
        Ok(Some(value))
    }

    #[inline]
    pub fn get_ipv4(&'a self, index: usize) -> crate::Result<Option<std::net::Ipv4Addr>> {
        match self {
            Mark::Ipv4(bv) => {
                let value = bv.get(index).copied().map(Into::into);
                Ok(value)
            }
            _ => Err(crate::Error::MismatchedType(self.as_str(), "Ipv4")),
        }
    }

    #[inline]
    pub fn get_bool(&'a self, index: usize) -> crate::Result<Option<bool>> {
        match self {
            Mark::Bool(bv) => {
                let value = bv.get(index).copied().map(|v| v != 0);
                Ok(value)
            }
            _ => Err(crate::Error::MismatchedType(self.as_str(), "bool")),
        }
    }

    #[inline]
    pub fn get_f64(&'a self, index: usize) -> crate::Result<Option<f64>> {
        match self {
            Mark::Float64(bv) => {
                let value = bv.get(index).map(|v| v.get());
                Ok(value)
            }
            _ => Err(crate::Error::MismatchedType(self.as_str(), "f64")),
        }
    }

    #[inline]
    pub fn get_u32(&'a self, index: usize) -> crate::Result<Option<u32>> {
        match self {
            Mark::UInt32(bv) => {
                let value = bv.get(index).copied().map(U32::get);
                Ok(value)
            }
            _ => Err(crate::Error::MismatchedType(self.as_str(), "u32")),
        }
    }

    #[inline]
    pub fn slice_lc_strs(
        &'a self,
        idx: Range<usize>,
    ) -> crate::Result<impl Iterator<Item = &'a str>> {
        let Mark::LowCardinality(lc) = self else {
            return Err(crate::Error::MismatchedType(
                self.as_str(),
                "LowCardinality",
            ));
        };

        let Some(keys) = &lc.additional_keys else {
            return Err(crate::Error::CorruptedData(
                "LowCardinality marker without additional keys".to_owned(),
            ));
        };

        let Mark::String(keys) = keys.as_ref() else {
            return Err(crate::Error::MismatchedType(keys.as_str(), "String"));
        };

        let index_it: Box<dyn Iterator<Item = usize> + '_> = match lc.indices.as_ref() {
            Mark::UInt8(bv) => Box::new(bv[idx].iter().copied().map(usize::from)),
            Mark::UInt16(bv) => Box::new(bv[idx].iter().map(|v| usize::from(v.get()))),
            Mark::UInt32(bv) => Box::new(bv[idx].iter().map(|v| v.get() as usize)),
            Mark::UInt64(bv) => Box::new(bv[idx].iter().map(|v| usize::try_from(v.get()).unwrap())),
            _ => unreachable!("must never have any other type"),
        };

        Ok(index_it.map(move |idx| keys.get(idx).copied().unwrap()))
    }

    #[inline]
    pub fn get_array_lc_strs(
        &'a self,
        index: usize,
    ) -> crate::Result<Option<impl Iterator<Item = &'a str>>> {
        if matches!(self, Mark::Empty) {
            return Ok(None);
        }

        let Mark::Array(array) = self else {
            return Err(crate::Error::MismatchedType(self.as_str(), "Array"));
        };

        let Some((start, end)) = array.offsets.offset_indices(index)? else {
            return Ok(None);
        };

        if matches!(array.values.as_ref(), Mark::Empty) {
            return Ok(None);
        }

        let it = array.values.slice_lc_strs(start..end)?;
        Ok(Some(it))
    }

    #[inline]
    pub fn get_map<K, V>(&'a self, index: usize) -> crate::Result<Option<MapIterator<'a, K, V>>> {
        let Mark::Map(map) = self else {
            return Err(crate::Error::MismatchedType(self.as_str(), "Map"));
        };
        let Some((start, end)) = map.offsets.offset_indices(index)? else {
            return Ok(None);
        };

        let it = MapIterator {
            keys: &map.keys,
            values: &map.values,
            range: start..end,
            _marker: PhantomData,
        };

        Ok(Some(it))
    }

    #[inline]
    pub fn get_arr_bool_iter(
        &'a self,
        index: usize,
    ) -> crate::Result<Option<impl Iterator<Item = bool>>> {
        let Mark::Array(arr) = self else {
            return Err(crate::Error::MismatchedType(self.as_str(), "Array"));
        };

        let Some((start, end)) = arr.offsets.offset_indices(index)? else {
            return Ok(None);
        };

        let slice = match arr.values.as_ref() {
            Mark::Bool(bv) => &bv[start..end],
            Mark::Empty => &[],
            other => return Err(crate::Error::MismatchedType(other.as_str(), "Int8")),
        };

        Ok(Some(slice.iter().copied().map(|b| b != 0)))
    }

    define_slice_fns!(
        (Int8, i8),
        (Int16, I16),
        (Int32, I32),
        (Int64, I64),
        (Int128, I128),
        (Int256, I256),
        (UInt8, u8),
        (UInt16, U16),
        (UInt32, U32),
        (UInt64, U64),
        (UInt128, U128),
        (UInt256, U256),
        (Float32, F32),
        (Float64, F64),
        (BFloat16, Bf16Data),
        (String, &'a str),
        (Uuid, UuidData),
        (Date, Date16Data),
        (Date32, Date32Data),
        (Ipv4, Ipv4Data),
        (Ipv6, Ipv6Data),
    );
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr as _};

    use half::bf16;
    use pretty_assertions::assert_eq;
    use testresult::TestResult;
    use zerocopy::little_endian::{I64, U64, U128};

    use crate::value::{DynamicSliceIterator, JsonSliceIterator, VariantSliceIterator};
    use crate::{
        Bf16Data,
        common::load,
        parse::block::parse_single,
        value::{
            ArraySliceIterator, BoolSliceIterator, Enum8SliceIterator, Enum16SliceIterator,
            FixedStringSliceIterator, JsonIterator, LowCardinalitySliceIterator, MapIterator,
            MapSliceIterator, NestedIterator, NestedSliceIterator, NullableSliceIterator,
            TupleSliceIterator, Value,
        },
    };

    #[test]
    fn int_array() -> TestResult {
        let buf = load("./testdata/array.native")?;
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

        let (_, block) = parse_single(&buf)?;

        let index_marker = &block.markers[0];

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

        let arr_marker = &block.markers[1];

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
        let buf = load("./testdata/plain_strings.native")?;
        // 0,hello
        // 1,world
        // 2,clickhouse
        // 3,test
        // 4,example
        // 5,data

        let (_, block) = parse_single(&buf)?;

        let expected_strings = ["hello", "world", "clickhouse", "test", "example", "data"];

        let strings_marker = &block.markers[1];
        for (i, expected) in expected_strings.iter().enumerate() {
            let value: &str = strings_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn plain_strings_array() -> TestResult {
        let buf = load("./testdata/plain_strings_array.native")?;

        // 0,"['apple', 'banana', 'cherry']"
        // 1,"['date', 'elderberry']"
        // 2,"['fig', 'grape', 'honeydew']"
        // 3,['kiwi']
        // 4,[]
        // 5,"['lemon', 'mango']"

        let (_, block) = parse_single(&buf)?;

        let expected_arrays = [
            vec!["apple", "banana", "cherry"],
            vec!["date", "elderberry"],
            vec!["fig", "grape", "honeydew"],
            vec!["kiwi"],
            vec![],
            vec!["lemon", "mango"],
        ];

        let strings_marker = &block.markers[1];

        for (i, expected) in expected_arrays.iter().enumerate() {
            let slice: &[&str] = strings_marker.get(i).unwrap().try_into()?;
            let actual = slice.iter().copied().collect::<Vec<_>>();

            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn lc_string() -> TestResult {
        let buf = load("./testdata/plain_lc_string.native")?;
        let (_, block) = parse_single(&buf)?;

        // 0,apple
        // 1,banana
        // 2,cherry
        // 3,date
        // 4,elderberry
        // 5,fig

        let expected_strings = ["apple", "banana", "cherry", "date", "elderberry", "fig"];

        let strings_marker = &block.markers[1];
        for (i, expected) in expected_strings.iter().enumerate() {
            let value: &str = strings_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn lc_array_string() -> TestResult {
        let buf = load("./testdata/array_lc_string.native")?;
        let (_, block) = parse_single(&buf)?;

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

        let strings_marker = &block.markers[1];
        for (i, expected) in expected_arrays.iter().enumerate() {
            let it: LowCardinalitySliceIterator = strings_marker.get(i).unwrap().try_into()?;
            let mut actual = vec![];
            for value in it {
                let value: &str = value.try_into()?;
                actual.push(value);
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");

            let actual = strings_marker
                .get_array_lc_strs(i)?
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(
                actual, *expected,
                "Mismatch at index {i} (get_array_lc_strs)"
            );
        }

        Ok(())
    }

    #[test]
    fn array_in_array_in64() -> TestResult {
        let buf = load("./testdata/array_in_array_in64.native")?;
        let (_, block) = parse_single(&buf)?;

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

        let arrs_marker = &block.markers[1];

        for (i, expected) in expected_arrays.iter().enumerate() {
            let v = arrs_marker.get(i).unwrap();
            let outer: ArraySliceIterator<&[I64]> = v.try_into()?;
            let mut actual_outer = vec![];
            for slice in outer.flatten() {
                let inner = slice.iter().map(|&v| v.get()).collect::<Vec<_>>();
                actual_outer.push(inner);
            }

            assert_eq!(actual_outer, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn nullable_string() -> TestResult {
        let buf = load("./testdata/nullable_string.native")?;
        let (_, block) = parse_single(&buf)?;

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

        let strings_marker = &block.markers[1];
        for (i, expected) in expected_col.iter().enumerate() {
            let value: Option<&str> = strings_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");

            let value = strings_marker.get_opt_str(i)?.unwrap();
            assert_eq!(value, *expected, "Mismatch at index {i} (get_opt_str)");
        }

        Ok(())
    }

    #[test]
    fn tuple_sample() -> TestResult {
        let buf = load("./testdata/tuple.native")?;
        let (_, block) = parse_single(&buf)?;

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

        let tuples_marker = &block.markers[1];

        for (i, expected) in expected_tuples.iter().enumerate() {
            let value: (i64, &str) = tuples_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn map_sample() -> TestResult {
        let buf = load("./testdata/map_sample.native")?;
        let (_, block) = parse_single(&buf)?;

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

        let map_marker = &block.markers[1];
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
        let buf = load("./testdata/array_map_sample.native")?;
        let (_, block) = parse_single(&buf)?;

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

        let map_marker = &block.markers[1];
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
        let buf = load("./testdata/map_in_map.native")?;
        let (_, block) = parse_single(&buf)?;

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

        let map_marker = &block.markers[1];

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
        let buf = load("./testdata/array_of_tuples.native")?;
        let (_, block) = parse_single(&buf)?;

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

        let tuples_marker = &block.markers[1];

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
        let buf = load("./testdata/variant.native")?;
        let (_, block) = parse_single(&buf)?;
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

        let variant_marker = &block.markers[1];
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
        let buf = load("./testdata/uuid_and_dates.native")?;
        let (_, block) = parse_single(&buf)?;
        // UUID, Date, Date32, DateTime, DateTime64
        // 00000000-0000-0000-0000-000000000001,2023-01-01,2023-01-01,2023-01-01 12:00:00,2023-01-01T12:00:00.123Z
        // 00000000-0000-0000-0000-000000000002,2023-02-01,2023-02-01,2023-02-01 12:00:00,2023-02-01T12:00:00.456Z
        // 00000000-0000-0000-0000-000000000003,2023-03-01,2023-03-01,2023-03-01 12:00:00,2023-03-01T12:00:00.789Z
        // 00000000-0000-0000-0000-000000000004,2023-03-01,1969-09-23,2023-03-01 12:00:00,2023-03-01T12:00:00.789Z

        let uuid_marker = &block.markers[0];
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

        let date_marker = &block.markers[1];
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

        let date32_marker = &block.markers[2];
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

        let datetime_marker = &block.markers[3];
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

            let value = datetime_marker.get_datetime(i, chrono_tz::UTC)?.unwrap();
            assert_eq!(value, *expected, "Mismatch at index {i} (get_datetime)");
        }

        let datetime64_marker = &block.markers[4];
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
        let buf = load("./testdata/decimal_sample.native")?;
        let (_, block) = parse_single(&buf)?;

        // 0,1.234,1.234567,1.234567890123,1.234567890123456556104338
        // 1,2.345,2.345678,2.345678901234,2.345678901234567875661641
        // 2,3.456,3.456789,3.456789012345,3.456789012345678555325440

        let expected_d32 = [
            rust_decimal::Decimal::new(1234, 3),
            rust_decimal::Decimal::new(2345, 3),
            rust_decimal::Decimal::new(3456, 3),
        ];
        let decimal32_marker = &block.markers[1];
        for (i, expected) in expected_d32.iter().enumerate() {
            let value: rust_decimal::Decimal = decimal32_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let expected_d64 = [
            rust_decimal::Decimal::new(1234567, 6),
            rust_decimal::Decimal::new(2345678, 6),
            rust_decimal::Decimal::new(3456789, 6),
        ];

        let decimal64_marker = &block.markers[2];
        for (i, expected) in expected_d64.iter().enumerate() {
            let value: rust_decimal::Decimal = decimal64_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let expected_d128 = [
            rust_decimal::Decimal::new(1234567890123, 12),
            rust_decimal::Decimal::new(2345678901234, 12),
            rust_decimal::Decimal::new(3456789012345, 12),
        ];
        let decimal128_marker = &block.markers[3];
        for (i, expected) in expected_d128.iter().enumerate() {
            let value: rust_decimal::Decimal = decimal128_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        // expect panic for decimal256, it's not implemented

        Ok(())
    }

    #[test]
    fn ip_sample() -> TestResult {
        let buf = load("./testdata/ip_sample.native")?;
        let (_, block) = parse_single(&buf)?;

        // 0,100.64.0.2,2001:db8:0:0:0:ff00:42:8329
        // 1,127.0.0.1,0:0:0:0:0:0:0:1
        // 2,10.10.10.10,2001:db8:85a3:0:0:8a2e:370:7334

        let ipv4_marker = &block.markers[1];
        let expected_ipv4 = [
            std::net::Ipv4Addr::new(100, 64, 0, 2),
            std::net::Ipv4Addr::new(127, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 10, 10, 10),
        ];

        for (i, expected) in expected_ipv4.iter().enumerate() {
            let value: std::net::Ipv4Addr = ipv4_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let ipv6_marker = &block.markers[2];
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
        let buf = load("./testdata/geo_sample.native")?;
        let (_, block) = parse_single(&buf)?;

        let expected_points = [(10.0, 10.0), (5.0, 5.0), (0.0, 0.0), (100.0, 100.0)];
        let points_marker = &block.markers[1];
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
        let rings_marker = &block.markers[2];
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
        let polygons_marker = &block.markers[3];
        for (i, expected) in expected_polygons.iter().enumerate() {
            let value: ArraySliceIterator<TupleSliceIterator> =
                polygons_marker.get(i).unwrap().try_into()?;
            let mut actual = Vec::with_capacity(expected.len());
            for points in value.flatten() {
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
        let multipolygons_marker = &block.markers[4];
        for (i, expected) in expected_multipolygons.iter().enumerate() {
            let polygons: ArraySliceIterator<ArraySliceIterator<TupleSliceIterator>> =
                multipolygons_marker.get(i).unwrap().try_into()?;
            let mut actual = Vec::new();

            for polygon in polygons.flatten() {
                for ring in polygon.flatten() {
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
        let linestrings_marker = &block.markers[5];
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
        let multilinestrings_marker = &block.markers[6];
        for (i, expected) in expected_multilinestrings.iter().enumerate() {
            let lines: ArraySliceIterator<TupleSliceIterator> =
                multilinestrings_marker.get(i).unwrap().try_into()?;
            let mut actual = Vec::with_capacity(expected.len());

            for pts in lines.flatten() {
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
    #[expect(clippy::approx_constant)]
    #[test]
    fn float_sample() -> TestResult {
        let buf = load("./testdata/float_sample.native")?;
        let (_, block) = parse_single(&buf)?;

        //    ┌─id─┬─────f32─┬────────────────f64─┬───────bf16─┐
        // 1. │  0 │    3.14 │  3.141592653589793 │      3.125 │
        // 2. │  1 │    2.71 │  2.718281828459045 │   2.703125 │
        // 3. │  2 │    1.41 │ 1.4142135623730951 │    1.40625 │
        // 4. │  3 │ 0.57721 │ 0.5772156649015329 │ 0.57421875 │
        //    └────┴─────────┴────────────────────┴────────────┘

        let f32_marker = &block.markers[1];
        let expected_f32 = [3.14f32, 2.71, 1.41, 0.57721];

        for (i, expected) in expected_f32.iter().enumerate() {
            let value: f32 = f32_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let f64_marker = &block.markers[2];
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

        let bf16_marker = &block.markers[3];
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
        let buf = load("./testdata/bool_array_sample.native")?;
        let (_, block) = parse_single(&buf)?;

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
        let bool_array_marker = &block.markers[1];
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

    #[test]
    fn nullable_string_array() -> TestResult {
        let buf = load("./testdata/nullable_string_array.native")?;
        let (_, block) = parse_single(&buf)?;

        // 0,"['apple', 'banana', null]"
        // 1,"[null, 'date', 'elderberry']"
        // 2,"['fig', null, 'honeydew']"
        // 3,[null]
        // 4,[]
        // 5,"['lemon', null, 'mango']"

        let expected = [
            vec![Some("apple"), Some("banana"), None],
            vec![None, Some("date"), Some("elderberry")],
            vec![Some("fig"), None, Some("honeydew")],
            vec![None],
            vec![],
            vec![Some("lemon"), None, Some("mango")],
        ];

        let nullable_string_array_marker = &block.markers[1];
        for (i, expected) in expected.iter().enumerate() {
            let value: NullableSliceIterator =
                nullable_string_array_marker.get(i).unwrap().try_into()?;
            let mut actual = vec![];
            for item in value {
                let item: Option<&str> = item.try_into()?;
                actual.push(item);
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn metric_activity() -> TestResult {
        let data = load("./testdata/metric_activity.native")?;
        let (_, block) = parse_single(&data)?;

        for index in 0..block.num_rows {
            for (col, name) in block.markers.iter().zip(block.col_names.iter()) {
                if !name.contains("attrs") {
                    continue;
                }
                let value = col.get(index).unwrap();
                let value: MapIterator<&str, &str> = value.try_into()?;

                let mut map = HashMap::new();
                for (key, val) in value.flatten() {
                    map.insert(key, val);
                }
            }
        }

        Ok(())
    }

    #[test]
    fn array_of_nested() -> TestResult {
        let data = load("./testdata/array_of_nested.native")?;
        let (_, block) = parse_single(&data)?;

        let expected: [Vec<Vec<(i64, &str)>>; 6] = [
            vec![vec![(1, "Alice"), (2, "Bob")]],
            vec![vec![(3, "Charlie"), (4, "Diana")]],
            vec![vec![(5, "Eve")]],
            vec![vec![]],
            vec![vec![(6, "Frank"), (7, "Grace")]],
            vec![vec![(8, "Heidi")]],
        ];

        let nested_mark = &block.markers[1];

        for (row_idx, expected_outer) in expected.iter().enumerate() {
            let outer_slice: NestedSliceIterator = nested_mark.get(row_idx).unwrap().try_into()?;

            let mut actual_outer = Vec::<Vec<(i64, &str)>>::new();

            for nested_res in outer_slice {
                let nested_iter: NestedIterator = nested_res?;

                let mut inner_rows = Vec::<(i64, &str)>::new();

                for nested_row in nested_iter {
                    let (mut id, mut name) = (None, None);

                    for (field_name, field_value) in nested_row {
                        match field_name {
                            "child_id" => id = Some(field_value.try_into()?),
                            "child_name" => name = Some(field_value.try_into()?),
                            _ => {}
                        }
                    }

                    inner_rows.push((
                        id.expect("missing child_id"),
                        name.expect("missing child_name"),
                    ));
                }

                actual_outer.push(inner_rows);
            }

            assert_eq!(
                actual_outer, *expected_outer,
                "Mismatch in Array(Nested) at top-level row {row_idx}"
            );
        }

        Ok(())
    }

    #[test]
    fn simple_nested() -> TestResult {
        let data = load("./testdata/simple_nested.native")?;
        let (_, block) = parse_single(&data)?;

        let expected: [Vec<(i64, &str)>; 6] = [
            vec![(1, "Alice"), (2, "Bob")],
            vec![(3, "Charlie"), (4, "Diana")],
            vec![(5, "Eve")],
            vec![],
            vec![(6, "Frank"), (7, "Grace")],
            vec![(8, "Heidi")],
        ];

        let nested_marker = &block.markers[1];

        for (row_idx, expected_nested) in expected.iter().enumerate() {
            let nested_iter: NestedIterator = nested_marker.get(row_idx).unwrap().try_into()?;

            let mut actual_nested = Vec::<(i64, &str)>::new();
            for nested_row in nested_iter {
                let mut id: Option<i64> = None;
                let mut name: Option<&str> = None;

                for (field_name, field_value) in nested_row {
                    match field_name {
                        "child_id" => id = Some(field_value.try_into()?),
                        "child_name" => name = Some(field_value.try_into()?),
                        _ => {}
                    }
                }

                actual_nested.push((
                    id.expect("missing child_id"),
                    name.expect("missing child_name"),
                ));
            }

            assert_eq!(
                actual_nested, *expected_nested,
                "Mismatch in nested data at top-level row {row_idx}"
            );
        }

        Ok(())
    }

    #[test]
    fn fixed_string_sample() -> TestResult {
        let data = load("./testdata/fixed_string_sample.native")?;
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
            let value: &str = fixed_string_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn fixed_string_array() -> TestResult {
        let data = load("./testdata/fixed_string_array.native")?;
        let (_, block) = parse_single(&data)?;

        // 0,"['fixed string 1\u0000\u0000', 'fixed string 2\u0000\u0000']"
        // 1,"['fixed string 3\u0000\u0000', 'fixed string 4\u0000\u0000']"
        // 2,"['fixed string 5\u0000\u0000', 'fixed string 6\u0000\u0000']"
        // 3,['fixed string 7\u0000\u0000']
        // 4,[]
        // 5,"['fixed string 8\u0000\u0000', 'fixed string 9\u0000\u0000']"

        let expected = [
            vec!["fixed string 1", "fixed string 2"],
            vec!["fixed string 3", "fixed string 4"],
            vec!["fixed string 5", "fixed string 6"],
            vec!["fixed string 7"],
            vec![],
            vec!["fixed string 8", "fixed string 9"],
        ];

        let fixed_string_array_marker = &block.markers[1];
        for (i, expected) in expected.iter().enumerate() {
            let value: FixedStringSliceIterator =
                fixed_string_array_marker.get(i).unwrap().try_into()?;
            let mut actual = vec![];
            for item in value {
                actual.push(item);
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn enums_sample() -> TestResult {
        let data = load("./testdata/enums_sample.native")?;
        let (_, block) = parse_single(&data)?;

        // 0,Red,Foo
        // 1,Green,Bar
        // 2,Blue,Foo
        // 3,Red,Bar
        // 4,Green,Foo
        // 5,Blue,Bar

        let expected_e8 = ["Red", "Green", "Blue", "Red", "Green", "Blue"];

        let e8_marker = &block.markers[1];
        for (i, expected) in expected_e8.iter().enumerate() {
            let value: &str = e8_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let expected_e16 = ["Foo", "Bar", "Foo", "Bar", "Foo", "Bar"];
        let e16_marker = &block.markers[2];
        for (i, expected) in expected_e16.iter().enumerate() {
            let value: &str = e16_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn enums_array_sample() -> TestResult {
        let data = load("./testdata/enums_array_sample.native")?;
        let (_, block) = parse_single(&data)?;

        // 0,"['Red', 'Green']",['Foo']
        // 1,"['Blue', 'Red']",['Bar']
        // 2,['Green'],"['Foo', 'Bar']"
        // 3,[],['Foo']
        // 4,"['Red', 'Blue']",[]
        // 5,"['Green', 'Red', 'Blue']",['Bar']

        let expected_e8 = [
            vec!["Red", "Green"],
            vec!["Blue", "Red"],
            vec!["Green"],
            vec![],
            vec!["Red", "Blue"],
            vec!["Green", "Red", "Blue"],
        ];

        let e8_marker = &block.markers[1];
        for (i, expected) in expected_e8.iter().enumerate() {
            let value: Enum8SliceIterator = e8_marker.get(i).unwrap().try_into()?;
            let mut actual = vec![];
            for item in value {
                actual.push(item);
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        let expected_e16 = [
            vec!["Foo"],
            vec!["Bar"],
            vec!["Foo", "Bar"],
            vec!["Foo"],
            vec![],
            vec!["Bar"],
        ];

        let e16_marker = &block.markers[2];
        for (i, expected) in expected_e16.iter().enumerate() {
            let value: Enum16SliceIterator = e16_marker.get(i).unwrap().try_into()?;
            let mut actual = vec![];
            for item in value {
                actual.push(item);
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn bfloat16_array_sample() -> TestResult {
        let data = load("./testdata/bfloat16_array_sample.native")?;
        let (_, block) = parse_single(&data)?;

        //    ┌─id─┬─arr_bf16─────────────────┐
        // 1. │  0 │ [3.125,2.703125,1.40625] │
        // 2. │  1 │ [0.57421875,1.6171875]   │
        // 3. │  2 │ [2.234375]               │
        // 4. │  3 │ []                       │
        // 5. │  4 │ [1.4140625,3.140625]     │
        //    └────┴──────────────────────────┘

        let expected = [
            vec![
                bf16::from_f32(3.125),
                bf16::from_f32(2.703125),
                bf16::from_f32(1.40625),
            ],
            vec![bf16::from_f32(0.57421875), bf16::from_f32(1.6171875)],
            vec![bf16::from_f32(2.234375)],
            vec![],
            vec![bf16::from_f32(1.4140625), bf16::from_f32(3.140625)],
        ];

        let bfloat16_array_marker = &block.markers[1];
        for (i, expected) in expected.iter().enumerate() {
            let value: &[Bf16Data] = bfloat16_array_marker.get(i).unwrap().try_into()?;
            let mut actual: Vec<bf16> = vec![];
            for item in value.iter().copied() {
                actual.push(item.into());
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn array_lc_string_empty() -> TestResult {
        let data = load("./testdata/array_lc_string_empty.native")?;
        let (_, block) = parse_single(&data)?;

        let marker = &block.markers[1];
        for i in 0..block.num_rows {
            let it = marker.get_array_lc_strs(i)?;
            assert!(it.is_none());
        }

        Ok(())
    }

    #[test]
    fn sample_128() -> TestResult {
        let data = load("./testdata/sample_128.native")?;
        let (_, block) = parse_single(&data)?;

        //    ┌─id─┬──────────────────────u128_single─┬─u128_array──────────────────────────────────────────────────────────┬──────────────────────i128_single─┬─i128_array───────────────────────────────────────────────────────────┐
        // 1. │  0 │ 12345678901234567890123456789012 │ [12345678901234567890123456789012,98765432109876543210987654321098] │ 12345678901234567890123456789012 │ [12345678901234567890123456789012,-98765432109876543210987654321098] │
        //    └────┴──────────────────────────────────┴─────────────────────────────────────────────────────────────────────┴──────────────────────────────────┴──────────────────────────────────────────────────────────────────────┘

        let u128_marker = &block.markers[1];
        let expected_u128 = [12345678901234567890123456789012u128];
        for (i, expected) in expected_u128.iter().enumerate() {
            let value: u128 = u128_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        let u128_array_marker = &block.markers[2];
        let expected_u128_array = [vec![
            12345678901234567890123456789012u128,
            98765432109876543210987654321098u128,
        ]];

        for (i, expected) in expected_u128_array.iter().enumerate() {
            let value: &[U128] = u128_array_marker.get(i).unwrap().try_into()?;
            let mut actual = vec![];
            for item in value {
                actual.push(item.get());
            }
            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn nullable_lc_str() -> TestResult {
        let data = load("./testdata/nullable_lc_str.native")?;
        let (_, block) = parse_single(&data)?;

        //    ┌─id─┬─nlc_str─┐
        // 1. │  0 │ apple   │
        // 2. │  1 │ ᴺᵁᴸᴸ    │
        // 3. │  2 │ banana  │
        // 4. │  3 │ cherry  │
        // 5. │  4 │ ᴺᵁᴸᴸ    │
        // 6. │  5 │ date    │
        //    └────┴─────────┘

        let expected = [
            Some("apple"),
            None,
            Some("banana"),
            Some("cherry"),
            None,
            Some("date"),
        ];

        let nlc_str_marker = &block.markers[1];
        for (i, expected) in expected.iter().enumerate() {
            let value: Option<&str> = nlc_str_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");

            let value = nlc_str_marker.get_opt_str(i)?.unwrap();
            assert_eq!(value, *expected, "Mismatch at index {i} (get_opt_str)");
        }

        Ok(())
    }

    #[expect(clippy::approx_constant)]
    #[test]
    fn dynamic() -> TestResult {
        let data = load("./testdata/dynamic.native")?;
        let (_, block) = parse_single(&data)?;
        println!("{}", block.num_rows);

        // ┌─id─┬─dyn──────────────────────────────────┐
        // │  0 │ string value                         │
        // │  1 │ 12345                                │
        // │  2 │ [1,2,3]                              │
        // │  3 │ {'key':'value'}                      │
        // │  4 │ 2023-01-01                           │
        // │  5 │ 0                                    │
        // │  6 │ 2023-01-01 12:00:00                  │
        // │  7 │ d60b7c85-0739-4786-a8d9-f1bbc72104df │
        // │  8 │ 3.14                                 │
        // │  9 │ 1.23                                 │
        // └────┴──────────────────────────────────────┘

        let expected = [
            "string value",
            "12345",
            "[1,2,3]",
            "{'key':'value'}",
            "2023-01-01",
            "0",
            "2023-01-01 12:00:00",
            "d60b7c85-0739-4786-a8d9-f1bbc72104df",
            "3.14",
            "1.23",
        ];

        let dynamic_marker = &block.markers[1];

        let row0: &str = dynamic_marker.get(0).unwrap().try_into()?;
        assert_eq!(row0, expected[0], "Mismatch at index 0");

        let row1: i64 = dynamic_marker.get(1).unwrap().try_into()?;
        assert_eq!(row1, 12345, "Mismatch at index 1");

        let row2: &[I64] = dynamic_marker.get(2).unwrap().try_into()?;
        assert_eq!(row2, &[1, 2, 3], "Mismatch at index 2");

        let row3: MapIterator<&str, &str> = dynamic_marker.get(3).unwrap().try_into()?;
        let mut map = HashMap::new();
        for (key, value) in row3.flatten() {
            map.insert(key, value);
        }
        assert_eq!(map.get("key"), Some(&"value"), "Mismatch at index 3");

        let row4: chrono::NaiveDate = dynamic_marker.get(4).unwrap().try_into()?;
        assert_eq!(
            row4,
            chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            "Mismatch at index 4"
        );

        let row5: i64 = dynamic_marker.get(5).unwrap().try_into()?;
        assert_eq!(row5, 0, "Mismatch at index 5");

        let row6: chrono::DateTime<chrono_tz::Tz> = dynamic_marker.get(6).unwrap().try_into()?;
        assert_eq!(row6.to_string(), "2023-01-01 12:00:00 UTC");

        let row7: uuid::Uuid = dynamic_marker.get(7).unwrap().try_into()?;
        assert_eq!(
            row7,
            uuid::Uuid::parse_str("d60b7c85-0739-4786-a8d9-f1bbc72104df")?,
            "Mismatch at index 7"
        );

        let row8: f64 = dynamic_marker.get(8).unwrap().try_into()?;
        assert_eq!(row8, 3.14, "Mismatch at index 8");

        let row9: rust_decimal::Decimal = dynamic_marker.get(9).unwrap().try_into()?;
        assert_eq!(
            row9,
            rust_decimal::Decimal::try_from(1.23f32)?,
            "Mismatch at index 9"
        );

        Ok(())
    }

    #[test]
    fn json() -> TestResult {
        let data = load("./testdata/json.native")?;
        let (_, block) = parse_single(&data)?;

        //    ┌─id─┬─json──────────────────────────────────────────────────────────┐
        //  1. │  0 │ {"key":"value"}                                               │
        //  2. │  1 │ {"array":["1","2","3"]}                                       │
        //  3. │  2 │ {"nested":{"a":"1","b":"2"}}                                  │
        //  4. │  3 │ {"boolean":true}                                              │
        //  5. │  4 │ {}                                                            │
        //  6. │  5 │ {"date":"2023-01-01"}                                         │
        //  7. │  6 │ {"datetime":"2023-01-01T12:00:00Z"}                           │
        //  8. │  7 │ {"array":{"haha":true}}                                       │
        //  9. │  8 │ {"complex":{"nested":{"array":["1","2","3"],"value":"test"}}} │
        // 10. │  9 │ {}                                                            │
        // 11. │ 10 │ {"empty_array":[]}                                            │
        // 12. │ 11 │ {"mixed_types":["1","string","true",null]}                    │
        // 13. │ 12 │ {"uuid":"bb679be2-e161-4e5e-b09b-d66f3ed12464"}               │
        //     └────┴───────────────────────────────────────────────────────────────┘

        let expected_paths = &[
            ["key"].as_slice(),
            ["array"].as_slice(),
            ["nested.a", "nested.b"].as_slice(),
            ["boolean"].as_slice(),
            [].as_slice(),
            ["date"].as_slice(),
            ["datetime"].as_slice(),
            ["array.haha"].as_slice(),
            ["complex.nested.array", "complex.nested.value"].as_slice(),
            [].as_slice(),
            ["empty_array"].as_slice(),
            ["mixed_types"].as_slice(),
            ["uuid"].as_slice(),
        ];

        let json_marker = &block.markers[1];
        for (i, expected_paths) in expected_paths.iter().copied().enumerate() {
            let mut expected_paths = Vec::from(expected_paths);
            expected_paths.sort_unstable();

            let it: JsonIterator = json_marker.get(i).unwrap().try_into()?;
            let mut actual_paths: Vec<&str> = Vec::new();
            for (path, _value) in it {
                actual_paths.push(path);
            }
            actual_paths.sort_unstable();
            assert_eq!(
                actual_paths, expected_paths,
                "Mismatch at index {i}: expected {:?}, got {:?}",
                expected_paths, actual_paths
            );
        }

        Ok(())
    }

    #[test]
    fn lc_empty_string_bug() -> TestResult {
        let data = load("./testdata/activity_hw.native")?;
        let (rem, block) = parse_single(&data)?;
        assert!(rem.is_empty());

        // ["first_seen", "last_seen", "name", "resource_attrs", "scope_attrs", "attrs", "type", "temporality", "is_monotonic"]
        let marker = &block.markers[3];
        for i in 0..block.num_rows {
            let map_it: MapIterator<&str, &str> = marker.get(i).unwrap().try_into()?;
            for kv in map_it {
                assert!(kv.is_ok(), "empty strings should not be Value::Empty");
            }
        }

        Ok(())
    }

    #[test]
    fn json_arr() -> TestResult {
        let data = load("./testdata/json_arr.native")?;
        let (_, block) = parse_single(&data)?;

        // ┌─id─┬─json_arr──────────────────────────────────────────────────────────────────────────────────┐
        // │  0 │ ['{"key":"value"}','{"array":["1","2","3"]}']                                             │
        // │  1 │ ['{"nested":{"a":"1","b":"2"}}','{"boolean":true}']                                       │
        // │  2 │ ['{}','{"date":"2023-01-01"}']                                                            │
        // │  3 │ ['{"datetime":"2023-01-01T12:00:00Z"}','{"uuid":"c995b14b-ff14-4f4f-8d25-8eb934785e90"}'] │
        // └────┴───────────────────────────────────────────────────────────────────────────────────────────┘

        let json_arr_marker = &block.markers[1];
        assert_eq!(block.num_rows, 4, "Expected 4 rows in json_arr");

        let expected: [&[_]; 4] = [
            &["key", "array"],
            &["nested.a", "nested.b", "boolean"],
            &["date"],
            &["datetime", "uuid"],
        ];

        for (row_idx, exp_paths) in expected.iter().enumerate() {
            let slice: JsonSliceIterator = json_arr_marker.get(row_idx).unwrap().try_into()?;

            let mut actual_paths: Vec<&str> = Vec::new();
            for mut json_it in slice {
                for (path, _value) in &mut json_it {
                    actual_paths.push(path);
                }
            }

            actual_paths.sort_unstable();
            let mut expected_paths = exp_paths.to_vec();
            expected_paths.sort_unstable();

            assert_eq!(
                actual_paths, expected_paths,
                "Mismatch in row {row_idx}: expected {:?}, got {:?}",
                expected_paths, actual_paths
            );
        }

        Ok(())
    }

    #[test]
    fn variant_arr() -> TestResult {
        let data = load("./testdata/variant_arr.native")?;
        let (_, block) = parse_single(&data)?;

        // │  0 │ ['string value',12345,[1,2,3],'{"key":"value"}'] │
        // │  1 │ ['another string',1232,[4,5],'{"array":[6,7]}']  │
        // │  2 │ ['more strings',3333,[],'{"nested":{"a":"1"}}']  │
        // │  3 │ ['test json',44,[8,9],'{"boolean":true}']        │

        let variant_marker = &block.markers[1];
        assert_eq!(block.num_rows, 4, "Expected 4 rows in variant_arr");

        for i in 0..block.num_rows {
            let it: VariantSliceIterator = variant_marker.get(i).unwrap().try_into()?;
            for val in it {
                let str_value: Result<&str, _> = val.clone().try_into();
                let int_value: Result<i64, _> = val.clone().try_into();
                let arr_value: Result<&[U64], _> = val.clone().try_into();
                let json_value: Result<JsonIterator, _> = val.try_into();

                // We should have exactly one successful conversion for each row.
                // TODO: check actual values returned
                let total = usize::from(str_value.is_ok())
                    + usize::from(int_value.is_ok())
                    + usize::from(arr_value.is_ok())
                    + usize::from(json_value.is_ok());

                assert_eq!(total, 1, "some types were not parsed");
            }
        }

        Ok(())
    }

    #[test]
    fn dynamic_arr() -> TestResult {
        let data = load("./testdata/dynamic_arr.native")?;
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
            let arr: DynamicSliceIterator = marker.get(0).unwrap().try_into()?;
            let actual: Vec<i64> = arr.map(TryFrom::try_from).collect::<Result<_, _>>()?;
            assert_eq!(actual, [1, 2, 3], "Row 0 mismatch");
        }

        {
            let arr: DynamicSliceIterator = marker.get(1).unwrap().try_into()?;
            let actual: Vec<&str> = arr.map(TryFrom::try_from).collect::<Result<_, _>>()?;
            assert_eq!(actual, ["a", "b", "c"], "Row 1 mismatch");
        }

        {
            let arr: DynamicSliceIterator = marker.get(2).unwrap().try_into()?;
            let actual: Vec<bool> = arr.map(TryFrom::try_from).collect::<Result<_, _>>()?;
            assert_eq!(actual, [true, false, true], "Row 2 mismatch");
        }

        {
            let arr: DynamicSliceIterator = marker.get(3).unwrap().try_into()?;
            let actual: Vec<f64> = arr.map(TryFrom::try_from).collect::<Result<_, _>>()?;
            // 4.56: meh
            let expected = [1.23, 4.5600000000000005, 7.89];
            assert_eq!(actual, expected, "Row 3 mismatch");
        }

        {
            let arr: DynamicSliceIterator = marker.get(4).unwrap().try_into()?;
            let actual: Vec<chrono::NaiveDate> =
                arr.map(TryFrom::try_from).collect::<Result<_, _>>()?;
            let expected = [
                chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                chrono::NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
            ];
            assert_eq!(actual, expected, "Row 4 mismatch");
        }

        {
            let arr: DynamicSliceIterator = marker.get(5).unwrap().try_into()?;
            let actual: Vec<chrono::DateTime<chrono_tz::Tz>> =
                arr.map(TryFrom::try_from).collect::<Result<_, _>>()?;
            let expected = [
                chrono::DateTime::parse_from_rfc3339("2023-01-01T12:00:00+00:00")?
                    .with_timezone(&chrono_tz::UTC),
                chrono::DateTime::parse_from_rfc3339("2023-01-02T12:00:00+00:00")?
                    .with_timezone(&chrono_tz::UTC),
            ];
            assert_eq!(actual, expected, "Row 5 mismatch");
        }

        {
            let mut it: DynamicSliceIterator = marker.get(6).unwrap().try_into()?;
            let json_it: JsonIterator = it.next().unwrap().try_into()?;

            let mut paths: Vec<&str> = json_it.map(|(p, _)| p).collect();
            paths.sort_unstable();
            assert_eq!(paths, ["sample"], "Row 6 mismatch");
        }

        Ok(())
    }
}
