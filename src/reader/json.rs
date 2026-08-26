use std::{hint::cold_path, ops::Range};

use super::{Readable, TryRead};
#[cfg(feature = "serde1")]
use crate::types::OffsetIndexPair as _;
use crate::{
    Error,
    mark::{Json as JsonMark, Mark},
    value::Value,
};

#[derive(Clone, Copy)]
pub struct Json<'a>(&'a JsonMark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for Json<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Json(mark) => Ok(Self(mark)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "Json"))
            }
        }
    }
}

impl<'a> TryRead<'a> for Json<'a> {
    type Item = JsonValue<'a>;

    #[inline(always)]
    fn try_read(&self, row: usize) -> crate::Result<Self::Item> {
        if !self.0.contains_row(row) {
            cold_path();
            return Err(Error::IndexOutOfBounds(row, "Json"));
        }
        Ok(JsonValue { mark: self.0, row })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct JsonValue<'a> {
    mark: &'a JsonMark<'a>,
    row: usize,
}

impl<'a> JsonValue<'a> {
    #[inline]
    pub const fn paths(self) -> JsonIterator<'a> {
        JsonIterator {
            mark: self.mark,
            row: self.row,
            path_index: 0,
        }
    }

    #[cfg(feature = "serde1")]
    pub fn deserialize<T>(self) -> Result<T, JsonDeserializeError>
    where
        T: serde::Deserialize<'a>,
    {
        T::deserialize(NodeDeserializer {
            mark: self.mark,
            row: self.row,
            node: self.mark.root(),
        })
    }
}

impl<'a> Readable<'a> for JsonValue<'a> {
    type Reader = Json<'a>;
}

impl<'a> TryFrom<Value<'a>> for JsonValue<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Json { mark, index } => Ok(Self { mark, row: index }),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "JsonValue"))
            }
        }
    }
}

pub struct JsonIterator<'a> {
    mark: &'a JsonMark<'a>,
    row: usize,
    path_index: usize,
}

impl<'a> TryFrom<Value<'a>> for JsonIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match JsonValue::try_from(value) {
            Ok(value) => Ok(value.paths()),
            Err(error) => Err(error),
        }
    }
}

impl<'a> Iterator for JsonIterator<'a> {
    type Item = Result<(&'a str, Value<'a>), Error>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let path_index = self.path_index;
            if path_index >= self.mark.paths.len() {
                return None;
            }
            let path = self.mark.paths[path_index];
            self.path_index += 1;

            match self.mark.value(path_index, self.row) {
                Ok(Some(value)) => break Some(Ok((path, value))),
                Ok(None) => continue,
                Err(error) => break Some(Err(error)),
            }
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.mark.paths.len() - self.path_index;
        (0, Some(remaining))
    }
}

pub struct JsonSliceIterator<'a> {
    mark: &'a JsonMark<'a>,
    range: Range<usize>,
}

impl<'a> TryFrom<Value<'a>> for JsonSliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::JsonSlice { mark, range } => Ok(Self {
                mark,
                range: range.into(),
            }),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "JsonSliceIterator"))
            }
        }
    }
}

impl<'a> Iterator for JsonSliceIterator<'a> {
    type Item = JsonIterator<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let row = self.range.next()?;
        Some(
            JsonValue {
                mark: self.mark,
                row,
            }
            .paths(),
        )
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl ExactSizeIterator for JsonSliceIterator<'_> {}

#[cfg(feature = "serde1")]
#[derive(Debug, thiserror::Error)]
pub enum JsonDeserializeError {
    #[error(transparent)]
    Source(Box<Error>),
    #[error("conflicting scalar and object JSON path at {0:?}")]
    StructuralConflict(String),
    #[error("unsupported ClickHouse JSON value type: {0}")]
    Unsupported(&'static str),
    #[error("{0}")]
    Message(String),
}

#[cfg(feature = "serde1")]
impl From<Error> for JsonDeserializeError {
    fn from(error: Error) -> Self {
        Self::Source(Box::new(error))
    }
}

#[cfg(feature = "serde1")]
impl serde::de::Error for JsonDeserializeError {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::Message(message.to_string())
    }
}

#[cfg(feature = "serde1")]
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

#[cfg(feature = "serde1")]
#[derive(Clone, Copy)]
struct NodeDeserializer<'de> {
    mark: &'de JsonMark<'de>,
    row: usize,
    node: usize,
}

#[cfg(feature = "serde1")]
#[derive(Clone, Copy)]
enum NodeShape<'de> {
    Leaf(CellDeserializer<'de>),
    Object,
}

#[cfg(feature = "serde1")]
impl<'de> NodeDeserializer<'de> {
    fn shape(self) -> Result<NodeShape<'de>, JsonDeserializeError> {
        let leaf_index = self.mark.node_leaf(self.node);
        let mut leaf = None;
        if let Some(path) = leaf_index
            && let Some(header) = self.mark.headers.get(path)
        {
            leaf = Some(CellDeserializer {
                mark: &header.mark,
                row: self.row,
            });
        }
        let leaf_is_active = match leaf {
            Some(cell) => cell.is_present()?,
            None => false,
        };

        let mut child = self.mark.first_child(self.node);
        let mut has_child = false;
        while let Some(index) = child {
            if subtree_is_active(self.mark, self.row, index)? {
                has_child = true;
                break;
            }
            child = self.mark.next_sibling(index);
        }

        match (leaf, leaf_is_active, has_child) {
            (Some(_), true, true) => {
                let path = match leaf_index {
                    Some(index) => match self.mark.paths.get(index) {
                        Some(&path) => path,
                        None => "",
                    },
                    None => "",
                };
                Err(JsonDeserializeError::StructuralConflict(path.to_owned()))
            }
            (Some(leaf), true, false) => Ok(NodeShape::Leaf(leaf)),
            _ => Ok(NodeShape::Object),
        }
    }

    fn leaf(self) -> Result<CellDeserializer<'de>, JsonDeserializeError> {
        match self.shape()? {
            NodeShape::Leaf(cell) => Ok(cell),
            NodeShape::Object => Err(JsonDeserializeError::Unsupported("JSON object enum")),
        }
    }
}

#[cfg(feature = "serde1")]
fn subtree_is_active(
    mark: &JsonMark<'_>,
    row: usize,
    node_index: usize,
) -> Result<bool, JsonDeserializeError> {
    if let Some(path) = mark.node_leaf(node_index)
        && let Some(header) = mark.headers.get(path)
        && (CellDeserializer {
            mark: &header.mark,
            row,
        })
        .is_present()?
    {
        return Ok(true);
    }

    let mut child = mark.first_child(node_index);
    while let Some(index) = child {
        if subtree_is_active(mark, row, index)? {
            return Ok(true);
        }
        child = mark.next_sibling(index);
    }
    Ok(false)
}

#[cfg(feature = "serde1")]
impl<'de> de::Deserializer<'de> for NodeDeserializer<'de> {
    type Error = JsonDeserializeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.shape()? {
            NodeShape::Leaf(cell) => cell.deserialize_any(visitor),
            NodeShape::Object => visitor.visit_map(PathMapAccess {
                mark: self.mark,
                row: self.row,
                next_child: self.mark.first_child(self.node),
                pending: None,
            }),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.leaf()?.deserialize_enum(name, variants, visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.leaf()?.deserialize_bytes(visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.leaf()?.deserialize_byte_buf(visitor)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        unit unit_struct seq tuple tuple_struct map struct identifier ignored_any
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

#[cfg(feature = "serde1")]
struct PathMapAccess<'de> {
    mark: &'de JsonMark<'de>,
    row: usize,
    next_child: Option<usize>,
    pending: Option<usize>,
}

#[cfg(feature = "serde1")]
impl<'de> MapAccess<'de> for PathMapAccess<'de> {
    type Error = JsonDeserializeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        while let Some(index) = self.next_child {
            self.next_child = self.mark.next_sibling(index);
            if !subtree_is_active(self.mark, self.row, index)? {
                continue;
            }
            self.pending = Some(index);
            let key: &'de str = self.mark.node_key(index);
            return match seed.deserialize(de::value::BorrowedStrDeserializer::new(key)) {
                Ok(key) => Ok(Some(key)),
                Err(error) => Err(error),
            };
        }
        Ok(None)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let Some(node) = self.pending.take() else {
            return Err(JsonDeserializeError::Message(
                "JSON map value requested before its key".to_owned(),
            ));
        };
        seed.deserialize(NodeDeserializer {
            mark: self.mark,
            row: self.row,
            node,
        })
    }
}

// ClickHouse FORMAT JSON emits 256-bit values as unquoted JSON numbers. Serde's numeric model
// stops at 128 bits, so preserve their exact decimal representation as strings instead of
// forcing every consumer to enable serde_json's `arbitrary_precision` feature.
#[cfg(feature = "serde1")]
struct WideNumber {
    bytes: [u8; 80],
    len: usize,
}

#[cfg(feature = "serde1")]
impl WideNumber {
    #[inline]
    const fn push(&mut self, byte: u8) {
        self.bytes[self.len] = byte;
        self.len += 1;
    }

    #[inline]
    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len]).expect("wide number formatter emits ASCII")
    }
}

#[cfg(feature = "serde1")]
fn format_wide_number(mut magnitude: [u8; 32], signed: bool, scale: u8) -> WideNumber {
    let negative = signed && magnitude[31] & 0x80 != 0;
    if negative {
        let mut carry = true;
        for byte in &mut magnitude {
            *byte = !*byte;
            if carry {
                let (value, overflow) = byte.overflowing_add(1);
                *byte = value;
                carry = overflow;
            }
        }
    }

    let mut formatted = WideNumber {
        bytes: [0; 80],
        len: 0,
    };
    let mut is_zero = true;
    for byte in &magnitude {
        if *byte != 0 {
            is_zero = false;
            break;
        }
    }
    if is_zero {
        formatted.push(b'0');
        return formatted;
    }

    let mut reversed = [0_u8; 78];
    let mut digit_count = 0;
    loop {
        let mut remainder = 0_u16;
        let mut has_more = false;
        for byte in magnitude.iter_mut().rev() {
            let dividend = (remainder << 8) | u16::from(*byte);
            *byte = u8::try_from(dividend / 10).expect("quotient fits in u8");
            remainder = dividend % 10;
            has_more |= *byte != 0;
        }
        reversed[digit_count] = b'0' + u8::try_from(remainder).expect("remainder is below 10");
        digit_count += 1;
        if !has_more {
            break;
        }
    }

    let mut trimmed = 0;
    let scale = usize::from(scale);
    while trimmed < scale && reversed[trimmed] == b'0' {
        trimmed += 1;
    }
    let scale = scale - trimmed;
    let digits = digit_count - trimmed;
    if negative {
        formatted.push(b'-');
    }
    if scale == 0 {
        for index in (trimmed..digit_count).rev() {
            formatted.push(reversed[index]);
        }
    } else if digits > scale {
        for index in ((trimmed + scale)..digit_count).rev() {
            formatted.push(reversed[index]);
        }
        formatted.push(b'.');
        for index in (trimmed..(trimmed + scale)).rev() {
            formatted.push(reversed[index]);
        }
    } else {
        formatted.push(b'0');
        formatted.push(b'.');
        for _ in 0..(scale - digits) {
            formatted.push(b'0');
        }
        for index in (trimmed..digit_count).rev() {
            formatted.push(reversed[index]);
        }
    }
    formatted
}

#[cfg(feature = "serde1")]
#[derive(Clone, Copy)]
struct CellDeserializer<'de> {
    mark: &'de Mark<'de>,
    row: usize,
}

#[cfg(feature = "serde1")]
#[derive(Clone, Copy)]
enum CellState<'de> {
    Missing,
    Null,
    Present(CellDeserializer<'de>),
}

#[cfg(feature = "serde1")]
impl<'de> CellDeserializer<'de> {
    fn state(self) -> Result<CellState<'de>, JsonDeserializeError> {
        let mut cell = self;
        loop {
            match cell.mark {
                Mark::Empty => return Ok(CellState::Missing),
                Mark::Nullable(nullable) => match nullable.mask.get(cell.row) {
                    Some(1) => return Ok(CellState::Null),
                    Some(_) => {
                        cell.mark = &nullable.data;
                    }
                    None => return Ok(CellState::Missing),
                },
                Mark::LowCardinality(low_cardinality) => {
                    let Some(index) = low_cardinality.indices.get(cell.row)? else {
                        return Ok(CellState::Missing);
                    };
                    if low_cardinality.is_nullable && index == 0 {
                        return Ok(CellState::Null);
                    }
                    let Some(keys) = low_cardinality.additional_keys.as_deref() else {
                        return Ok(CellState::Missing);
                    };
                    cell = Self {
                        mark: keys,
                        row: index,
                    };
                }
                Mark::Variant(variant) => {
                    let Some(&discriminator) = variant.discriminators.get(cell.row) else {
                        return Ok(CellState::Missing);
                    };
                    if discriminator == crate::mark::Variant::NULL_DISCRIMINATOR {
                        return Ok(CellState::Missing);
                    }
                    let Some(&row) = variant.offsets.get(cell.row) else {
                        return Ok(CellState::Missing);
                    };
                    let Some(mark) = variant.types.get(usize::from(discriminator)) else {
                        return Ok(CellState::Missing);
                    };
                    cell = Self { mark, row };
                }
                Mark::Dynamic(dynamic) => {
                    let Some(&discriminator) = dynamic.discriminators.get(cell.row) else {
                        return Ok(CellState::Missing);
                    };
                    let Some(&row) = dynamic.offsets.get(cell.row) else {
                        return Ok(CellState::Missing);
                    };
                    let Some(mark) = dynamic.columns.get(discriminator) else {
                        return Ok(CellState::Missing);
                    };
                    cell = Self { mark, row };
                }
                mark => {
                    return if base_contains(mark, cell.row)? {
                        Ok(CellState::Present(cell))
                    } else {
                        Ok(CellState::Missing)
                    };
                }
            }
        }
    }

    fn is_present(self) -> Result<bool, JsonDeserializeError> {
        Ok(!matches!(self.state()?, CellState::Missing))
    }

    fn borrowed_str(self) -> Result<Option<&'de str>, JsonDeserializeError> {
        let CellState::Present(cell) = self.state()? else {
            return Ok(None);
        };
        let value = match cell.mark {
            Mark::String(strings) => strings.get(cell.row),
            Mark::FixedString(fixed) => fixed.get_str(cell.row),
            Mark::Enum8(enumeration) => enum8_name(enumeration, cell.row),
            Mark::Enum16(enumeration) => enum16_name(enumeration, cell.row),
            _ => None,
        };
        Ok(value)
    }
}

#[cfg(feature = "serde1")]
fn enum8_name<'a>(mark: &'a crate::mark::Enum8<'a>, row: usize) -> Option<&'a str> {
    if row >= mark.data.len() {
        return None;
    }
    let value = mark.data[row];
    match mark.variants.binary_search_by_key(&value, |(_, id)| *id) {
        Ok(index) => Some(mark.variants[index].0),
        Err(_) => None,
    }
}

#[cfg(feature = "serde1")]
fn enum16_name<'a>(mark: &'a crate::mark::Enum16<'a>, row: usize) -> Option<&'a str> {
    if row >= mark.data.len() {
        return None;
    }
    let value = mark.data[row].get();
    match mark.variants.binary_search_by_key(&value, |(_, id)| *id) {
        Ok(index) => Some(mark.variants[index].0),
        Err(_) => None,
    }
}

#[cfg(feature = "serde1")]
fn base_contains(mark: &Mark<'_>, row: usize) -> Result<bool, JsonDeserializeError> {
    let present = match mark {
        Mark::Empty => false,
        Mark::Bool(value) => value.get(row).is_some(),
        Mark::Int8(value) => value.get(row).is_some(),
        Mark::Int16(value) => value.get(row).is_some(),
        Mark::Int32(value) => value.get(row).is_some(),
        Mark::Int64(value) => value.get(row).is_some(),
        Mark::Int128(value) => value.get(row).is_some(),
        Mark::Int256(value) => value.get(row).is_some(),
        Mark::UInt8(value) => value.get(row).is_some(),
        Mark::UInt16(value) => value.get(row).is_some(),
        Mark::UInt32(value) => value.get(row).is_some(),
        Mark::UInt64(value) => value.get(row).is_some(),
        Mark::UInt128(value) => value.get(row).is_some(),
        Mark::UInt256(value) => value.get(row).is_some(),
        Mark::Float32(value) => value.get(row).is_some(),
        Mark::Float64(value) => value.get(row).is_some(),
        Mark::BFloat16(value) => value.get(row).is_some(),
        Mark::Decimal32(value) => value.data.get(row).is_some(),
        Mark::Decimal64(value) => value.data.get(row).is_some(),
        Mark::Decimal128(value) => value.data.get(row).is_some(),
        Mark::Decimal256(value) => value.data.get(row).is_some(),
        Mark::String(value) => value.get(row).is_some(),
        Mark::FixedString(value) => value.get_str(row).is_some(),
        Mark::Uuid(value) => value.get(row).is_some(),
        Mark::Date(value) => value.get(row).is_some(),
        Mark::Date32(value) => value.get(row).is_some(),
        Mark::DateTime(value) => value.data.get(row).is_some(),
        Mark::DateTime64(value) => value.data.get(row).is_some(),
        Mark::Ipv4(value) => value.get(row).is_some(),
        Mark::Ipv6(value) => value.get(row).is_some(),
        Mark::Enum8(value) => enum8_name(value, row).is_some(),
        Mark::Enum16(value) => enum16_name(value, row).is_some(),
        Mark::Array(value) => value.offsets.offset_indices(row)?.is_some(),
        Mark::Tuple(_) | Mark::Map(_) | Mark::NamedTuple(_) => true,
        Mark::Nested(value) => match value.array_of_tuples.as_ref() {
            Mark::Array(array) => array.offsets.offset_indices(row)?.is_some(),
            _ => false,
        },
        Mark::Json(value) => value.contains_row(row),
        Mark::Nullable(_) | Mark::LowCardinality(_) | Mark::Variant(_) | Mark::Dynamic(_) => {
            unreachable!("wrapper marks are resolved before the presence check")
        }
    };
    Ok(present)
}

#[cfg(feature = "serde1")]
impl<'de> de::Deserializer<'de> for CellDeserializer<'de> {
    type Error = JsonDeserializeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let cell = match self.state()? {
            CellState::Missing => {
                return Err(Error::IndexOutOfBounds(self.row, self.mark.as_str()).into());
            }
            CellState::Null => return visitor.visit_unit(),
            CellState::Present(cell) => cell,
        };

        macro_rules! at {
            ($value:expr) => {{
                let Some(value) = $value else {
                    return Err(Error::IndexOutOfBounds(cell.row, cell.mark.as_str()).into());
                };
                value
            }};
        }

        match cell.mark {
            Mark::Empty => unreachable!("empty marks are resolved as missing"),
            Mark::Bool(value) => visitor.visit_bool(at!(value.get(cell.row))),
            Mark::Int8(value) => visitor.visit_i8(*at!(value.get(cell.row))),
            Mark::Int16(value) => visitor.visit_i16(at!(value.get(cell.row)).get()),
            Mark::Int32(value) => visitor.visit_i32(at!(value.get(cell.row)).get()),
            Mark::Int64(value) => visitor.visit_i64(at!(value.get(cell.row)).get()),
            Mark::Int128(value) => visitor.visit_i128(at!(value.get(cell.row)).get()),
            Mark::Int256(value) => {
                let formatted = format_wide_number(at!(value.get(cell.row)).0, true, 0);
                visitor.visit_str(formatted.as_str())
            }
            Mark::UInt8(value) => visitor.visit_u8(*at!(value.get(cell.row))),
            Mark::UInt16(value) => visitor.visit_u16(at!(value.get(cell.row)).get()),
            Mark::UInt32(value) => visitor.visit_u32(at!(value.get(cell.row)).get()),
            Mark::UInt64(value) => visitor.visit_u64(at!(value.get(cell.row)).get()),
            Mark::UInt128(value) => visitor.visit_u128(at!(value.get(cell.row)).get()),
            Mark::UInt256(value) => {
                let formatted = format_wide_number(at!(value.get(cell.row)).0, false, 0);
                visitor.visit_str(formatted.as_str())
            }
            Mark::Float32(value) => visitor.visit_f32(at!(value.get(cell.row)).get()),
            Mark::Float64(value) => visitor.visit_f64(at!(value.get(cell.row)).get()),
            Mark::BFloat16(value) => {
                let value = half::bf16::from(*at!(value.get(cell.row)));
                visitor.visit_f32(value.into())
            }
            Mark::Decimal32(value) => visitor.visit_string(
                at!(value.data.get(cell.row))
                    .with_precision(value.precision)
                    .to_string(),
            ),
            Mark::Decimal64(value) => visitor.visit_string(
                at!(value.data.get(cell.row))
                    .with_precision(value.precision)
                    .to_string(),
            ),
            Mark::Decimal128(value) => visitor.visit_string(
                at!(value.data.get(cell.row))
                    .with_precision(value.precision)?
                    .to_string(),
            ),
            Mark::Decimal256(value) => {
                let formatted =
                    format_wide_number(at!(value.data.get(cell.row)).0.0, true, value.precision);
                visitor.visit_str(formatted.as_str())
            }
            Mark::String(value) => visitor.visit_borrowed_str(at!(value.get(cell.row))),
            Mark::FixedString(value) => visitor.visit_borrowed_str(at!(value.get_str(cell.row))),
            Mark::Uuid(value) => {
                let value = uuid::Uuid::from(*at!(value.get(cell.row)));
                visitor.visit_string(value.to_string())
            }
            Mark::Date(value) => {
                let value = chrono::NaiveDate::from(*at!(value.get(cell.row)));
                visitor.visit_string(value.to_string())
            }
            Mark::Date32(value) => {
                let value = chrono::NaiveDate::from(*at!(value.get(cell.row)));
                visitor.visit_string(value.to_string())
            }
            Mark::DateTime(value) => {
                let value = at!(value.data.get(cell.row)).with_tz(value.tz);
                visitor.visit_string(value.to_rfc3339())
            }
            Mark::DateTime64(value) => {
                let Some(value) =
                    at!(value.data.get(cell.row)).with_tz_and_precision(value.tz, value.precision)
                else {
                    return Err(Error::Overflow("DateTime64 value out of range".to_owned()).into());
                };
                visitor.visit_string(value.to_rfc3339())
            }
            Mark::Ipv4(value) => {
                let value = std::net::Ipv4Addr::from(*at!(value.get(cell.row)));
                visitor.visit_string(value.to_string())
            }
            Mark::Ipv6(value) => {
                let value = std::net::Ipv6Addr::from(*at!(value.get(cell.row)));
                visitor.visit_string(value.to_string())
            }
            Mark::Enum8(_) | Mark::Enum16(_) => {
                let Some(value) = self.borrowed_str()? else {
                    return Err(Error::IndexOutOfBounds(cell.row, cell.mark.as_str()).into());
                };
                visitor.visit_borrowed_str(value)
            }
            Mark::Array(array) => {
                let Some((start, end)) = array.offsets.offset_indices(cell.row)? else {
                    return Err(Error::IndexOutOfBounds(cell.row, "Array").into());
                };
                visitor.visit_seq(ColumnSeqAccess {
                    mark: &array.values,
                    range: start..end,
                })
            }
            Mark::Tuple(tuple) => visitor.visit_seq(TupleSeqAccess {
                tuple,
                row: cell.row,
                next: 0,
            }),
            Mark::Map(map) => visitor.visit_map(ColumnMapAccess::new(map, cell.row)?),
            Mark::Nested(nested) => {
                let Mark::Array(array) = nested.array_of_tuples.as_ref() else {
                    return Err(
                        Error::MismatchedType(nested.array_of_tuples.as_str(), "Array").into(),
                    );
                };
                let Mark::Tuple(tuple) = array.values.as_ref() else {
                    return Err(Error::MismatchedType(array.values.as_str(), "Tuple").into());
                };
                let Some((start, end)) = array.offsets.offset_indices(cell.row)? else {
                    return Err(Error::IndexOutOfBounds(cell.row, "Nested").into());
                };
                visitor.visit_seq(NamedRowsSeqAccess {
                    names: &nested.col_names,
                    tuple,
                    range: start..end,
                })
            }
            Mark::NamedTuple(named) => {
                let Mark::Tuple(tuple) = named.tuple.as_ref() else {
                    return Err(Error::MismatchedType(named.tuple.as_str(), "Tuple").into());
                };
                visitor.visit_map(NamedMapAccess {
                    names: &named.col_names,
                    tuple,
                    row: cell.row,
                    next: 0,
                    pending: None,
                })
            }
            Mark::Json(json) => NodeDeserializer {
                mark: json,
                row: cell.row,
                node: json.root(),
            }
            .deserialize_any(visitor),
            Mark::Nullable(_) | Mark::LowCardinality(_) | Mark::Variant(_) | Mark::Dynamic(_) => {
                unreachable!("wrapper marks are resolved before deserialization")
            }
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.state()? {
            CellState::Missing => Err(Error::IndexOutOfBounds(self.row, self.mark.as_str()).into()),
            CellState::Null => visitor.visit_none(),
            CellState::Present(cell) => visitor.visit_some(cell),
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let Some(value) = self.borrowed_str()? else {
            return Err(JsonDeserializeError::Message(format!(
                "expected enum string, got {}",
                self.mark.as_str()
            )));
        };
        visitor.visit_enum(de::value::BorrowedStrDeserializer::<JsonDeserializeError>::new(value))
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.borrowed_str()? {
            Some(value) => visitor.visit_borrowed_bytes(value.as_bytes()),
            None => self.deserialize_any(visitor),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.borrowed_str()? {
            Some(value) => visitor.visit_byte_buf(value.as_bytes().to_vec()),
            None => self.deserialize_any(visitor),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        unit unit_struct seq tuple tuple_struct map struct identifier ignored_any
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

#[cfg(feature = "serde1")]
struct ColumnSeqAccess<'de> {
    mark: &'de Mark<'de>,
    range: Range<usize>,
}

#[cfg(feature = "serde1")]
impl<'de> SeqAccess<'de> for ColumnSeqAccess<'de> {
    type Error = JsonDeserializeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let Some(row) = self.range.next() else {
            return Ok(None);
        };
        match seed.deserialize(CellDeserializer {
            mark: self.mark,
            row,
        }) {
            Ok(value) => Ok(Some(value)),
            Err(error) => Err(error),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.range.len())
    }
}

#[cfg(feature = "serde1")]
struct TupleSeqAccess<'de> {
    tuple: &'de crate::mark::Tuple<'de>,
    row: usize,
    next: usize,
}

#[cfg(feature = "serde1")]
impl<'de> SeqAccess<'de> for TupleSeqAccess<'de> {
    type Error = JsonDeserializeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let Some(mark) = self.tuple.values.get(self.next) else {
            return Ok(None);
        };
        self.next += 1;
        match seed.deserialize(CellDeserializer {
            mark,
            row: self.row,
        }) {
            Ok(value) => Ok(Some(value)),
            Err(error) => Err(error),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.tuple.values.len() - self.next)
    }
}

#[cfg(feature = "serde1")]
struct ColumnMapAccess<'de> {
    keys: &'de Mark<'de>,
    values: &'de Mark<'de>,
    range: Range<usize>,
    pending: Option<usize>,
}

#[cfg(feature = "serde1")]
impl<'de> ColumnMapAccess<'de> {
    fn new(mark: &'de crate::mark::Map<'de>, row: usize) -> Result<Self, JsonDeserializeError> {
        let Some((start, end)) = mark.offsets.offset_indices(row)? else {
            return Err(Error::IndexOutOfBounds(row, "Map").into());
        };
        Ok(Self {
            keys: &mark.keys,
            values: &mark.values,
            range: start..end,
            pending: None,
        })
    }
}

#[cfg(feature = "serde1")]
impl<'de> MapAccess<'de> for ColumnMapAccess<'de> {
    type Error = JsonDeserializeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        let Some(row) = self.range.next() else {
            return Ok(None);
        };
        self.pending = Some(row);
        match seed.deserialize(CellDeserializer {
            mark: self.keys,
            row,
        }) {
            Ok(value) => Ok(Some(value)),
            Err(error) => Err(error),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let Some(row) = self.pending.take() else {
            return Err(JsonDeserializeError::Message(
                "map value requested before its key".to_owned(),
            ));
        };
        seed.deserialize(CellDeserializer {
            mark: self.values,
            row,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.range.len())
    }
}

#[cfg(feature = "serde1")]
struct NamedMapAccess<'de> {
    names: &'de [&'de str],
    tuple: &'de crate::mark::Tuple<'de>,
    row: usize,
    next: usize,
    pending: Option<usize>,
}

#[cfg(feature = "serde1")]
impl<'de> MapAccess<'de> for NamedMapAccess<'de> {
    type Error = JsonDeserializeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        let Some(&name) = self.names.get(self.next) else {
            return Ok(None);
        };
        if self.tuple.values.get(self.next).is_none() {
            return Err(JsonDeserializeError::Message(
                "named tuple has fewer values than names".to_owned(),
            ));
        }
        self.pending = Some(self.next);
        self.next += 1;
        match seed.deserialize(de::value::BorrowedStrDeserializer::new(name)) {
            Ok(name) => Ok(Some(name)),
            Err(error) => Err(error),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let Some(index) = self.pending.take() else {
            return Err(JsonDeserializeError::Message(
                "tuple value requested before its key".to_owned(),
            ));
        };
        seed.deserialize(CellDeserializer {
            mark: &self.tuple.values[index],
            row: self.row,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.names.len() - self.next)
    }
}

#[cfg(feature = "serde1")]
struct NamedRowsSeqAccess<'de> {
    names: &'de [&'de str],
    tuple: &'de crate::mark::Tuple<'de>,
    range: Range<usize>,
}

#[cfg(feature = "serde1")]
impl<'de> SeqAccess<'de> for NamedRowsSeqAccess<'de> {
    type Error = JsonDeserializeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let Some(row) = self.range.next() else {
            return Ok(None);
        };
        match seed.deserialize(NamedRowDeserializer {
            names: self.names,
            tuple: self.tuple,
            row,
        }) {
            Ok(value) => Ok(Some(value)),
            Err(error) => Err(error),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.range.len())
    }
}

#[cfg(feature = "serde1")]
#[derive(Clone, Copy)]
struct NamedRowDeserializer<'de> {
    names: &'de [&'de str],
    tuple: &'de crate::mark::Tuple<'de>,
    row: usize,
}

#[cfg(feature = "serde1")]
impl<'de> de::Deserializer<'de> for NamedRowDeserializer<'de> {
    type Error = JsonDeserializeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(NamedMapAccess {
            names: self.names,
            tuple: self.tuple,
            row: self.row,
            next: 0,
            pending: None,
        })
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct seq tuple tuple_struct map struct enum identifier ignored_any
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

#[cfg(all(test, feature = "serde1"))]
mod serde_tests {
    use std::{collections::BTreeMap, net::Ipv4Addr};

    use serde::Deserialize;
    use serde_json::json;
    use testresult::TestResult;

    use super::{Json, JsonDeserializeError, format_wide_number};
    use crate::{
        DateTime32Data, Ipv4Data,
        mark::{
            Array as ArrayMark, DateTime, Decimal64, Decimal256, Dynamic as DynamicMark,
            Json as JsonMark, LcIndices, LowCardinality, Map as MapMark, Mark,
            NamedTuple as NamedTupleMark, Nested as NestedMark, Nullable as NullableMark,
            StringView, Tuple as TupleMark, Variant as VariantMark,
        },
        parse::block::parse_single,
        reader::TryRead as _,
        slice::ByteView,
        types::{JsonColumnHeader, Type},
    };

    fn header<'a>(typ: Type<'a>, mark: Mark<'a>) -> JsonColumnHeader<'a> {
        JsonColumnHeader {
            path_version: 0,
            max_types: 0,
            total_types: 1,
            types: vec![typ],
            variant_version: 0,
            is_typed: true,
            type_headers: vec![],
            mark,
        }
    }

    #[test]
    fn deserializes_flat_nested_and_borrowed_structs() -> TestResult {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Flat<'a> {
            #[serde(borrow)]
            key: &'a str,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Nested {
            a: u64,
            b: u64,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Root {
            nested: Nested,
        }

        let data = crate::common::load("./testdata/json.native")?;
        let (_, block) = parse_single(&data)?;
        let reader = Json::try_from(block.mark("json")?)?;

        let flat: Flat<'_> = reader.try_read(0)?.deserialize()?;
        assert_eq!(flat, Flat { key: "value" });
        let input = data.as_ptr_range();
        assert!(input.contains(&flat.key.as_ptr()));

        let nested: Root = reader.try_read(2)?.deserialize()?;
        assert_eq!(
            nested,
            Root {
                nested: Nested { a: 1, b: 2 }
            }
        );
        Ok(())
    }

    #[test]
    fn handles_missing_null_empty_and_row_shape_changes() -> TestResult {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Missing<'a> {
            #[serde(borrow)]
            absent: Option<&'a str>,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Numbers {
            array: Vec<u64>,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Object {
            array: Child,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Child {
            haha: bool,
        }

        let data = crate::common::load("./testdata/json.native")?;
        let (_, block) = parse_single(&data)?;
        let reader = Json::try_from(block.mark("json")?)?;

        assert_eq!(
            reader.try_read(0)?.deserialize::<Missing<'_>>()?,
            Missing { absent: None }
        );
        assert_eq!(
            reader.try_read(4)?.deserialize::<serde_json::Value>()?,
            json!({})
        );
        assert_eq!(
            reader.try_read(9)?.deserialize::<serde_json::Value>()?,
            json!({})
        );
        assert_eq!(
            reader.try_read(1)?.deserialize::<Numbers>()?,
            Numbers {
                array: vec![1, 2, 3]
            }
        );
        assert_eq!(
            reader.try_read(7)?.deserialize::<Object>()?,
            Object {
                array: Child { haha: true }
            }
        );
        assert_eq!(
            reader.try_read(11)?.deserialize::<serde_json::Value>()?,
            json!({"mixed_types": ["1", "string", "true", null]})
        );
        Ok(())
    }

    #[test]
    fn supports_serde_field_attributes() -> TestResult {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Renamed<'a> {
            #[serde(borrow, rename = "key")]
            name: &'a str,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Flattened<'a> {
            #[serde(borrow, flatten)]
            rest: BTreeMap<&'a str, &'a str>,
        }
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Strict {}
        #[derive(Debug, Deserialize, PartialEq)]
        struct Lenient {}

        let data = crate::common::load("./testdata/json.native")?;
        let (_, block) = parse_single(&data)?;
        let reader = Json::try_from(block.mark("json")?)?;

        assert_eq!(
            reader.try_read(0)?.deserialize::<Renamed<'_>>()?,
            Renamed { name: "value" }
        );
        assert_eq!(
            reader.try_read(0)?.deserialize::<Flattened<'_>>()?,
            Flattened {
                rest: BTreeMap::from([("key", "value")])
            }
        );
        assert_eq!(reader.try_read(0)?.deserialize::<Lenient>()?, Lenient {});
        assert!(reader.try_read(0)?.deserialize::<Strict>().is_err());
        Ok(())
    }

    #[test]
    fn rejects_duplicate_and_active_conflicting_paths() -> TestResult {
        let duplicate = JsonMark::new(
            vec!["a", "a"],
            vec![
                header(Type::String, Mark::String(StringView { data: vec!["x"] })),
                header(Type::String, Mark::String(StringView { data: vec!["y"] })),
            ],
            1,
        );
        assert!(matches!(duplicate, Err(crate::Error::CorruptedData(_))));

        let mark = Mark::Json(JsonMark::new(
            vec!["a", "a.b"],
            vec![
                header(Type::String, Mark::String(StringView { data: vec!["x"] })),
                header(Type::String, Mark::String(StringView { data: vec!["y"] })),
            ],
            1,
        )?);
        let error = Json::try_from(&mark)?
            .try_read(0)?
            .deserialize::<serde_json::Value>()
            .expect_err("active scalar and child paths must conflict");
        assert!(matches!(error, JsonDeserializeError::StructuralConflict(path) if path == "a"));
        Ok(())
    }

    #[test]
    fn splits_paths_before_decoding_escaped_dots() -> TestResult {
        let mark = Mark::Json(JsonMark::new(
            vec!["a%2Eb", "nested.value"],
            vec![
                header(Type::String, Mark::String(StringView { data: vec!["dot"] })),
                header(
                    Type::String,
                    Mark::String(StringView {
                        data: vec!["nested"],
                    }),
                ),
            ],
            1,
        )?);
        let actual: serde_json::Value = Json::try_from(&mark)?.try_read(0)?.deserialize()?;
        assert_eq!(actual, json!({"a.b": "dot", "nested": {"value": "nested"}}));
        Ok(())
    }

    #[test]
    fn deserializes_nested_arrays_of_json_rows() -> TestResult {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Child<'a> {
            #[serde(borrow)]
            name: &'a str,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Root<'a> {
            #[serde(borrow)]
            items: Vec<Vec<Child<'a>>>,
        }

        let nested_json = JsonMark::new(
            vec!["name"],
            vec![header(
                Type::String,
                Mark::String(StringView {
                    data: vec!["one", "two", "three"],
                }),
            )],
            3,
        )?;
        let inner_offsets = [1_u64.to_le_bytes(), 3_u64.to_le_bytes()].concat();
        let inner = Mark::Array(ArrayMark {
            offsets: ByteView::try_from(inner_offsets.as_slice())?,
            values: Box::new(Mark::Json(nested_json)),
        });
        let outer_offsets = 2_u64.to_le_bytes();
        let outer = Mark::Array(ArrayMark {
            offsets: ByteView::try_from(outer_offsets.as_slice())?,
            values: Box::new(inner),
        });
        let mark = Mark::Json(JsonMark::new(
            vec!["items"],
            vec![header(Type::Array(Box::new(Type::Json(vec![]))), outer)],
            1,
        )?);

        let actual: Root<'_> = Json::try_from(&mark)?.try_read(0)?.deserialize()?;
        assert_eq!(
            actual,
            Root {
                items: vec![
                    vec![Child { name: "one" }],
                    vec![Child { name: "two" }, Child { name: "three" }]
                ]
            }
        );
        Ok(())
    }

    #[test]
    fn preserves_numeric_boundaries_and_scalar_string_policy() -> TestResult {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Boundaries {
            signed: i64,
            unsigned: u64,
        }

        let signed = i64::MIN.to_le_bytes();
        let unsigned = u64::MAX.to_le_bytes();
        let mark = Mark::Json(JsonMark::new(
            vec!["signed", "unsigned"],
            vec![
                header(
                    Type::Int64,
                    Mark::Int64(ByteView::try_from(signed.as_slice())?),
                ),
                header(
                    Type::UInt64,
                    Mark::UInt64(ByteView::try_from(unsigned.as_slice())?),
                ),
            ],
            1,
        )?);
        assert_eq!(
            Json::try_from(&mark)?
                .try_read(0)?
                .deserialize::<Boundaries>()?,
            Boundaries {
                signed: i64::MIN,
                unsigned: u64::MAX
            }
        );

        let date = chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date_days = u16::try_from((date - epoch).num_days())?.to_le_bytes();
        let address = Ipv4Addr::new(127, 0, 0, 1);
        let address_bytes = u32::from(address).to_le_bytes();
        let uuid = uuid::Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef")?;
        let (hi, lo) = uuid.as_u64_pair();
        let uuid_bytes = [hi.to_le_bytes(), lo.to_le_bytes()].concat();
        let decimal_bytes = 12_345_i64.to_le_bytes();
        let datetime_bytes = 0_u32.to_le_bytes();
        let formatted = Mark::Json(JsonMark::new(
            vec!["date", "ip", "uuid", "decimal", "datetime"],
            vec![
                header(
                    Type::Date,
                    Mark::Date(ByteView::try_from(date_days.as_slice())?),
                ),
                header(
                    Type::Ipv4,
                    Mark::Ipv4(ByteView::<Ipv4Data>::try_from(address_bytes.as_slice())?),
                ),
                header(
                    Type::Uuid,
                    Mark::Uuid(ByteView::try_from(uuid_bytes.as_slice())?),
                ),
                header(
                    Type::Decimal64(2),
                    Mark::Decimal64(Decimal64 {
                        precision: 2,
                        data: ByteView::try_from(decimal_bytes.as_slice())?,
                    }),
                ),
                header(
                    Type::DateTime(chrono_tz::UTC),
                    Mark::DateTime(DateTime {
                        tz: chrono_tz::UTC,
                        data: ByteView::<DateTime32Data>::try_from(datetime_bytes.as_slice())?,
                    }),
                ),
            ],
            1,
        )?);
        let actual: serde_json::Value = Json::try_from(&formatted)?.try_read(0)?.deserialize()?;
        assert_eq!(
            actual,
            json!({
                "date": "2024-02-29",
                "ip": "127.0.0.1",
                "uuid": uuid.to_string(),
                "decimal": "123.45",
                "datetime": "1970-01-01T00:00:00+00:00",
            })
        );
        Ok(())
    }

    #[test]
    fn preserves_wide_numeric_values_as_strings() -> TestResult {
        const I256_MIN: &str =
            "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
        const U256_MAX: &str =
            "115792089237316195423570985008687907853269984665640564039457584007913129639935";

        let mut i256_min = [0_u8; 32];
        i256_min[31] = 0x80;
        let u256_max = [0xff_u8; 32];
        let mut decimal = [0xff_u8; 32];
        decimal[..16].copy_from_slice(&(-123_456_789_i128).to_le_bytes());
        let mut trailing_zeros = [0_u8; 32];
        trailing_zeros[..16].copy_from_slice(&12_000_u128.to_le_bytes());
        let minus_one = [0xff_u8; 32];

        assert_eq!(format_wide_number(i256_min, true, 0).as_str(), I256_MIN);
        assert_eq!(format_wide_number(u256_max, false, 0).as_str(), U256_MAX);
        assert_eq!(format_wide_number(decimal, true, 4).as_str(), "-12345.6789");
        assert_eq!(format_wide_number(trailing_zeros, false, 3).as_str(), "12");
        assert_eq!(format_wide_number(minus_one, true, 3).as_str(), "-0.001");
        assert_eq!(format_wide_number([0_u8; 32], true, 76).as_str(), "0");

        let mark = Mark::Json(JsonMark::new(
            vec!["i", "u", "d"],
            vec![
                header(
                    Type::Int256,
                    Mark::Int256(ByteView::try_from(i256_min.as_slice())?),
                ),
                header(
                    Type::UInt256,
                    Mark::UInt256(ByteView::try_from(u256_max.as_slice())?),
                ),
                header(
                    Type::Decimal256(4),
                    Mark::Decimal256(Decimal256 {
                        precision: 4,
                        data: ByteView::try_from(decimal.as_slice())?,
                    }),
                ),
            ],
            1,
        )?);
        let actual: serde_json::Value = Json::try_from(&mark)?.try_read(0)?.deserialize()?;
        let expected = json!({
            "i": I256_MIN,
            "u": U256_MAX,
            "d": "-12345.6789",
        });
        assert_eq!(actual, expected);

        let array_offsets = 1_u64.to_le_bytes();
        let array_mark = Mark::Json(JsonMark::new(
            vec!["i_array", "u_array", "d_array"],
            vec![
                header(
                    Type::Array(Box::new(Type::Int256)),
                    Mark::Array(ArrayMark {
                        offsets: ByteView::try_from(array_offsets.as_slice())?,
                        values: Box::new(Mark::Int256(ByteView::try_from(i256_min.as_slice())?)),
                    }),
                ),
                header(
                    Type::Array(Box::new(Type::UInt256)),
                    Mark::Array(ArrayMark {
                        offsets: ByteView::try_from(array_offsets.as_slice())?,
                        values: Box::new(Mark::UInt256(ByteView::try_from(u256_max.as_slice())?)),
                    }),
                ),
                header(
                    Type::Array(Box::new(Type::Decimal256(4))),
                    Mark::Array(ArrayMark {
                        offsets: ByteView::try_from(array_offsets.as_slice())?,
                        values: Box::new(Mark::Decimal256(Decimal256 {
                            precision: 4,
                            data: ByteView::try_from(decimal.as_slice())?,
                        })),
                    }),
                ),
            ],
            1,
        )?);
        let arrays: serde_json::Value = Json::try_from(&array_mark)?.try_read(0)?.deserialize()?;
        assert_eq!(arrays["i_array"], json!([I256_MIN]));
        assert_eq!(arrays["u_array"], json!([U256_MAX]));
        assert_eq!(arrays["d_array"], json!(["-12345.6789"]));
        Ok(())
    }

    #[test]
    fn deserializes_composite_marks() -> TestResult {
        let mask_present = [0_u8];
        let mask_null = [1_u8];
        let low_cardinality_indices = [0_u8];
        let variant_discriminators = [0_u8];
        let map_offsets = 1_u64.to_le_bytes();
        let nested_offsets = 2_u64.to_le_bytes();
        let map_value = 7_u64.to_le_bytes();
        let tuple_value = 1_u64.to_le_bytes();
        let named_value = 9_u64.to_le_bytes();

        let marks = vec![
            header(
                Type::String,
                Mark::Nullable(NullableMark {
                    mask: &mask_present,
                    data: Box::new(Mark::String(StringView { data: vec!["some"] })),
                }),
            ),
            header(
                Type::String,
                Mark::Nullable(NullableMark {
                    mask: &mask_null,
                    data: Box::new(Mark::String(StringView {
                        data: vec!["unused"],
                    })),
                }),
            ),
            header(
                Type::String,
                Mark::LowCardinality(LowCardinality {
                    is_nullable: false,
                    indices: LcIndices::U8(&low_cardinality_indices),
                    global_dictionary: None,
                    additional_keys: Some(Box::new(Mark::String(StringView {
                        data: vec!["dict"],
                    }))),
                }),
            ),
            header(
                Type::String,
                Mark::Map(MapMark {
                    offsets: ByteView::try_from(map_offsets.as_slice())?,
                    keys: Box::new(Mark::String(StringView { data: vec!["k"] })),
                    values: Box::new(Mark::UInt64(ByteView::try_from(map_value.as_slice())?)),
                }),
            ),
            header(
                Type::String,
                Mark::Tuple(TupleMark {
                    values: vec![
                        Mark::UInt64(ByteView::try_from(tuple_value.as_slice())?),
                        Mark::String(StringView { data: vec!["two"] }),
                    ],
                }),
            ),
            header(
                Type::String,
                Mark::NamedTuple(NamedTupleMark {
                    col_names: vec!["x"],
                    tuple: Box::new(Mark::Tuple(TupleMark {
                        values: vec![Mark::UInt64(ByteView::try_from(named_value.as_slice())?)],
                    })),
                }),
            ),
            header(
                Type::String,
                Mark::Nested(NestedMark {
                    col_names: vec!["name"],
                    array_of_tuples: Box::new(Mark::Array(ArrayMark {
                        offsets: ByteView::try_from(nested_offsets.as_slice())?,
                        values: Box::new(Mark::Tuple(TupleMark {
                            values: vec![Mark::String(StringView {
                                data: vec!["a", "b"],
                            })],
                        })),
                    })),
                }),
            ),
            header(
                Type::String,
                Mark::Variant(VariantMark {
                    offsets: vec![0],
                    discriminators: &variant_discriminators,
                    types: vec![Mark::String(StringView {
                        data: vec!["variant"],
                    })],
                }),
            ),
            header(
                Type::String,
                Mark::Dynamic(DynamicMark {
                    offsets: vec![0],
                    discriminators: vec![0],
                    columns: vec![Mark::String(StringView {
                        data: vec!["dynamic"],
                    })],
                }),
            ),
        ];
        let mark = Mark::Json(JsonMark::new(
            vec![
                "nullable", "null", "lc", "map", "tuple", "named", "nested", "variant", "dynamic",
            ],
            marks,
            1,
        )?);

        let actual: serde_json::Value = Json::try_from(&mark)?.try_read(0)?.deserialize()?;
        assert_eq!(
            actual,
            json!({
                "nullable": "some",
                "null": null,
                "lc": "dict",
                "map": {"k": 7},
                "tuple": [1, "two"],
                "named": {"x": 9},
                "nested": [{"name": "a"}, {"name": "b"}],
                "variant": "variant",
                "dynamic": "dynamic",
            })
        );
        Ok(())
    }

    #[test]
    fn deserializes_typed_fixture_and_owned_value() -> TestResult {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Nested<'a> {
            #[serde(borrow)]
            name: &'a str,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Typed<'a> {
            a: u64,
            #[serde(borrow)]
            nested: Nested<'a>,
            extra: Option<serde_json::Value>,
        }

        let data = crate::common::load("./testdata/json_typed.native")?;
        let (_, block) = parse_single(&data)?;
        let reader = Json::try_from(block.mark("json")?)?;
        assert_eq!(
            reader.try_read(0)?.deserialize::<Typed<'_>>()?,
            Typed {
                a: 42,
                nested: Nested { name: "alpha" },
                extra: Some(json!(true))
            }
        );
        assert_eq!(
            reader.try_read(2)?.deserialize::<serde_json::Value>()?,
            json!({"a": 0, "nested": {"name": ""}})
        );

        let shared = crate::common::load("./testdata/json_shared.native")?;
        let Err(error) = parse_single(&shared) else {
            panic!("shared data must remain rejected");
        };
        assert!(matches!(
            error,
            crate::Error::NotImplemented(message) if message == "non-empty JSON shared data"
        ));
        Ok(())
    }
}
