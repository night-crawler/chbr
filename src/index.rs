use crate::marker::Marker;

pub enum IndexableColumn<'a> {
    Stateless(Marker<'a>),
    Stateful { marker: Marker<'a> },
}

impl<'a> From<Marker<'a>> for IndexableColumn<'a> {
    fn from(marker: Marker<'a>) -> Self {
        match marker {
            Marker::Bool(_)
            | Marker::Int8(_)
            | Marker::Int16(_)
            | Marker::Int32(_)
            | Marker::Int64(_)
            | Marker::Int128(_)
            | Marker::Int256(_)
            | Marker::UInt8(_)
            | Marker::UInt16(_)
            | Marker::UInt32(_)
            | Marker::UInt64(_)
            | Marker::UInt128(_)
            | Marker::UInt256(_)
            | Marker::Float32(_)
            | Marker::Float64(_)
            | Marker::BFloat16(_)
            | Marker::Uuid(_)
            | Marker::Decimal32(_, _)
            | Marker::Decimal64(_, _)
            | Marker::Decimal128(_, _)
            | Marker::Decimal256(_, _)
            | Marker::FixedString(_, _)
            | Marker::Ipv4(_)
            | Marker::Ipv6(_)
            | Marker::Date(_)
            | Marker::Date32(_)
            | Marker::DateTime(_, _)
            | Marker::DateTime64(_, _, _)
            | Marker::Enum8(_, _)
            | Marker::Enum16(_, _) => IndexableColumn::Stateless(marker),
            _ => IndexableColumn::Stateful { marker },
        }
    }
}
