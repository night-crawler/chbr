use super::{Array, ArrayIter, F64, Tuple};
use crate::FromVariant;

pub type Point<'a> = Tuple<(F64<'a>, F64<'a>)>;
pub type Ring<'a> = Array<'a, Point<'a>>;
pub type Polygon<'a> = Array<'a, Ring<'a>>;
pub type MultiPolygon<'a> = Array<'a, Polygon<'a>>;

/// One row of a `Geometry` column, read through [`Variant`](super::Variant) or
/// [`VariantNullable`](super::VariantNullable).
///
/// ClickHouse fixes the discriminator order of `Geometry` (it is not the alphabetical order of a
/// user-declared `Variant(...)`), so this enum is provided instead of being derived per project.
#[expect(
    clippy::large_enum_variant,
    reason = "readers are borrowed views; boxing would allocate per row"
)]
#[derive(FromVariant)]
pub enum Geometry<'a> {
    LineString(ArrayIter<'a, Point<'a>>),
    MultiLineString(ArrayIter<'a, Ring<'a>>),
    MultiPolygon(ArrayIter<'a, Polygon<'a>>),
    #[col(reader = Point<'a>)]
    Point((f64, f64)),
    Polygon(ArrayIter<'a, Ring<'a>>),
    Ring(ArrayIter<'a, Point<'a>>),
    MultiPoint(ArrayIter<'a, Point<'a>>),
}
