macro_rules! t {
    ($name:ident) => { Type::$name };
    ($name:ident( $($inner:tt)* )) => { Type::$name($($inner)*) };
}

macro_rules! bt {
    ($name:ident) => { Box::new(Type::$name) };
    ($name:ident( $($inner:tt)* )) => { Box::new(Type::$name($($inner)*)) };
}

macro_rules! define_slice_fns {
    ($( ($mark_type:ident, $ret_type:ty) ),+ $(,)?) => {
        paste::paste! {
            $(
                #[inline]
                pub fn [<get_arr_ $mark_type:lower _slice>](
                    &'a self,
                    index: usize,
                ) -> crate::Result<Option<&'a [$ret_type]>> {

                    let Mark::Array(arr) = self else {
                        return Err(crate::Error::MismatchedType(self.as_str(), "Array"));
                    };

                    let Some((start, end)) = arr.offsets.offset_indices(index)? else {
                        return Ok(None);
                    };

                    match arr.values.as_ref() {
                        Mark::$mark_type(bv) => Ok(Some(&bv[start..end])),
                        Mark::Empty => Ok(Some(&[])),
                        other => Err(crate::Error::MismatchedType(
                            other.as_str(),
                            stringify!($mark_type),
                        )),
                    }
                }
            )+
        }
    };
}

pub(crate) use bt;
pub(crate) use define_slice_fns;
pub(crate) use t;
