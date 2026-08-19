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

macro_rules! define_int_getters {
    ($( ($mark_variant:ident, $ret_type:ty, $transform:expr) ),+ $(,)?) => {
        paste::paste! {
            $(
                #[inline]
                pub fn [<get_ $ret_type:lower>](&'a self, index: usize) -> crate::Result<Option<$ret_type>> {
                    match self {
                        Mark::$mark_variant(bv) => {
                            Ok(bv.get(index).copied().map($transform))
                        }
                        _ => Err(crate::Error::MismatchedType(self.as_str(), stringify!($ret_type))),
                    }
                }
            )+
        }
    };
}

macro_rules! define_ip_getters {
    ($( ($mark_variant:ident, $ret_type:ty) ),+ $(,)?) => {
        paste::paste! {
            $(
                #[inline]
                pub fn [<get_ $mark_variant:lower>](&'a self, index: usize)
                    -> crate::Result<Option<$ret_type>>
                {
                    match self {
                        Mark::$mark_variant(bv) => Ok(bv.get(index).copied().map(Into::into)),
                        _ => Err(crate::Error::MismatchedType(self.as_str(), stringify!($mark_variant))),
                    }
                }
            )+
        }
    };
}

macro_rules! define_opt_getters {
    ($( ($suffix:ident, $ret_type:ty) ),+ $(,)?) => {
        paste::paste! {
            $(
                #[inline]
                pub fn [<get_opt_ $suffix:lower>](&'a self, index: usize) -> crate::Result<Option<Option<$ret_type>>> {
                    let Mark::Nullable(Nullable { mask, data }) = self else {
                        let value = self.[<get_ $suffix:lower>](index)?;
                        return Ok(Some(value));
                    };

                    if mask.get(index) == Some(&1) {
                        return Ok(Some(None));
                    }

                    let value = data.[<get_ $suffix:lower>](index)?;
                    Ok(Some(value))
                }
            )+
        }
    };
}

pub(crate) use bt;
pub(crate) use define_int_getters;
pub(crate) use define_ip_getters;
pub(crate) use define_opt_getters;
pub(crate) use define_slice_fns;
pub(crate) use t;
