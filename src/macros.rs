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
                pub fn [<get_arr_ $mark_type:lower _slice>](
                    &self,
                    index: usize,
                ) -> crate::Result<Option<&'a [$ret_type]>> {
                    let Some((values, range)) = self.array_elements(index)? else {
                        return Ok(None);
                    };
                    match values {
                        Mark::$mark_type(bv) => Ok(Some(&bv.as_slice()[range])),
                        Mark::Empty => Ok(Some(&[])),
                        other => {
                            cold_path();
                            Err(crate::Error::MismatchedType(
                                other.as_str(),
                                stringify!($mark_type),
                            ))
                        }
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

                #[inline(always)]
                pub fn [<get_ $ret_type:lower>](&self, index: usize) -> crate::Result<Option<$ret_type>> {
                    match self {
                        Mark::$mark_variant(bv) => {
                            Ok(bv.get(index).copied().map($transform))
                        }
                        _ => {
                            cold_path();
                            Err(crate::Error::MismatchedType(self.as_str(), stringify!($ret_type)))
                        }
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

                #[inline(always)]
                pub(crate) fn [<get_ $mark_variant:lower>](&self, index: usize)
                    -> crate::Result<Option<$ret_type>>
                {
                    match self {
                        Mark::$mark_variant(bv) => Ok(bv.get(index).copied().map(Into::into)),
                        _ => {
                            cold_path();
                            Err(crate::Error::MismatchedType(self.as_str(), stringify!($mark_variant)))
                        }
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
                /// Outer `None`: index out of range. Inner `None`: NULL.
                #[inline(always)]
                pub fn [<get_opt_ $suffix:lower>](&self, index: usize) -> crate::Result<Option<Option<$ret_type>>> {
                    let Mark::Nullable(nullable) = self else {
                        // convenience wrapper for non-nullable columns
                        return match self.[<get_ $suffix:lower>](index)? {
                            Some(value) => Ok(Some(Some(value))),
                            None => Ok(None),
                        };
                    };

                    match nullable.is_null(index) {
                        None => Ok(None),
                        Some(true) => Ok(Some(None)),
                        Some(false) => match nullable.data.[<get_ $suffix:lower>](index)? {
                            Some(value) => Ok(Some(Some(value))),
                            None => Ok(None),
                        },
                    }
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
