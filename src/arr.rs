use std::alloc::{Layout, alloc, dealloc, handle_alloc_error, realloc};
use std::hint::cold_path;
use std::ptr::NonNull;

const ALIGN: usize = align_of::<u64>();

const PAD: usize = size_of::<u64>() - 1;

const LIMITS: [u64; 4] = [u8::MAX as u64, u16::MAX as u64, u32::MAX as u64, u64::MAX];

pub struct VarArray {
    data: NonNull<u8>,
    len: usize,
    limit: u64,
    shift: u8,
}

unsafe impl Send for VarArray {}
unsafe impl Sync for VarArray {}

impl VarArray {
    pub const fn empty(elem_size: u8) -> Self {
        let shift = shift_of(elem_size);

        Self {
            data: NonNull::dangling(),
            len: 0,
            limit: LIMITS[shift as usize],
            shift,
        }
    }

    pub fn uninit(len: usize, elem_size: u8) -> Self {
        if len == 0 {
            cold_path();
            return Self::empty(elem_size);
        }

        let shift = shift_of(elem_size);
        let layout = layout(len, shift);

        let data = unsafe { alloc(layout) };
        let Some(data) = NonNull::new(data) else {
            cold_path();
            handle_alloc_error(layout);
        };

        Self {
            data,
            len,
            limit: LIMITS[shift as usize],
            shift,
        }
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn set(&mut self, index: usize, value: u64) {
        debug_assert!(index < self.len);

        if value > self.limit {
            cold_path();
            self.widen(value, index);
        }

        unsafe {
            self.data
                .as_ptr()
                .add(index << self.shift)
                .cast::<u64>()
                .write_unaligned(value.to_le());
        }
    }

    #[inline(always)]
    pub fn get(&self, index: usize) -> u64 {
        debug_assert!(index < self.len);

        let raw = unsafe {
            self.data
                .as_ptr()
                .add(index << self.shift)
                .cast::<u64>()
                .read_unaligned()
        };

        u64::from_le(raw) & self.limit
    }

    #[cold]
    #[inline(never)]
    fn widen(&mut self, value: u64, written: usize) {
        let new_shift = want_shift(value);
        debug_assert!(new_shift > self.shift);

        let old_shift = self.shift;
        self.shift = new_shift;
        self.limit = LIMITS[new_shift as usize];

        if self.len == 0 {
            return;
        }

        let old_layout = layout(self.len, old_shift);
        let new_layout = layout(self.len, new_shift);

        let ptr = unsafe { realloc(self.data.as_ptr(), old_layout, new_layout.size()) };

        self.data = match NonNull::new(ptr) {
            Some(ptr) => ptr,
            None => {
                cold_path();
                handle_alloc_error(new_layout);
            }
        };

        macro_rules! widen {
            ($src:ty => $dst:ty) => {{
                let src = self.data.cast::<$src>().as_ptr();
                let dst = self.data.cast::<$dst>().as_ptr();

                for i in (0..written).rev() {
                    unsafe {
                        let curr = <$dst>::from(<$src>::from_le(src.add(i).read()));
                        dst.add(i).write(curr.to_le());
                    }
                }
            }};
        }

        match (1u8 << old_shift, 1u8 << new_shift) {
            (1, 2) => widen!(u8 => u16),
            (1, 4) => widen!(u8 => u32),
            (1, 8) => widen!(u8 => u64),
            (2, 4) => widen!(u16 => u32),
            (2, 8) => widen!(u16 => u64),
            (4, 8) => widen!(u32 => u64),
            _ => unreachable!(),
        }
    }
}

impl Drop for VarArray {
    fn drop(&mut self) {
        if self.len == 0 {
            return;
        }

        unsafe { dealloc(self.data.as_ptr(), layout(self.len, self.shift)) }
    }
}

fn layout(len: usize, shift: u8) -> Layout {
    let size = (len << shift)
        .checked_add(PAD)
        .expect("can't park here mate");

    Layout::from_size_align(size, ALIGN).expect("can't park here mate")
}

const fn shift_of(elem_size: u8) -> u8 {
    match elem_size {
        1 => 0,
        2 => 1,
        4 => 2,
        8 => 3,
        _ => panic!("elem_size must be 1, 2, 4, or 8"),
    }
}

const fn want_shift(val: u64) -> u8 {
    if val <= u8::MAX as u64 {
        0
    } else if val <= u16::MAX as u64 {
        1
    } else if val <= u32::MAX as u64 {
        2
    } else {
        3
    }
}
