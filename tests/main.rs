use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use chbr::parse::block::parse_single;
use chbr::reader::{
    ArrayIter, ColArray, ColI64, ColLcStr, ColMap, ColStr, ColVariant, TryRead as _,
};
use chbr::{FromBlock, FromVariant};
use testresult::TestResult;

struct CountingAlloc;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

static TEST_LOCK: Mutex<()> = Mutex::new(());

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

#[test]
fn derive_alloc() -> TestResult {
    #[derive(FromBlock)]
    struct MapRow<'a> {
        id: ColI64<'a>,
        #[col(name = "arr_map")]
        maps: ColArray<'a, ColMap<'a, ColStr<'a>, ColStr<'a>>>,
    }

    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);

    let buf = fs::read("./testdata/array_map_sample.native")?;
    let (_, block) = parse_single(&buf)?;

    let before = ALLOCATIONS.load(Ordering::Relaxed);

    let mut total_pairs = 0usize;
    for (row_idx, row) in MapRow::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(row_idx)?);
        for map in row.maps {
            total_pairs += map?.count();
        }
    }

    let allocated = ALLOCATIONS.load(Ordering::Relaxed) - before;
    assert_eq!(total_pairs, 11);
    assert_eq!(
        allocated, 0,
        "column resolution and row iteration must not allocate"
    );

    Ok(())
}

#[test]
fn smoke_derive_variant() -> TestResult {
    #[derive(FromVariant)]
    enum Var<'a> {
        Arr(ArrayIter<'a, ColI64<'a>>),
        Num(i64),
        Str(&'a str),
    }

    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);

    let buf = fs::read("./testdata/variant.native")?;
    let (_, block) = parse_single(&buf)?;

    let reader: ColVariant<Var> = ColVariant::try_from(&block.markers[1])?;
    let mut repr = Vec::with_capacity(block.num_rows);
    for i in 0..block.num_rows {
        repr.push(match reader.try_read(i)? {
            Var::Arr(it) => format!("{:?}", it.try_collect_vec()?),
            Var::Num(n) => n.to_string(),
            Var::Str(s) => s.to_owned(),
        });
    }
    assert_eq!(repr, ["1", "a", "[1, 2, 3]", "2", "b", "[4, 5, 6]", "3"]);

    Ok(())
}

#[test]
fn smoke_derive_nested_struct() -> TestResult {
    #[derive(FromBlock)]
    struct Fruit<'a> {
        name: ColLcStr<'a>,
        rank: ColI64<'a>,
    }

    #[derive(FromBlock)]
    struct Row<'a> {
        id: ColI64<'a>,
        arr: ColArray<'a, Fruit<'a>>,
    }

    let _guard = TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);

    let buf = fs::read("./testdata/array_of_tuples.native")?;
    let (_, block) = parse_single(&buf)?;

    let mut ranks = Vec::new();
    for (row_idx, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(row_idx)?);
        for fruit in row.arr {
            let fruit = fruit?;
            assert!(!fruit.name.is_empty());
            ranks.push(fruit.rank);
        }
    }
    assert_eq!(ranks, (1..=11).collect::<Vec<i64>>());

    Ok(())
}
