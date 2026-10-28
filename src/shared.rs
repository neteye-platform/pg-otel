//! The span queue in Postgres shared memory.
//!
//! One chunk of shared memory holds a small header followed by the data area
//! of the byte ring ([`crate::queue`]). Its size is `pg_otel.queue_size_kb`,
//! which is only known at server start, so the chunk is requested and
//! initialised by hand in the shmem request/startup hooks instead of with
//! pgrx's `pg_shmem_init!` (which supports statically sized types only).
//!
//! # Concurrency
//!
//! * Backends (producers) and the exporter worker (consumer) serialise access
//!   to the ring with one named LWLock. The critical sections only copy bytes
//!   and never call into Postgres, because an ERROR raised (and caught) while
//!   holding an LWLock would leak it: Rust unwinding must not release it again
//!   after Postgres' own error cleanup, which is what [`LockGuard::drop`]
//!   checks for.
//! * The worker publishes its `ProcNumber` so backends can set its latch
//!   without signals; the other header fields are lock-free atomics.

use std::{
    cell::UnsafeCell,
    mem::size_of,
    ptr,
    sync::{
        OnceLock,
        atomic::{AtomicI32, AtomicPtr, AtomicU64, Ordering},
    },
};

use pgrx::{pg_guard, pg_sys};

use crate::{
    config::get_queue_size_kb,
    queue::{Batch, Corrupted, Ring, RingState, should_wake},
};

const SHMEM_NAME: &std::ffi::CStr = c"pg_otel_queue";
const LWLOCK_TRANCHE: &std::ffi::CStr = c"pg_otel_queue";
/// Index of `AddinShmemInitLock` in `MainLWLockArray` (see `lwlocklist.h`);
/// pgrx's own shared memory support relies on the same index.
const ADDIN_SHMEM_INIT_LOCK: usize = 21;
const NO_WORKER: i32 = -1;

#[repr(C)]
struct Header {
    /// Ring positions; only accessed with the queue lock held. The cell is what
    /// makes it legal to mutate this through a shared `&Header`.
    state: UnsafeCell<RingState>,
    /// Size of the data area following the header; never changes.
    capacity: u64,
    /// `ProcNumber` of the exporter worker, or [`NO_WORKER`].
    worker_proc_number: AtomicI32,
    /// Spans discarded because the queue was full (or a span was too large).
    dropped_spans: AtomicU64,
    /// Totals of `pg_otel.collection_timing`.
    timing: TimingCounters,
}

/// Cumulative [`CollectionStats`], one atomic per field.
#[repr(C)]
#[derive(Default)]
struct TimingCounters {
    statements: AtomicU64,
    build_ns: AtomicU64,
    encode_ns: AtomicU64,
    publish_ns: AtomicU64,
}

const HEADER_BYTES: usize = size_of::<Header>();

static HEADER: AtomicPtr<Header> = AtomicPtr::new(ptr::null_mut());

/// This process's pointer to the queue's LWLock. LWLock addresses are looked up
/// per process (`GetNamedLWLockTranche`) instead of being stored in shared
/// memory, where a pointer is only meaningful if every process maps it at the
/// same address.
static QUEUE_LOCK: AtomicPtr<pg_sys::LWLock> = AtomicPtr::new(ptr::null_mut());

/// Hooks that were installed before ours, to chain to. Each is set exactly once,
/// by [`install_hooks`] in `_PG_init` (in the postmaster, before any other
/// process exists), and only read afterwards.
static PREV_SHMEM_REQUEST_HOOK: OnceLock<pg_sys::shmem_request_hook_type> = OnceLock::new();
static PREV_SHMEM_STARTUP_HOOK: OnceLock<pg_sys::shmem_startup_hook_type> = OnceLock::new();

/// Bytes of shared memory needed for a queue of `queue_size_kb` kilobytes.
fn shared_memory_size(queue_size_kb: i32) -> usize {
    HEADER_BYTES + usize::try_from(queue_size_kb).unwrap_or(0) * 1024
}

/// Installs the shared memory hooks. Must be called from `_PG_init`, after the
/// GUCs are defined, while `shared_preload_libraries` is being processed.
pub fn install_hooks() {
    // SAFETY: `shmem_request_hook` is a plain global of the postmaster. It is
    // only written during startup, from this single thread (`_PG_init`).
    let prev_request = unsafe { pg_sys::shmem_request_hook };
    // SAFETY: `shmem_startup_hook` is likewise a startup-only global, read on the same single thread.
    let prev_startup = unsafe { pg_sys::shmem_startup_hook };
    assert!(
        PREV_SHMEM_REQUEST_HOOK.set(prev_request).is_ok()
            && PREV_SHMEM_STARTUP_HOOK.set(prev_startup).is_ok(),
        "shared memory hooks installed twice"
    );
    // SAFETY: same single-threaded startup context. Chaining to the previous
    // hook is what the hook protocol asks for:
    // https://www.postgresql.org/docs/current/xfunc-c.html#XFUNC-SHARED-ADDIN
    unsafe { pg_sys::shmem_request_hook = Some(on_shmem_request) };
    // SAFETY: still the single-threaded `_PG_init` context, so nothing reads the hook concurrently.
    unsafe { pg_sys::shmem_startup_hook = Some(on_shmem_startup) };
}

/// # Safety
///
/// Must only be called by Postgres as `shmem_request_hook`, in the postmaster
/// while `shared_preload_libraries` is processed.
#[pg_guard]
unsafe extern "C-unwind" fn on_shmem_request() {
    // `PREV_*` is set before this hook can be called (see `install_hooks`).
    if let Some(Some(prev)) = PREV_SHMEM_REQUEST_HOOK.get().copied() {
        // SAFETY: the previous hook is called in the same context as ours.
        unsafe { prev() };
    }
    // SAFETY: allowed (and only valid) inside the shmem request hook:
    // https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/storage/ipc/ipci.c
    unsafe { pg_sys::RequestAddinShmemSpace(shared_memory_size(get_queue_size_kb())) };
    // SAFETY: still inside the shmem request hook. The name is a static
    // NUL-terminated string; Postgres copies it.
    unsafe { pg_sys::RequestNamedLWLockTranche(LWLOCK_TRANCHE.as_ptr(), 1) };
}

/// # Safety
///
/// Must only be called by Postgres as `shmem_startup_hook`, in the postmaster
/// (or in each backend with `EXEC_BACKEND`) after shared memory exists.
#[pg_guard]
unsafe extern "C-unwind" fn on_shmem_startup() {
    // `PREV_*` is set before this hook can be called (see `install_hooks`).
    if let Some(Some(prev)) = PREV_SHMEM_STARTUP_HOOK.get().copied() {
        // SAFETY: the previous hook is called in the same context as ours.
        unsafe { prev() };
    }
    // SAFETY: we are inside the shmem startup hook.
    unsafe { init_shared_header() };
}

/// # Safety
///
/// Must be called from the shmem startup hook, after `on_shmem_request` ran.
unsafe fn init_shared_header() {
    let size = shared_memory_size(get_queue_size_kb());
    // SAFETY: plain read of a global; `MainLWLockArray` is allocated before the
    // startup hook runs.
    let array = unsafe { pg_sys::MainLWLockArray };
    // SAFETY: `ADDIN_SHMEM_INIT_LOCK` is within the array (lwlocklist.h).
    let padded = unsafe { array.add(ADDIN_SHMEM_INIT_LOCK) };
    // SAFETY: `padded` points to a valid `LWLockPadded`; only a raw field
    // address is formed, no reference.
    let init_lock = unsafe { &raw mut (*padded).lock };
    // SAFETY: `ShmemInitStruct` must be called with AddinShmemInitLock held;
    // this takes it. The lock pointer is valid (above):
    // https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/storage/ipc/shmem.c
    unsafe { pg_sys::LWLockAcquire(init_lock, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let mut found = false;
    // SAFETY: lock held (above); `found` is a valid `bool` to write. Returns
    // `size` bytes, MAXALIGN-aligned, valid until server shutdown (it raises an
    // ERROR instead of returning null when out of memory).
    let base = unsafe { pg_sys::ShmemInitStruct(SHMEM_NAME.as_ptr(), size, &raw mut found) };
    let header = base.cast::<Header>();
    assert!(!header.is_null(), "ShmemInitStruct returned null");
    assert!(header.is_aligned(), "shared memory is not aligned");
    if !found {
        // SAFETY: first initialisation, with the init lock held, so no other
        // process can have a reference into the region yet. The region is
        // `HEADER_BYTES + capacity` bytes long and suitably aligned (asserted
        // above), and this is the only writer.
        unsafe {
            header.write(Header {
                state: UnsafeCell::new(RingState::new()),
                capacity: (size - HEADER_BYTES) as u64,
                worker_proc_number: AtomicI32::new(NO_WORKER),
                dropped_spans: AtomicU64::new(0),
                timing: TimingCounters::default(),
            });
        }
        // SAFETY: the data area directly follows the header inside the
        // allocation (`size` bytes in total).
        let data = unsafe { base.cast::<u8>().add(HEADER_BYTES) };
        // SAFETY: `size - HEADER_BYTES` bytes at `data` lie in the allocation.
        // Zeroing makes every byte initialised, which `with_ring` relies on
        // when it forms a `&mut [u8]` over the data area. Cost: touches (and so
        // commits) the whole queue, up to 1 GiB at the maximum `queue_size_kb`,
        // once at server start while holding AddinShmemInitLock.
        unsafe { ptr::write_bytes(data, 0, size - HEADER_BYTES) };
    }
    // `HEADER` and `QUEUE_LOCK` are per-process statics. They are set here in
    // the postmaster, and backends and workers inherit them (and the mapping at
    // the same address) through `fork`; the header was written under
    // AddinShmemInitLock before that. The Release/Acquire ordering is therefore
    // not what makes this correct; it only keeps the statics well-ordered if
    // this hook ever runs in a process that other threads read from.
    HEADER.store(header, Ordering::Release);

    // SAFETY: the tranche was requested in `on_shmem_request`; Postgres sets up
    // the named tranches before the startup hook. The result points into
    // `MainLWLockArray` and is only used as a lock address.
    let tranche = unsafe { pg_sys::GetNamedLWLockTranche(LWLOCK_TRANCHE.as_ptr()) };
    // SAFETY: `tranche` points to the first of our (one) `LWLockPadded`.
    QUEUE_LOCK.store(unsafe { &raw mut (*tranche).lock }, Ordering::Release);

    // SAFETY: acquired above by this process and not yet released.
    unsafe { pg_sys::LWLockRelease(init_lock) };
}

fn header() -> Option<&'static Header> {
    // SAFETY: `HEADER` is null or set once, in the startup hook, to a
    // `Header` that was fully initialised before the (release) store.
    //  * non-null, aligned (asserted at init), initialised, correct type;
    //  * the shared memory segment lives until the server shuts down, which
    //    outlives any use of the reference ('static);
    //  * nothing mutates the pointee while the `&Header` exists: `capacity` is
    //    never written after init, `worker_proc_number`, `dropped_spans` and
    //    `timing` are atomics and `state` is an `UnsafeCell`, so interior mutation by other
    //    processes is permitted;
    //  * the reference only covers `HEADER_BYTES`; the data area behind it is
    //    reached through the raw pointer instead (see `with_ring`).
    // https://doc.rust-lang.org/std/ptr/index.html#pointer-to-reference-conversion
    unsafe { HEADER.load(Ordering::Acquire).as_ref() }
}

/// Holds the queue's LWLock.
struct LockGuard {
    lock: *mut pg_sys::LWLock,
}

impl LockGuard {
    /// Takes the queue lock, or `None` if shared memory was never set up.
    fn acquire() -> Option<Self> {
        let lock = QUEUE_LOCK.load(Ordering::Acquire);
        if lock.is_null() {
            return None;
        }
        // SAFETY: `lock` was stored by `init_shared_header` and points to this
        // extension's LWLock in shared memory, valid until shutdown. Callers are
        // backends or the worker, which have a PGPROC (required to take LWLocks):
        // https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/storage/lmgr/lwlock.c
        unsafe { pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE) };
        Some(Self { lock })
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // After an ERROR, Postgres' abort cleanup (`LWLockReleaseAll`) already
        // released every LWLock this process held; releasing again would fail
        // ("lock is not held"). `LWLockHeldByMe` consults the same per-process
        // list that cleanup empties, so it tells exactly whether we still own it.
        // SAFETY: `lock` is the valid lock pointer given to `acquire`;
        // `LWLockHeldByMe` only reads this process's own held-lock list.
        let held = unsafe { pg_sys::LWLockHeldByMe(self.lock) };
        if held {
            // SAFETY: held by this process (checked above), so releasing is valid.
            unsafe { pg_sys::LWLockRelease(self.lock) };
        }
    }
}

/// Runs `f` on the ring with the queue lock held.
///
/// `f` must not call into Postgres (see the module documentation). Returns
/// `None` when shared memory was never set up (the extension is not preloaded).
pub fn with_ring<R>(f: impl FnOnce(&mut Ring) -> R) -> Option<R> {
    let header = header()?;
    let _guard = LockGuard::acquire()?;
    // The data area is addressed through the raw pointer returned by
    // `ShmemInitStruct`, which (unlike a reference to the header) has
    // provenance over the whole allocation.
    let base = HEADER.load(Ordering::Acquire);
    // SAFETY: the allocation is `HEADER_BYTES + capacity` bytes, so the data
    // area starts inside it.
    let data_ptr = unsafe { base.cast::<u8>().add(HEADER_BYTES) };
    // SAFETY:
    //  * non-null, aligned (u8), `capacity` bytes, zeroed at init, so initialised;
    //  * valid for the lifetime of the reference: shared memory lives until
    //    shutdown, and the slice is dropped (with `ring`) before `_guard`;
    //  * exclusive: every access to the data area, in any process, happens
    //    through this function, which holds the queue LWLock exclusively
    //    until `_guard` drops after the last use of the slice;
    //  * provenance: `data_ptr` derives from the whole-allocation pointer, and
    //    the slice does not overlap the `Header` (it starts after it);
    //  * `capacity` fits in `isize` (it is at most `queue_size_kb` KiB).
    let data = unsafe { std::slice::from_raw_parts_mut(data_ptr, header.capacity as usize) };
    // SAFETY: `state` is only ever accessed here, under
    // the exclusive queue lock and until `_guard` drops, so no other reference
    // (in this or another process) exists. The `UnsafeCell` makes mutation
    // through the shared `&Header` legal. Aligned (offset 0 of a repr(C),
    // 8-aligned struct), initialised, and the pointer has provenance over
    // exactly `size_of::<RingState>()` bytes.
    let state = unsafe { &mut *header.state.get() };
    let mut ring = Ring::new(state, data);
    Some(f(&mut ring))
}

/// Result of [`publish`].
#[derive(Debug, PartialEq, Eq)]
pub enum Published {
    /// The batch is queued.
    Queued,
    /// The batch was discarded (queue full, or no shared memory).
    Dropped,
}

/// Queues `batch` as a whole or drops it, counting the dropped spans, and wakes
/// the worker when that is useful (see [`should_wake`]).
///
/// Call without holding any lock; `skipped` is the number of spans that could
/// not even be encoded into `batch`.
pub fn publish(batch: &Batch, skipped: usize) -> Published {
    count_dropped(skipped);
    if batch.is_empty() {
        return Published::Queued;
    }
    let pushed = with_ring(|ring| {
        let used_before = ring.used();
        ring.push_batch(batch)
            .ok()
            .map(|()| should_wake(used_before, ring.used(), ring.capacity()))
    });
    match pushed {
        Some(Some(wake)) => {
            if wake {
                wake_worker();
            }
            Published::Queued
        }
        Some(None) | None => {
            count_dropped(batch.records());
            Published::Dropped
        }
    }
}

fn count_dropped(spans: usize) {
    if let Some(header) = header().filter(|_| spans > 0) {
        header
            .dropped_spans
            .fetch_add(spans as u64, Ordering::Relaxed);
    }
}

/// Total number of spans dropped since server start.
pub fn dropped_spans() -> u64 {
    header().map_or(0, |header| header.dropped_spans.load(Ordering::Relaxed))
}

/// Time spent collecting the spans of a statement, in nanoseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CollectionTiming {
    /// Building the span records.
    pub build_ns: u64,
    /// Encoding them for the queue.
    pub encode_ns: u64,
    /// Taking the queue lock and copying the batch in.
    pub publish_ns: u64,
}

/// What `pg_otel.collection_timing` has measured since server start.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CollectionStats {
    /// Statements measured.
    pub statements: u64,
    pub total: CollectionTiming,
}

/// Adds the timing of one statement to the totals.
pub fn record_collection_timing(timing: CollectionTiming) {
    if let Some(header) = header() {
        let counters = &header.timing;
        counters.statements.fetch_add(1, Ordering::Relaxed);
        counters
            .build_ns
            .fetch_add(timing.build_ns, Ordering::Relaxed);
        counters
            .encode_ns
            .fetch_add(timing.encode_ns, Ordering::Relaxed);
        counters
            .publish_ns
            .fetch_add(timing.publish_ns, Ordering::Relaxed);
    }
}

/// The totals measured so far. The fields are read one by one, so a statement
/// recorded concurrently can show up in some of them only.
pub fn collection_stats() -> CollectionStats {
    header().map_or_else(CollectionStats::default, |header| {
        let counters = &header.timing;
        CollectionStats {
            statements: counters.statements.load(Ordering::Relaxed),
            total: CollectionTiming {
                build_ns: counters.build_ns.load(Ordering::Relaxed),
                encode_ns: counters.encode_ns.load(Ordering::Relaxed),
                publish_ns: counters.publish_ns.load(Ordering::Relaxed),
            },
        }
    })
}

/// Moves up to `max_bytes` of whole records into `out`; see
/// [`Ring::drain_into`]. Returns the number of records.
pub fn drain_into(out: &mut Vec<u8>, max_bytes: usize) -> Result<usize, Corrupted> {
    with_ring(|ring| ring.drain_into(out, max_bytes)).unwrap_or(Ok(0))
}

/// Records the calling process as the exporter worker, so backends can wake
/// it, and arranges for the registration to be removed when it exits (also
/// after an ERROR or FATAL, via `before_shmem_exit`).
pub fn register_worker() {
    let Some(header) = header() else { return };
    // SAFETY: plain read of a global that is set once during process start-up
    // (`InitProcess`) and valid for any process attached to shared memory.
    let proc_number = unsafe { pg_sys::MyProcNumber };
    header
        .worker_proc_number
        .store(proc_number, Ordering::Release);
    // SAFETY: the callback is a plain `extern "C-unwind"` function that ignores
    // its argument. Registration is valid in a process attached to shared memory.
    unsafe {
        pg_sys::before_shmem_exit(Some(unregister_worker_on_exit), pg_sys::Datum::from(0));
    }
}

/// # Safety
///
/// Must only be called by Postgres as a `before_shmem_exit` callback; it
/// touches nothing but the atomics in the shared header.
unsafe extern "C-unwind" fn unregister_worker_on_exit(_code: i32, _arg: pg_sys::Datum) {
    if let Some(header) = header() {
        header
            .worker_proc_number
            .store(NO_WORKER, Ordering::Release);
    }
}

/// Sets the worker's latch if a worker is registered.
///
/// A stale `ProcNumber` is harmless: setting the latch of a process slot that
/// is no longer (or newly) used only causes a spurious wake-up.
pub fn wake_worker() {
    let Some(header) = header() else { return };
    let proc_number = header.worker_proc_number.load(Ordering::Acquire);
    // SAFETY: `ProcGlobal` is a plain global, set in the postmaster
    // (`InitProcGlobal`) before any other process exists.
    let globals = unsafe { pg_sys::ProcGlobal };
    if proc_number < 0 || globals.is_null() {
        return;
    }
    // SAFETY: `globals` is non-null and points to the shared `PROC_HDR`; a
    // by-value read of a field, no reference is formed.
    let proc_count = unsafe { (*globals).allProcCount };
    if proc_number as u32 >= proc_count {
        return;
    }
    // SAFETY: field read through the valid `globals`; no reference is formed.
    let all_procs = unsafe { (*globals).allProcs };
    // SAFETY: `allProcs` points to an array of `allProcCount` PGPROCs, and
    // `proc_number` is below that count:
    // https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/storage/lmgr/proc.c
    let proc = unsafe { all_procs.add(proc_number as usize) };
    // SAFETY: `proc` is a valid PGPROC (above); only a raw field address is formed.
    let latch = unsafe { &raw mut (*proc).procLatch };
    // SAFETY: `procLatch` is a shared latch initialised for every PGPROC at
    // startup; `SetLatch` may be called by any process on a shared latch, and
    // does nothing for a slot without an owner:
    // https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/storage/ipc/latch.c
    unsafe { pg_sys::SetLatch(latch) };
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn memory_size_includes_the_header() {
        assert_eq!(shared_memory_size(0), HEADER_BYTES);
        assert_eq!(shared_memory_size(64), HEADER_BYTES + 64 * 1024);
        assert_eq!(shared_memory_size(-5), HEADER_BYTES);
    }

    #[test]
    fn header_keeps_the_data_area_aligned() {
        assert_eq!(HEADER_BYTES % 8, 0);
    }
}

// The module must be called `tests`: pgrx looks `#[pg_test]` functions up in
// that schema.
#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ffi::CStr;

    use pgrx::{pg_sys, prelude::*};

    use super::ADDIN_SHMEM_INIT_LOCK;

    #[pg_test]
    fn addin_shmem_init_lock_index_names_the_expected_lock() {
        // SAFETY: plain read of a global, initialised in every backend.
        let array = unsafe { pg_sys::MainLWLockArray };
        // SAFETY: the index is within the array's fixed part.
        let padded = unsafe { array.add(ADDIN_SHMEM_INIT_LOCK) };
        // SAFETY: `padded` is valid; only a raw field address is formed.
        let lock = unsafe { &raw const (*padded).lock };
        // SAFETY: `lock` is valid for reads; plain field read.
        let tranche = unsafe { (*lock).tranche };
        // SAFETY: returns a NUL-terminated string that Postgres owns.
        let identifier = unsafe { pg_sys::GetLWLockIdentifier(pg_sys::PG_WAIT_LWLOCK, tranche) };
        // SAFETY: valid NUL-terminated static string (above), only read here.
        let name = unsafe { CStr::from_ptr(identifier) };
        assert_eq!(name.to_str().unwrap(), "AddinShmemInit");
    }

    #[pg_test]
    fn queue_lock_is_resolved_for_this_process() {
        assert!(!super::QUEUE_LOCK.load(super::Ordering::Acquire).is_null());
        assert!(super::with_ring(|ring| ring.capacity()).is_some_and(|c| c >= 64 * 1024));
    }
}
