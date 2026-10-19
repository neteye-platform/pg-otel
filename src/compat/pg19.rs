//! PostgreSQL 19.

use pgrx::pg_sys::{self, NodeInstrumentation, PlanState, QueryDesc};

use std::{
    ffi::{c_char, c_int},
    sync::OnceLock,
};

use pgrx::pg_guard;

use super::{NodeInstrument, NodeStats, instrument_options, is_mid_timer};

/// Fixed-point shift used by Postgres to convert instrumentation ticks to
/// nanoseconds (`TICKS_TO_NS_SHIFT` in `portability/instr_time.h`, a macro and
/// therefore not part of the bindings).
const TICKS_TO_NS_SHIFT: u32 = 14;

/// Converts `instr_time` ticks to nanoseconds with the given scale factors.
///
/// This is a port of the static inline `pg_ticks_to_ns` of
/// `portability/instr_time.h`. Since PG19, `instr_time.ticks` counts raw clock
/// ticks (for example TSC ticks on x86-64), not nanoseconds. A
/// `ticks_per_ns_scaled` of zero means the clock already counts nanoseconds.
/// Large tick counts are scaled in two parts exactly as Postgres does so the
/// multiplication cannot overflow; the arithmetic saturates instead of
/// wrapping should the inputs ever be inconsistent.
fn ticks_to_ns(ticks: i64, ticks_per_ns_scaled: u64, max_ticks_no_overflow: u64) -> i64 {
    if ticks_per_ns_scaled == 0 {
        return ticks;
    }
    let scale = i64::try_from(ticks_per_ns_scaled).unwrap_or(i64::MAX);
    let mut ticks = ticks;
    let mut ns = 0_i64;
    if ticks > i64::try_from(max_ticks_no_overflow).unwrap_or(i64::MAX) {
        let count = ticks >> TICKS_TO_NS_SHIFT;
        ns = count.saturating_mul(scale);
        ticks -= count << TICKS_TO_NS_SHIFT;
    }
    ns.saturating_add(ticks.saturating_mul(scale) >> TICKS_TO_NS_SHIFT)
}

/// Converts `instr_time.ticks` read from Postgres instrumentation to
/// nanoseconds using this backend's timing configuration.
fn instr_ticks_to_ns(ticks: i64) -> i64 {
    // SAFETY: plain read of a process-wide `uint64`, set by
    // `pg_initialize_timing` at backend start and only changed by
    // `pg_set_timing_clock_source` on this thread, never concurrently.
    // <https://github.com/postgres/postgres/blob/REL_19_STABLE/src/include/portability/instr_time.h>
    let scale = unsafe { pg_sys::ticks_per_ns_scaled };
    // SAFETY: same kind of read as `ticks_per_ns_scaled` above (`uint64`
    // set together with it by the timing initialisation).
    let max = unsafe { pg_sys::max_ticks_no_overflow };
    ticks_to_ns(ticks, scale, max)
}

/// Requests statement and per-node instrumentation. Must run before
/// `ExecutorStart` creates the plan state.
///
/// The options are OR-ed in: bits requested by other extensions (for example
/// `INSTRUMENT_ALL` by `pg_stat_statements`, which needs buffer and WAL usage)
/// are never cleared, so the result does not depend on the order of
/// `shared_preload_libraries`.
///
/// # Safety
///
/// `query_desc` must point to a valid, aligned `QueryDesc` that no other code
/// accesses during the call.
pub unsafe fn request_instrumentation_before_start(query_desc: *mut QueryDesc) {
    // SAFETY: valid and unaliased per the function contract. Raw field
    // read-modify-write of a plain `int`; no `&mut QueryDesc` is created, so
    // no reference claims exclusivity over the rest of the struct. Fields:
    // <https://github.com/postgres/postgres/blob/REL_19_STABLE/src/include/executor/execdesc.h>
    unsafe { (*query_desc).query_instr_options |= instrument_options() };
    // SAFETY: `query_desc` is valid and unaliased per the function contract;
    // raw field read-modify-write of a plain `int`.
    unsafe { (*query_desc).instrument_options |= instrument_options() };
}

/// Nothing to do after `ExecutorStart` on PG19 (see the PG18 version).
///
/// # Safety
///
/// None: `query_desc` is not accessed. The function exists, and is `unsafe`,
/// only for signature symmetry with the PG18 version.
pub unsafe fn request_instrumentation_after_start(_query_desc: *mut QueryDesc) {}

/// Nanoseconds the statement spent in ExecutorRun and ExecutorFinish, or
/// `None` if no statement instrumentation was requested.
///
/// # Safety
///
/// `query_desc` must point to a valid `QueryDesc` in `ExecutorEnd`.
pub unsafe fn query_total_ns(query_desc: &QueryDesc) -> Option<i64> {
    // SAFETY: `query_instr` is null or a valid, aligned `Instrumentation`
    // allocated in the per-query context, which is released only after
    // `ExecutorEnd`. Hooks run sequentially on this thread, so nothing mutates
    // it while this shared reference (one field read) lives. In PG19 the
    // statement total is only read, never folded with `InstrEndLoop`:
    // <https://github.com/postgres/postgres/blob/REL_19_STABLE/src/include/executor/execdesc.h>
    let instrument = unsafe { query_desc.query_instr.as_ref()? };
    Some(instr_ticks_to_ns(instrument.total.ticks).max(0))
}

/// Folds the node's last execution cycle into its totals (as `ExplainNode`
/// does) and returns its measurements.
///
/// `InstrEndLoop` is a no-op for a node that is not `running` (never ran, or
/// already folded in), so repeated calls are harmless. It raises an ERROR for a
/// node that is `running` while its timer is still started (execution was
/// interrupted mid-node); that node is reported as
/// [`NodeInstrument::Incomplete`] instead of failing the whole collection.
///
/// # Safety
///
/// `plan_node` must be a valid plan state whose instrumentation (null or a
/// valid, aligned `NodeInstrumentation`) no one else accesses during the call.
pub unsafe fn node_instrument(plan_node: &PlanState) -> NodeInstrument {
    let instrument: *mut NodeInstrumentation = plan_node.instrument;
    // SAFETY: null or valid per the function contract. The shared reference is
    // dead before `InstrEndLoop` mutates the pointee (last use is below), so
    // the two never overlap.
    let Some(before) = (unsafe { instrument.as_ref() }) else {
        return NodeInstrument::Incomplete;
    };
    if is_mid_timer(before.running, before.instr.starttime.ticks) {
        return NodeInstrument::Incomplete;
    }
    // SAFETY: valid per the function contract. `InstrEndLoop` only touches the
    // pointee, is a no-op when the node is not `running`, and (given the
    // mid-timer check above) cannot raise its "still running" ERROR:
    // <https://github.com/postgres/postgres/blob/REL_19_STABLE/src/backend/executor/instrument.c>
    unsafe { pg_sys::InstrEndLoop(instrument) };
    // SAFETY: non-null and valid per the function contract; the mutation is finished, and the
    // reference (provenance: `size_of::<NodeInstrumentation>()` bytes) is
    // dropped before this function returns, as only copies leave it.
    let instrument = unsafe { &*instrument };
    NodeInstrument::Done(NodeStats {
        startup_ns: instr_ticks_to_ns(instrument.startup.ticks).max(0),
        total_ns: instr_ticks_to_ns(instrument.instr.total.ticks).max(0),
        rows: instrument.ntuples,
        secondary_rows: instrument.ntuples2,
        loops: instrument.nloops,
        filtered_by_scan_or_join: instrument.nfiltered1,
        filtered_by_other: instrument.nfiltered2,
    })
}

/// This backend's `log_min_messages` setting.
///
/// PG19 keeps one level per backend type: the C variable is an array declared
/// as `extern int log_min_messages[];` in `utils/guc.h`, which bindgen exposes
/// as a zero-length array. Indexing it directly would be out of bounds for the
/// Rust type, so the slot is reached through raw pointer arithmetic on the
/// array's address. The bounds check against `B_LOGGER` (the last backend type)
/// keeps the offset inside the real array, which has `BACKEND_NUM_TYPES`
/// (`B_LOGGER + 1`) elements:
/// <https://github.com/postgres/postgres/blob/REL_19_STABLE/src/include/miscadmin.h>
/// <https://github.com/postgres/postgres/blob/REL_19_STABLE/src/backend/utils/misc/guc_tables.c>
///
/// The Rust type of the static is `[c_int; 0]`, so the offset is applied with
/// `wrapping_add`, which (unlike `add`) does not require the result to stay
/// inside the zero-sized Rust object. The resulting address is inside the real
/// C array; callers must only read/write it as a plain `int` from this thread.
///
/// # Safety
///
/// Must be called in a Postgres backend (`MyBackendType` is set).
pub unsafe fn log_min_messages_slot() -> Option<*mut c_int> {
    // SAFETY: plain read of a backend-local enum variable, set early in
    // backend startup. Every valid `BackendType` is a non-negative value
    // `<= B_LOGGER`; an out-of-range value fails the check below.
    let backend_type = unsafe { pg_sys::MyBackendType } as usize;
    (backend_type <= pg_sys::BackendType::B_LOGGER as usize).then(|| {
        // Taking the address of a `static mut` is safe and creates no reference.
        let base = &raw mut pg_sys::log_min_messages;
        // `backend_type <= B_LOGGER < BACKEND_NUM_TYPES` (checked above), so
        // the address is an element of the C array. `wrapping_add` is safe and
        // avoids the in-bounds requirement of `add` on the `[c_int; 0]` type.
        base.cast::<c_int>().wrapping_add(backend_type)
    })
}

/// The `planner_hook` that was installed before ours (`None` if there was
/// none). Set exactly once by [`install_planner_hook`] in `_PG_init`, read by
/// the hook afterwards.
static PREV_PLANNER_HOOK: OnceLock<pg_sys::planner_hook_type> = OnceLock::new();

/// Installs the planner hook that times planning (see [`crate::planning`]),
/// chaining to a previously installed hook or `standard_planner`. Call once
/// from `_PG_init`.
///
/// The hook has a different signature on each version: PG19 passes the
/// `ExplainState` of an `EXPLAIN` that is planning (null otherwise).
pub fn install_planner_hook() {
    // SAFETY: plain read of a backend-global hook variable; `_PG_init` is
    // single threaded, so nothing writes it concurrently. Hooks are by
    // convention only changed while a library loads:
    // <https://www.postgresql.org/docs/devel/xfunc-c.html#XFUNC-C-DYNLOAD>
    let previous = unsafe { pg_sys::planner_hook };
    assert!(
        PREV_PLANNER_HOOK.set(previous).is_ok(),
        "install_planner_hook called twice"
    );
    // SAFETY: as for the read above; written after `PREV_PLANNER_HOOK` is set, so
    // the hook can never observe an unset value.
    unsafe { pg_sys::planner_hook = Some(planner_hook) };
}

/// # Safety
///
/// Only called by the planner, with the arguments it passes to `planner_hook`
/// (<https://github.com/postgres/postgres/blob/REL_19_STABLE/src/include/optimizer/planner.h>).
#[pg_guard]
unsafe extern "C-unwind" fn planner_hook(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: c_int,
    bound_params: pg_sys::ParamListInfo,
    explain_state: *mut pg_sys::ExplainState,
) -> *mut pg_sys::PlannedStmt {
    crate::planning::timed(|| {
        // Set in `_PG_init` before this hook was installed; `None` only if the
        // hook is somehow called earlier, which falls back to `standard_planner`.
        match PREV_PLANNER_HOOK.get().copied().flatten() {
            // The previous hook is C code that may longjmp on ERROR: let pgrx
            // turn that into an unwind through our frames.
            Some(prev) => {
                // SAFETY: `prev` was a valid planner hook, called with the
                // arguments the planner gave us.
                let call = || unsafe {
                    prev(
                        parse,
                        query_string,
                        cursor_options,
                        bound_params,
                        explain_state,
                    )
                };
                // SAFETY: `call` is plain Rust; the boundary converts a
                // longjmp out of `prev` into an unwind.
                unsafe { pg_sys::submodules::ffi::pg_guard_ffi_boundary(call) }
            }
            // SAFETY: same arguments the planner passed to us; errors are
            // handled by `#[pg_guard]` on this function.
            None => unsafe {
                pg_sys::standard_planner(
                    parse,
                    query_string,
                    cursor_options,
                    bound_params,
                    explain_state,
                )
            },
        }
    })
}

/// The name `EXPLAIN` gives a sub-plan: `CTE x`, `InitPlan 1` or `SubPlan 2`.
/// In PG19 `plan_name` is the bare name chosen by the planner.
pub fn subplan_display_name(subplan: &pg_sys::SubPlan) -> String {
    // SAFETY: `plan_name` is null or a NUL-terminated string in the plan tree,
    // which outlives `subplan`; it is only used within this call.
    let plan_name = unsafe { crate::postgres::pg_str(subplan.plan_name) };
    cook_subplan_name(
        subplan.subLinkType == pg_sys::SubLinkType::CTE_SUBLINK,
        subplan.isInitPlan,
        plan_name.as_deref().unwrap_or_default(),
    )
}

fn cook_subplan_name(is_cte: bool, is_init_plan: bool, plan_name: &str) -> String {
    let kind = if is_cte {
        "CTE"
    } else if is_init_plan {
        "InitPlan"
    } else {
        "SubPlan"
    };
    format!("{kind} {plan_name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exact reference for the conversion: `ticks * scale / 2^14`, rounded down.
    fn reference_ticks_to_ns(ticks: i64, scale: u64) -> i64 {
        ((i128::from(ticks) * i128::from(scale)) >> TICKS_TO_NS_SHIFT) as i64
    }

    #[test]
    fn ticks_are_nanoseconds_when_scale_is_zero() {
        assert_eq!(ticks_to_ns(123_456, 0, 0), 123_456);
        assert_eq!(ticks_to_ns(i64::MAX, 0, 0), i64::MAX);
    }

    #[test]
    fn converts_tsc_ticks_with_fixed_point_scale() {
        // 3 GHz TSC: scale = (1e6 << 14) / 3_000_000 kHz = 5461 (1/3 ns per tick).
        let scale = (1_000_000_u64 << TICKS_TO_NS_SHIFT) / 3_000_000;
        let max = (i64::MAX as u64) / scale;
        assert_eq!(scale, 5461);
        assert_eq!(ticks_to_ns(0, scale, max), 0);
        assert_eq!(ticks_to_ns(16_384, scale, max), 5_461);
        // One second of ticks is one second up to the fixed-point rounding error.
        let one_second = ticks_to_ns(3_000_000_000, scale, max);
        assert!(
            (999_000_000..=1_000_000_000).contains(&one_second),
            "{one_second}"
        );
    }

    #[test]
    fn matches_exact_arithmetic_including_the_overflow_branch() {
        for &(scale, max_ticks) in &[
            (5461_u64, i64::MAX as u64 / 5461),
            (16_384, i64::MAX as u64 / 16_384),
            (1, i64::MAX as u64),
        ] {
            let mut samples = vec![
                0,
                1,
                16_383,
                16_384,
                16_385,
                max_ticks as i64 - 1,
                max_ticks as i64,
                (max_ticks as i64).saturating_add(1),
                i64::MAX / 2,
            ];
            let mut rng = fastrand::Rng::with_seed(7);
            samples.extend((0..2_000).map(|_| rng.i64(0..=max_ticks as i64)));
            samples.extend((0..2_000).map(|_| rng.i64(0..=i64::MAX)));
            for ticks in samples {
                let expected = reference_ticks_to_ns(ticks, scale);
                assert_eq!(
                    ticks_to_ns(ticks, scale, max_ticks),
                    expected,
                    "ticks={ticks} scale={scale}"
                );
            }
        }
    }

    #[test]
    fn conversion_saturates_instead_of_overflowing() {
        // Inconsistent inputs (max too large for the scale) must not panic.
        assert!(ticks_to_ns(i64::MAX, 1 << 40, u64::MAX) > 0);
        assert!(ticks_to_ns(i64::MAX, u64::MAX, 0) > 0);
    }

    #[test]
    fn subplan_names_follow_explain() {
        assert_eq!(cook_subplan_name(false, true, "1"), "InitPlan 1");
        assert_eq!(cook_subplan_name(false, false, "2"), "SubPlan 2");
        assert_eq!(cook_subplan_name(true, true, "c"), "CTE c");
        assert_eq!(cook_subplan_name(true, false, "c"), "CTE c");
    }
}
