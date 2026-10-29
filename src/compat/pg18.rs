//! PostgreSQL 18.

use pgrx::pg_sys::{self, Instrumentation, PlanState, QueryDesc};

use std::{
    ffi::{c_char, c_int},
    sync::OnceLock,
};

use pgrx::pg_guard;

use super::{NodeInstrument, NodeStats, instrument_options, is_mid_timer};

/// Converts a time in seconds (how PG18 stores accumulated durations) to
/// nanoseconds, rounding to the nearest. Negative and NaN values become 0.
fn seconds_to_ns(seconds: f64) -> i64 {
    if seconds.is_nan() || seconds <= 0.0 {
        return 0;
    }
    // `as` saturates at i64::MAX for huge values.
    (seconds * 1e9).round() as i64
}

/// Requests per-node instrumentation. Must run before `ExecutorStart` creates
/// the plan state. Statement-level timing is requested separately, after
/// `ExecutorStart` (see [`request_instrumentation_after_start`]).
///
/// # Safety
///
/// `query_desc` must point to a valid, aligned `QueryDesc` that no other code
/// accesses during the call.
pub unsafe fn request_instrumentation_before_start(query_desc: *mut QueryDesc) {
    // SAFETY: valid and unaliased per the function contract. This is a raw
    // field read-modify-write; no `&mut QueryDesc` is created, so no
    // reference claims exclusivity over the rest of the struct.
    // `instrument_options` is a plain `int`, see `QueryDesc` in
    // <https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/executor/execdesc.h>
    unsafe { (*query_desc).instrument_options |= instrument_options() };
}

/// Allocates the statement-level instrumentation (`QueryDesc.totaltime`), the
/// way `auto_explain` and `pg_stat_statements` do: after `ExecutorStart`, in
/// the per-query memory context, unless some other hook already did. The
/// executor then times ExecutorRun and ExecutorFinish with it.
///
/// It is allocated with `INSTRUMENT_ALL`, exactly like those extensions do:
/// there is only one `totaltime` per statement, and whoever allocates it first
/// decides what it measures. `pg_stat_statements` reports buffer and WAL usage
/// from it; a timer with only rows and timing would silently zero those numbers
/// when pg_otel is listed before it in `shared_preload_libraries`. (Per-node
/// instrumentation, which is not shared this way, stays rows and timer only.)
///
/// # Safety
///
/// `query_desc` must point to a valid, aligned `QueryDesc` whose
/// `ExecutorStart` has completed and that no other code accesses during the
/// call.
pub unsafe fn request_instrumentation_after_start(query_desc: *mut QueryDesc) {
    // SAFETY: valid per the function contract. Raw field read, no reference.
    let totaltime = unsafe { (*query_desc).totaltime };
    // SAFETY: `query_desc` is valid per the function contract; raw field read.
    let estate = unsafe { (*query_desc).estate };
    if !totaltime.is_null() || estate.is_null() {
        return;
    }
    // SAFETY: `estate` is non-null and was created by `ExecutorStart`
    // (`CreateExecutorState`), so it is a valid `EState`; raw field read.
    let query_context = unsafe { (*estate).es_query_cxt };
    // SAFETY: `CurrentMemoryContext` is a backend-local variable, only
    // accessed by this (single) thread. The previous context is restored below;
    // if `InstrAlloc` raises an ERROR, Postgres' abort handling resets
    // `CurrentMemoryContext`, as for any `MemoryContextSwitchTo` caller.
    let previous_context = unsafe { pg_sys::CurrentMemoryContext };
    // SAFETY: same backend-local variable and thread as the read above.
    unsafe { pg_sys::CurrentMemoryContext = query_context };
    // SAFETY: allocates in the current (per-query) context, so the result lives
    // as long as the `QueryDesc`'s executor state, like the allocations of
    // `pg_stat_statements`. See `InstrAlloc` in
    // <https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/instrument.c>
    let new_totaltime =
        unsafe { pg_sys::InstrAlloc(1, pg_sys::InstrumentOption::INSTRUMENT_ALL as i32, false) };
    // SAFETY: restores the value read above; same backend-local variable.
    unsafe { pg_sys::CurrentMemoryContext = previous_context };
    // SAFETY: valid per the function contract; raw field write.
    unsafe { (*query_desc).totaltime = new_totaltime };
}

/// Nanoseconds the statement spent in ExecutorRun and ExecutorFinish, or
/// `None` if there is no usable statement timer.
///
/// `totaltime` accumulates into `total` only when `InstrEndLoop` runs; it is
/// called here like `pg_stat_statements` does in its `ExecutorEnd`, and it is
/// harmless if several hooks do it.
///
/// # Safety
///
/// `query_desc` must point to a valid `QueryDesc` in `ExecutorEnd`.
pub unsafe fn query_total_ns(query_desc: &QueryDesc) -> Option<i64> {
    let totaltime = query_desc.totaltime;
    // SAFETY: null or a valid `Instrumentation` allocated by `InstrAlloc` in the
    // per-query context, which `FreeExecutorState` releases only after
    // `ExecutorEnd`. Hooks run one after the other on this thread, so nothing
    // mutates it while this shared reference (used for one field read) lives.
    // <https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/executor/execdesc.h>
    let need_timer = unsafe { totaltime.as_ref()? }.need_timer;
    if !need_timer {
        // Allocated by another extension without a timer.
        return None;
    }
    // SAFETY: valid per the check above; the reference above is dead.
    let totaltime = unsafe { finished(totaltime) }?;
    Some(seconds_to_ns(totaltime.total))
}

/// Folds the node's last execution cycle into its totals (as `ExplainNode`
/// does) and returns its measurements. See the PG19 version for the semantics;
/// here `InstrEndLoop` raises an ERROR for the same "still running" condition.
///
/// # Safety
///
/// `plan_node` must be a valid plan state whose instrumentation (null or a
/// valid, aligned `Instrumentation`) no one else accesses during the call.
pub unsafe fn node_instrument(plan_node: &PlanState) -> NodeInstrument {
    // SAFETY: the pointer is null or valid per the function contract.
    let Some(instrument) = (unsafe { finished(plan_node.instrument) }) else {
        return NodeInstrument::Incomplete;
    };
    NodeInstrument::Done(NodeStats {
        startup_ns: seconds_to_ns(instrument.startup),
        total_ns: seconds_to_ns(instrument.total),
        rows: instrument.ntuples,
        secondary_rows: instrument.ntuples2,
        loops: instrument.nloops,
        filtered_by_scan_or_join: instrument.nfiltered1,
        filtered_by_other: instrument.nfiltered2,
    })
}

/// Folds the last cycle into the totals with `InstrEndLoop`, unless the timer
/// is still started (which would raise an ERROR). Returns the instrumentation
/// if its totals are usable, `None` if it is null or interrupted mid-run.
///
/// The returned reference is only valid for as long as the caller's contract
/// on `instrument` holds; callers use it immediately and copy the values out.
///
/// # Safety
///
/// `instrument` must be null or point to a valid, aligned `Instrumentation`
/// (<https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/executor/instrument.h>)
/// that nothing else accesses during the call and while the result is used.
unsafe fn finished<'a>(instrument: *mut Instrumentation) -> Option<&'a Instrumentation> {
    // SAFETY: null or valid per the function contract. The shared reference is
    // dead before `InstrEndLoop` mutates the pointee, so it never overlaps the
    // mutation.
    let before = unsafe { instrument.as_ref()? };
    if is_mid_timer(before.running, before.starttime.ticks) {
        return None;
    }
    // SAFETY: valid per the function contract. `InstrEndLoop` only touches the
    // pointee, is a no-op when the node is not `running`, and (given the
    // mid-timer check above) cannot raise its "still running" ERROR. See
    // <https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/instrument.c>
    unsafe { pg_sys::InstrEndLoop(instrument) };
    // SAFETY: non-null and valid per the function contract; the mutation is finished, so a new
    // shared reference (provenance: `size_of::<Instrumentation>()` bytes, all
    // that is read) sees a consistent value.
    unsafe { instrument.as_ref() }
}

/// This backend's `log_min_messages` setting: a single `int` in PG18.
///
/// # Safety
///
/// Must be called in a Postgres backend (the variable is backend-local).
pub unsafe fn log_min_messages_slot() -> Option<*mut c_int> {
    // Taking the address of a `static mut` is safe; no reference is created.
    // `log_min_messages` is a plain `int` in `utils/guc.h`:
    // <https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/utils/guc.h>
    Some(&raw mut pg_sys::log_min_messages)
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
    // <https://www.postgresql.org/docs/18/xfunc-c.html#XFUNC-C-DYNLOAD>
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
/// (<https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/optimizer/planner.h>).
#[pg_guard]
unsafe extern "C-unwind" fn planner_hook(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: c_int,
    bound_params: pg_sys::ParamListInfo,
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
                let call = || unsafe { prev(parse, query_string, cursor_options, bound_params) };
                // SAFETY: `call` is plain Rust; the boundary converts a
                // longjmp out of `prev` into an unwind.
                unsafe { pg_sys::submodules::ffi::pg_guard_ffi_boundary(call) }
            }
            // SAFETY: same arguments the planner passed to us; errors are
            // handled by `#[pg_guard]` on this function.
            None => unsafe {
                pg_sys::standard_planner(parse, query_string, cursor_options, bound_params)
            },
        }
    })
}

/// The name `EXPLAIN` gives a sub-plan. In PG18 the planner already stores the
/// complete name (`InitPlan 1`, `SubPlan 2`, `CTE x`) in `plan_name`.
pub fn subplan_display_name(subplan: &pg_sys::SubPlan) -> String {
    // SAFETY: `plan_name` is null or a NUL-terminated string in the plan tree,
    // which outlives `subplan`; it is copied by `into_owned` within this call.
    unsafe { crate::postgres::pg_str(subplan.plan_name) }
        .map(std::borrow::Cow::into_owned)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds_convert_to_rounded_nanoseconds() {
        assert_eq!(seconds_to_ns(0.0), 0);
        assert_eq!(seconds_to_ns(1.0), 1_000_000_000);
        assert_eq!(seconds_to_ns(1.5e-6), 1_500);
        assert_eq!(seconds_to_ns(0.050_000_000_4), 50_000_000);
        assert_eq!(seconds_to_ns(0.050_000_000_6), 50_000_001);
    }

    #[test]
    fn invalid_durations_become_zero_and_huge_ones_saturate() {
        assert_eq!(seconds_to_ns(-1.0), 0);
        assert_eq!(seconds_to_ns(f64::NAN), 0);
        assert_eq!(seconds_to_ns(f64::INFINITY), i64::MAX);
        assert_eq!(seconds_to_ns(1e30), i64::MAX);
    }
}
