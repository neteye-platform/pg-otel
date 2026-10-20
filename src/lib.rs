use std::{sync::OnceLock, time::Duration};

use pgrx::{
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags},
    prelude::*,
};

use crate::{
    config::ExporterConfig,
    postgres::{collect_spans, complete_instrumentation_request, request_instrumentation},
    worker::background_worker_run,
};

mod codec;
mod compat;
mod config;
#[cfg(any(test, feature = "pg_test"))]
mod fuzz;
mod planning;
mod postgres;
mod queue;
mod sanitize;
mod shared;
mod span;
mod worker;

::pgrx::pg_module_magic!(name, version);

/// Name of the exporter background worker as shown in `pg_stat_activity`.
const WORKER_NAME: &str = "pg_otel exporter";

/// Postgres restarts the worker this long after it crashed or exited.
const WORKER_RESTART_TIME: Duration = Duration::from_secs(10);

/// The `ExecutorStart_hook` / `ExecutorEnd_hook` that were installed before
/// ours. Hooks form a chain: each one must call its predecessor (or the
/// standard function when there is none), or other extensions would break.
/// Set exactly once, in `_PG_init` (single-threaded, in the postmaster, before
/// any hook can run), and only read afterwards, hence no `static mut`.
/// See <https://www.postgresql.org/docs/current/xfunc-c.html>.
static PREV_EXECUTOR_START: OnceLock<pg_sys::ExecutorStart_hook_type> = OnceLock::new();
static PREV_EXECUTOR_END: OnceLock<pg_sys::ExecutorEnd_hook_type> = OnceLock::new();

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // SAFETY: plain read of a `bool` global that the postmaster sets while it
    // loads `shared_preload_libraries`; `_PG_init` runs on that single thread,
    // so nothing writes it concurrently. It is false when the library is loaded
    // any other way (`LOAD`, `CREATE EXTENSION`, session_preload_libraries):
    // <https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/init/miscinit.c>
    let preloading = unsafe { pg_sys::process_shared_preload_libraries_in_progress };
    if !preloading {
        pgrx::error!("this extension must be loaded via shared_preload_libraries.");
    }
    ExporterConfig::define_gucs();
    reserve_guc_prefix();

    shared::install_hooks();
    compat::install_planner_hook();
    planning::register_callbacks();

    BackgroundWorkerBuilder::new(WORKER_NAME)
        .set_function("background_worker_main")
        .set_library("pg_otel")
        .set_restart_time(Some(WORKER_RESTART_TIME))
        .load();

    // SAFETY: `_PG_init` runs once per process, single-threaded, before any
    // query executes, so nothing reads or writes the hook globals concurrently.
    let prev_start = unsafe { pg_sys::ExecutorStart_hook };
    // SAFETY: same single-threaded `_PG_init` context for this hook global.
    let prev_end = unsafe { pg_sys::ExecutorEnd_hook };
    assert!(
        PREV_EXECUTOR_START.set(prev_start).is_ok() && PREV_EXECUTOR_END.set(prev_end).is_ok(),
        "executor hooks installed twice"
    );
    // SAFETY: same single-threaded context; the hook has the signature
    // `ExecutorStart_hook_type` (see executor.h).
    unsafe { pg_sys::ExecutorStart_hook = Some(my_executor_start_hook) };
    // SAFETY: same single-threaded context; the hook has the signature
    // `ExecutorEnd_hook_type` (see executor.h).
    unsafe { pg_sys::ExecutorEnd_hook = Some(my_executor_end_hook) };
}

/// Rejects unknown `pg_otel.*` settings (typos in `postgresql.conf`) instead of
/// silently accepting them as placeholders.
fn reserve_guc_prefix() {
    // SAFETY: the argument is a static NUL-terminated string (Postgres copies
    // it). Called from `_PG_init` after all `pg_otel.*` GUCs are defined, as
    // required: <https://www.postgresql.org/docs/current/xfunc-c.html>
    unsafe { pg_sys::MarkGUCPrefixReserved(c"pg_otel".as_ptr()) };
}

// `no_mangle`: the postmaster resolves the worker entry point by name
// (`bgw_function_name` = "background_worker_main", `bgw_library_name` =
// "pg_otel") with `load_external_function()`, i.e. `dlsym`, so the symbol must
// keep its exact name:
// <https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/postmaster/bgworker.c>
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_main(_arg: pg_sys::Datum) {
    // These are the signals we want to receive. Without the SIGTERM handler we
    // would never be able to exit via an external notification. Backends wake
    // the worker through its latch, not with signals.
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Publish this process so backends can set its latch; the registration is
    // removed again when the process exits, whatever the reason.
    shared::register_worker();

    background_worker_run();

    log!("{} is shutting down", BackgroundWorker::get_name());

    // Exit with a non-zero status: the postmaster then restarts the worker
    // after `WORKER_RESTART_TIME` (a zero status would mean "do not restart",
    // so `pg_terminate_backend()` on the worker would silently end exporting
    // until the next server start). During a server shutdown the postmaster
    // does not restart workers; it only logs the exit code. `proc_exit` runs
    // the exit callbacks that unregister the worker.
    // SAFETY: `proc_exit` never returns; it runs the registered exit callbacks.
    // Called from the worker's main function at the end of its life, which is
    // where a background worker is supposed to exit:
    // <https://www.postgresql.org/docs/current/bgworker.html>
    unsafe { pg_sys::proc_exit(1) };
}

/// Number of spans dropped since server start because the span queue was full.
#[pg_extern]
fn pg_otel_dropped_spans() -> i64 {
    i64::try_from(shared::dropped_spans()).unwrap_or(i64::MAX)
}

/// Totals of `pg_otel.collection_timing` since server start: the statements
/// measured and the time spent building, encoding and queueing their spans, in
/// nanoseconds.
#[pg_extern]
fn pg_otel_collection_stats() -> TableIterator<
    'static,
    (
        name!(statements, i64),
        name!(build_ns, i64),
        name!(encode_ns, i64),
        name!(publish_ns, i64),
    ),
> {
    let to_i64 = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
    let stats = shared::collection_stats();
    TableIterator::once((
        to_i64(stats.statements),
        to_i64(stats.total.build_ns),
        to_i64(stats.total.encode_ns),
        to_i64(stats.total.publish_ns),
    ))
}

/// # Safety
///
/// Must only be called by Postgres as `ExecutorStart_hook`: `query_desc` points
/// to a valid, exclusively owned `QueryDesc` (executor.h).
#[pg_guard]
unsafe extern "C-unwind" fn my_executor_start_hook(
    query_desc: *mut pg_sys::QueryDesc,
    eflags: i32,
) {
    // Does nothing when tracing is disabled, in parallel workers, or for
    // EXPLAIN without ANALYZE.
    // SAFETY: `query_desc` is the valid `QueryDesc` Postgres passed to this
    // hook; ExecutorStart has not run yet, which is the phase this needs.
    unsafe { request_instrumentation(query_desc, eflags) };

    // `PREV_EXECUTOR_START` is set in `_PG_init`, before this hook is installed.
    if let Some(Some(prev)) = PREV_EXECUTOR_START.get().copied() {
        // SAFETY: chained hook, called with the arguments our caller (Postgres)
        // gave us, per the hook contract. See executor.h, `ExecutorStart_hook`.
        unsafe { prev(query_desc, eflags) };
    } else {
        // SAFETY: `query_desc` and `eflags` come straight from Postgres, as for
        // the chained hook; this is what Postgres itself calls with no hook.
        unsafe { pg_sys::standard_ExecutorStart(query_desc, eflags) };
    }

    // PG18 can only allocate the statement timer once the executor state
    // exists; a no-op on PG19.
    // SAFETY: `query_desc` is still the valid `QueryDesc` passed to this hook,
    // and ExecutorStart has just returned (or this is the PG19 no-op).
    unsafe { complete_instrumentation_request(query_desc, eflags) };
}

/// # Safety
///
/// Must only be called by Postgres as `ExecutorEnd_hook`: `query_desc` points
/// to a valid `QueryDesc` that was started (executor.h).
#[pg_guard]
unsafe extern "C-unwind" fn my_executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
    // Never raises: telemetry failures are contained so the previous / standard
    // ExecutorEnd below always runs. Parallel workers are skipped inside.
    // SAFETY: `query_desc` is the valid, started `QueryDesc` Postgres passed to
    // this hook; ExecutorEnd has not run yet, so its plan state is still intact.
    unsafe { collect_spans(query_desc) };

    // `PREV_EXECUTOR_END` is set in `_PG_init`, before this hook is installed.
    if let Some(Some(prev)) = PREV_EXECUTOR_END.get().copied() {
        // SAFETY: chained hook, called with the argument Postgres gave us, per
        // the hook contract. See executor.h, `ExecutorEnd_hook`.
        unsafe { prev(query_desc) };
    } else {
        // SAFETY: `query_desc` comes straight from Postgres, as for the chained
        // hook; this is what Postgres itself calls with no hook.
        unsafe { pg_sys::standard_ExecutorEnd(query_desc) };
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // `_PG_init` refuses to run unless the library is preloaded.
        // pg_stat_statements comes *after* pg_otel on purpose: it needs
        // statement-level buffer and WAL usage that pg_otel's own
        // instrumentation request must not take away (see the test
        // `pg_stat_statements_still_sees_buffer_usage`).
        vec!["shared_preload_libraries = 'pg_otel,pg_stat_statements'"]
    }
}
