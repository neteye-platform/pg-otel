//! Differential fuzzer: random queries run as `EXPLAIN (ANALYZE, FORMAT JSON)`
//! inside the test Postgres, and the plan it prints is compared with the spans
//! pg_otel builds for the same execution (diverted through [`capture`], so the
//! exporter is not involved).
//!
//! The test does nothing unless asked to. Environment:
//!
//! * `PG_OTEL_FUZZ_SEED` (required when fuzzing): the master seed.
//! * `PG_OTEL_FUZZ_SECONDS` / `PG_OTEL_FUZZ_CASES`: the budget; whichever is
//!   reached first ends the run.
//! * `PG_OTEL_FUZZ_CASE`: first case index to run (default 0). Alone it runs
//!   exactly that case; with `PG_OTEL_FUZZ_CASES=n` it runs `n` cases from there.
//! * `PG_OTEL_FUZZ_TIMEOUT_MS` (default 1000): time limit of a case.
//!
//! Case `i` runs with `case_seed(master, i)`; case 0 uses the master seed
//! itself. Every failure line and the summary print the command that replays
//! it: `PG_OTEL_FUZZ_SEED=<master> PG_OTEL_FUZZ_CASE=<index>`. Replaying the
//! case seed (`PG_OTEL_FUZZ_SEED=<case seed> PG_OTEL_FUZZ_CASES=1`) also works.
//! Lines starting with `FUZZ` are meant to be grepped.
//!
//! A replay regenerates the same settings and query (the generator depends on
//! the seed only), but not necessarily the same database state: earlier cases
//! of a full run leave dead tuples, statistics and bloat behind (their DML is
//! rolled back), so plans, timings and thus timeouts can differ. A mismatch
//! that depends on such state needs the full run to reproduce.
//!
//! A case that times out only with the plugin on is run once more; if that
//! passes it is reported as `FUZZ flaky-timeout` and only fails the run when
//! there are more than [`MAX_FLAKY_TIMEOUTS`] of them.
//!
//! ```text
//! PG_OTEL_FUZZ_SEED=1 PG_OTEL_FUZZ_SECONDS=300 RUST_TEST_NOCAPTURE=1 \
//!     cargo pgrx test pg18 --no-default-features fuzz_explain_vs_spans
//! ```

mod compare;
mod generate;

use std::{
    collections::{BTreeMap, HashMap},
    panic::AssertUnwindSafe,
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError},
    time::{Duration, Instant},
};

use opentelemetry_sdk::trace::SpanData;
use pgrx::{
    pg_sys::{self, panic::CaughtError},
    prelude::*,
};

use crate::postgres::capture;
use compare::compare;
use generate::Case;

/// Flaky timeouts tolerated in one run before it fails.
const MAX_FLAKY_TIMEOUTS: usize = 3;

/// Cancels the running case after a deadline, from a helper thread.
///
/// `SET [LOCAL] statement_timeout` only takes effect for the next top-level
/// statement and the test body is a single one, and pgrx has no bindings for
/// `enable_timeout_after` (whose `TimeoutId` would have to be copied by hand).
/// So a thread sends SIGINT to the backend thread: Postgres' own
/// `StatementCancelHandler` turns it into ERROR 57014 at the next
/// `CHECK_FOR_INTERRUPTS`. The thread calls no Postgres function.
///
/// The signal is only ever sent with `state` locked and the deadline still set,
/// and [`Watchdog::disarm`] clears the deadline under the same lock, so no
/// signal can be sent after `disarm` returns and a late one cannot hit the
/// next case.
struct Watchdog {
    state: Arc<(Mutex<WatchdogState>, Condvar)>,
}

#[derive(Default)]
struct WatchdogState {
    deadline: Option<Instant>,
    /// A signal was sent since the last `arm`.
    fired: bool,
}

impl Watchdog {
    /// The watchdog of the calling (backend) thread.
    fn get() -> &'static Watchdog {
        static WATCHDOG: OnceLock<Watchdog> = OnceLock::new();
        WATCHDOG.get_or_init(|| {
            let state = Arc::new((Mutex::new(WatchdogState::default()), Condvar::new()));
            // SAFETY: `pthread_self` has no preconditions.
            let backend = unsafe { libc::pthread_self() };
            let shared = Arc::clone(&state);
            std::thread::spawn(move || {
                let (lock, condvar) = &*shared;
                let mut state = lock_ignoring_poison(lock);
                loop {
                    state = match state.deadline {
                        None => condvar.wait(state).unwrap_or_else(PoisonError::into_inner),
                        Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
                            Some(left) => {
                                condvar
                                    .wait_timeout(state, left)
                                    .unwrap_or_else(PoisonError::into_inner)
                                    .0
                            }
                            None => {
                                // SAFETY: `backend` is the backend thread, which
                                // outlives this process-long helper thread.
                                unsafe { libc::pthread_kill(backend, libc::SIGINT) };
                                state.deadline = None;
                                state.fired = true;
                                state
                            }
                        },
                    };
                }
            });
            Watchdog { state }
        })
    }

    fn arm(&self, timeout: Duration) {
        let (lock, condvar) = &*self.state;
        *lock_ignoring_poison(lock) = WatchdogState {
            deadline: Some(Instant::now() + timeout),
            fired: false,
        };
        condvar.notify_all();
    }

    /// Stops the countdown. Returns whether a signal was sent since `arm`.
    fn disarm(&self) -> bool {
        let (lock, condvar) = &*self.state;
        let mut state = lock_ignoring_poison(lock);
        state.deadline = None;
        condvar.notify_all();
        state.fired
    }
}

fn lock_ignoring_poison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether `error` (as formatted by [`rolled_back`]) is SQLSTATE 57014.
fn is_canceled(error: &str) -> bool {
    error.starts_with("ERRCODE_QUERY_CANCELED")
}

/// After a signal was sent but the case finished (or failed otherwise) before
/// seeing it, the cancel is still on its way or pending and would hit whatever
/// runs next. Wait for the handler, then let it fire here and discard it.
fn discard_pending_cancel() {
    for _ in 0..1000 {
        // SAFETY: reads a `sig_atomic_t` that signal handlers write.
        if unsafe { std::ptr::read_volatile(&raw const pg_sys::QueryCancelPending) } != 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    PgTryBuilder::new(AssertUnwindSafe(|| {
        pgrx::check_for_interrupts!();
    }))
    .catch_others(|_| ())
    .execute();
}

/// Most distinct mismatch classes printed in the end.
const MAX_REPORTED: usize = 20;

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(GOLDEN);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Seed of case `index` of a run with `master`. Case 0 is the master seed.
fn case_seed(master: u64, index: u64) -> u64 {
    if index == 0 {
        master
    } else {
        splitmix(master ^ index.wrapping_mul(GOLDEN))
    }
}

fn env_number(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a non-negative integer, got {value:?}")),
    )
}

/// The command that reruns only case `index` of the run with `master`.
fn replay_command(master: u64, index: u64) -> String {
    format!(
        "PG_OTEL_FUZZ_SEED={master} PG_OTEL_FUZZ_CASE={index} RUST_TEST_NOCAPTURE=1 \
         cargo pgrx test pg{} --no-default-features fuzz_explain_vs_spans",
        pg_sys::PG_VERSION_NUM / 10000
    )
}

/// Written to the server log, so it carries the session prefix and survives a
/// crash of the backend. `TMSG: ` makes `cargo pgrx test` echo it live.
fn log_line(message: &str) {
    log!("TMSG: FUZZ {message}");
}

/// Like [`log_line`] for text that already starts with `FUZZ` on each line.
fn log_report(report: &str) {
    for line in report.lines() {
        log!("TMSG: {line}");
    }
}

/// Runs `f` in a subtransaction that is always rolled back, and catches any
/// error it raises.
fn rolled_back<T>(f: impl FnOnce() -> T) -> Result<T, String> {
    // SAFETY: the pattern PL/pgSQL and pgrx-bench use for a block with an
    // exception handler: remember the contexts, run, and restore them before
    // rolling the subtransaction back. Plain calls into the transaction
    // machinery of this backend, which is inside a transaction.
    #[allow(clippy::multiple_unsafe_ops_per_block)]
    unsafe {
        let memory_context = pg_sys::CurrentMemoryContext;
        let resource_owner = pg_sys::CurrentResourceOwner;
        pg_sys::BeginInternalSubTransaction(c"pg_otel_fuzz".as_ptr());
        let result = PgTryBuilder::new(AssertUnwindSafe(|| Ok(f())))
            .catch_others(|error| {
                Err(match error {
                    CaughtError::PostgresError(report)
                    | CaughtError::ErrorReport(report)
                    | CaughtError::RustPanic {
                        ereport: report, ..
                    } => format!("{:?}: {}", report.sql_error_code(), report.message()),
                })
            })
            .execute();
        if Watchdog::get().disarm() && !matches!(&result, Err(error) if is_canceled(error)) {
            discard_pending_cancel();
        }
        pg_sys::MemoryContextSwitchTo(memory_context);
        pg_sys::RollbackAndReleaseCurrentSubTransaction();
        pg_sys::MemoryContextSwitchTo(memory_context);
        pg_sys::CurrentResourceOwner = resource_owner;
        result
    }
}

/// What running one case produced.
enum Outcome {
    /// The flag is set when the first attempt timed out with the plugin on and
    /// only the retry compared.
    Compared(compare::Comparison, bool),
    /// The query failed with plugin on and off: a generator problem.
    GeneratorError(String),
    /// Timed out with plugin on and off: a generator problem.
    GeneratorTimeout,
    /// Failed or timed out only with the plugin on.
    PluginOnly(String),
}

/// Wall-clock split of the run, reported at the end to see where time goes.
#[derive(Default)]
struct Timings {
    /// Whole `explain` calls, including the subtransaction.
    explain_on: Duration,
    explain_off: Duration,
    /// The part of `explain` inside the subtransaction (SET LOCALs + EXPLAIN).
    inner: Duration,
    /// Planning + execution time as reported by EXPLAIN itself (successful runs).
    server: Duration,
    generate: Duration,
    compare: Duration,
    /// Cases that errored or timed out with the plugin on (then rerun off).
    failed_cases: Duration,
    /// Slowest cases: (wall time, index, query).
    slowest: Vec<(Duration, u64, String)>,
}

fn explain(
    case: &Case,
    tracing: bool,
    timeout_ms: u64,
    timings: &mut Timings,
) -> Result<(serde_json::Value, Vec<SpanData>), String> {
    let started = Instant::now();
    let mut inner = Duration::ZERO;
    let result = rolled_back(|| {
        let inner_started = Instant::now();
        for (name, value) in &case.settings {
            Spi::run(&format!("SET LOCAL {name} = '{value}'")).unwrap();
        }
        let min_duration = if tracing { 0 } else { -1 };
        Spi::run(&format!(
            "SET LOCAL pg_otel.min_duration_ms = {min_duration}"
        ))
        .unwrap();
        capture::start();
        Watchdog::get().arm(Duration::from_millis(timeout_ms));
        let result = PgTryBuilder::new(AssertUnwindSafe(|| {
            Spi::get_one::<pgrx::Json>(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", case.query))
        }))
        .catch_others(|error| {
            // Disarm and stop capturing before re-raising for `rolled_back`.
            capture::finish();
            error.rethrow()
        })
        .execute();
        Watchdog::get().disarm();
        let spans = capture::finish();
        let document = result
            .expect("EXPLAIN failed")
            .expect("EXPLAIN returned no row")
            .0;
        inner = inner_started.elapsed();
        (document, spans.into_iter().map(SpanData::from).collect())
    });
    let elapsed = started.elapsed();
    if tracing {
        timings.explain_on += elapsed;
    } else {
        timings.explain_off += elapsed;
    }
    timings.inner += inner;
    if let Ok((document, _)) = &result {
        let ms = |key: &str| document[0][key].as_f64().unwrap_or(0.0);
        timings.server +=
            Duration::from_secs_f64((ms("Planning Time") + ms("Execution Time")) / 1e3);
    }
    result
}

fn run_case(case: &Case, timeout_ms: u64, timings: &mut Timings) -> Outcome {
    fn timed_compare(
        timings: &mut Timings,
        (document, spans): &(serde_json::Value, Vec<SpanData>),
    ) -> compare::Comparison {
        let started = Instant::now();
        let comparison = compare(document, spans);
        timings.compare += started.elapsed();
        comparison
    }
    match explain(case, true, timeout_ms, timings) {
        Ok(first) => Outcome::Compared(timed_compare(timings, &first), false),
        Err(on_error) => match explain(case, false, timeout_ms, timings) {
            Err(off_error) if is_canceled(&on_error) && is_canceled(&off_error) => {
                Outcome::GeneratorTimeout
            }
            Err(off_error) => Outcome::GeneratorError(off_error),
            Ok(_) if is_canceled(&on_error) => match explain(case, true, timeout_ms, timings) {
                Ok(retry) => Outcome::Compared(timed_compare(timings, &retry), true),
                Err(retry_error) => {
                    Outcome::PluginOnly(format!("{on_error}; retry: {retry_error}"))
                }
            },
            Ok(_) => Outcome::PluginOnly(on_error),
        },
    }
}

/// A distinct problem found, with the first case that showed it.
struct Finding {
    signature: String,
    count: usize,
    index: u64,
    case_seed: u64,
    query: String,
    settings: String,
    diff: Vec<String>,
}

#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn fuzz_explain_vs_spans() {
        let seconds = env_number("PG_OTEL_FUZZ_SECONDS");
        let cases = env_number("PG_OTEL_FUZZ_CASES");
        if seconds.is_none() && cases.is_none() && env_number("PG_OTEL_FUZZ_CASE").is_none() {
            return;
        }
        let first_case = env_number("PG_OTEL_FUZZ_CASE");
        // `PG_OTEL_FUZZ_CASE` alone replays exactly that case.
        let (cases, first_case) = match (cases, first_case, seconds) {
            (None, Some(_), None) => (Some(1), first_case.unwrap_or(0)),
            (cases, first_case, _) => (cases, first_case.unwrap_or(0)),
        };
        let master = env_number("PG_OTEL_FUZZ_SEED")
            .expect("PG_OTEL_FUZZ_SEED is required when fuzzing (a replay uses the case seed)");
        let timeout_ms = env_number("PG_OTEL_FUZZ_TIMEOUT_MS").unwrap_or(1000);
        let deadline = seconds.map(Duration::from_secs);
        log_line(&format!(
            "start master_seed={master} first_case={first_case} seconds={seconds:?} cases={cases:?} timeout_ms={timeout_ms} pg={}",
            pg_sys::PG_VERSION_NUM
        ));

        Spi::run(generate::SCHEMA).unwrap();
        Spi::run("SET pg_otel.max_plan_spans = 100000").unwrap();

        let started = Instant::now();
        let mut findings: Vec<Finding> = Vec::new();
        let (mut total, mut compared, mut generator_errors, mut generator_timeouts) =
            (0u64, 0u64, 0u64, 0u64);
        let mut coverage: BTreeMap<String, u64> = BTreeMap::new();
        let mut timings = Timings::default();
        let mut error_samples: HashMap<String, (u64, String)> = HashMap::new();

        let mut flaky: Vec<String> = Vec::new();
        for index in first_case.. {
            if cases.is_some_and(|n| index - first_case >= n)
                || deadline.is_some_and(|d| started.elapsed() >= d)
            {
                break;
            }
            let seed = case_seed(master, index);
            let generated = Instant::now();
            let case = Case::generate(seed);
            timings.generate += generated.elapsed();
            // Before running: if the backend dies, this is the last line.
            log_line(&format!(
                "case master_seed={master} index={index} case_seed={seed} settings=[{}] query={}",
                case.settings_text(),
                case.query
            ));
            total += 1;

            let mut found: Vec<(String, Vec<String>)> = Vec::new();
            let case_started = Instant::now();
            let outcome = run_case(&case, timeout_ms, &mut timings);
            let case_time = case_started.elapsed();
            if !matches!(outcome, Outcome::Compared(..)) {
                timings.failed_cases += case_time;
            }
            timings.slowest.push((case_time, index, case.query.clone()));
            timings
                .slowest
                .sort_by_key(|slow| std::cmp::Reverse(slow.0));
            timings.slowest.truncate(5);
            match outcome {
                Outcome::Compared(comparison, was_flaky) => {
                    compared += 1;
                    if was_flaky {
                        let line = format!(
                            "flaky-timeout case={index} seed={seed} master_seed={master} query={} replay: {}",
                            case.query,
                            replay_command(master, index)
                        );
                        log_line(&line);
                        flaky.push(line);
                    }
                    for key in comparison.coverage {
                        *coverage.entry(key).or_default() += 1;
                    }
                    let mut by_signature: BTreeMap<String, Vec<String>> = BTreeMap::new();
                    for mismatch in comparison.mismatches {
                        by_signature
                            .entry(mismatch.signature())
                            .or_default()
                            .push(mismatch.detail);
                    }
                    found.extend(by_signature);
                }
                Outcome::GeneratorError(message) => {
                    generator_errors += 1;
                    error_samples
                        .entry(message)
                        .or_insert((index, case.query.clone()));
                }
                Outcome::GeneratorTimeout => {
                    generator_timeouts += 1;
                    error_samples
                        .entry("timeout with the plugin on and off".to_owned())
                        .or_insert((index, case.query.clone()));
                }
                Outcome::PluginOnly(message) => {
                    found.push((format!("plugin_only_failure: {message}"), vec![message]));
                }
            }
            for (signature, diff) in found {
                log_line(&format!(
                    "MISMATCH index={index} case_seed={seed} signature={signature} replay: {}",
                    replay_command(master, index)
                ));
                match findings.iter_mut().find(|f| f.signature == signature) {
                    Some(finding) => finding.count += 1,
                    None => findings.push(Finding {
                        signature,
                        count: 1,
                        index,
                        case_seed: seed,
                        query: case.query.clone(),
                        settings: case.settings_text(),
                        diff: diff.into_iter().take(3).collect(),
                    }),
                }
            }
        }

        let elapsed = started.elapsed().as_secs_f64();
        let mut report = format!(
            "FUZZ summary master_seed={master} cases={total} compared={compared} generator_errors={generator_errors} \
             generator_timeouts={generator_timeouts} classes={} flaky_timeouts={} elapsed_s={elapsed:.1} cases_per_s={:.2}\n",
            findings.len(),
            flaky.len(),
            total as f64 / elapsed.max(1e-9),
        );
        let secs = |d: Duration| d.as_secs_f64();
        report += &format!(
            "FUZZ timings_s generate={:.2} explain_on={:.2} explain_off={:.2} inner={:.2} server_reported={:.2} compare={:.2} failed_cases={:.2}\n",
            secs(timings.generate),
            secs(timings.explain_on),
            secs(timings.explain_off),
            secs(timings.inner),
            secs(timings.server),
            secs(timings.compare),
            secs(timings.failed_cases),
        );
        for (time, index, query) in &timings.slowest {
            report += &format!(
                "FUZZ slowest {:.3}s index={index} query={query}\n",
                secs(*time)
            );
        }
        report += &format!("FUZZ coverage {coverage:?}\n");
        for (message, (index, query)) in &error_samples {
            report += &format!("FUZZ generator_error index={index} {message} query={query}\n");
        }
        for finding in findings.iter().take(MAX_REPORTED) {
            report += &format!(
                "FUZZ MISMATCH signature=\"{}\" count={} master_seed={master} index={} case_seed={} settings=[{}] query={} replay: {}\n",
                finding.signature,
                finding.count,
                finding.index,
                finding.case_seed,
                finding.settings,
                finding.query,
                replay_command(master, finding.index)
            );
            for line in &finding.diff {
                report += &format!("FUZZ   diff {line}\n");
            }
        }
        for line in &flaky {
            report += &format!("FUZZ {line}\n");
        }
        if findings.len() > MAX_REPORTED {
            report += &format!(
                "FUZZ ... and {} more classes\n",
                findings.len() - MAX_REPORTED
            );
        }
        // Rolled-back DML leaves dead tuples that cannot be vacuumed inside this one
        // transaction, so the tables slowly bloat; report it to judge the drift.
        let sizes = Spi::get_one::<String>(
            "SELECT string_agg(relname || '=' || pg_size_pretty(pg_table_size(oid)), ' ' ORDER BY relname) \
             FROM pg_class WHERE relname IN ('a', 'b', 'c', 'p1', 'p2', 'p3')",
        )
        .ok()
        .flatten()
        .unwrap_or_default();
        report += &format!("FUZZ table_sizes {sizes}\n");
        log_report(&report);
        assert!(
            findings.is_empty(),
            "{} mismatch classes, see the FUZZ lines above",
            findings.len()
        );
        assert!(
            flaky.len() <= MAX_FLAKY_TIMEOUTS,
            "{} plugin-only timeouts that passed on retry (limit {MAX_FLAKY_TIMEOUTS})",
            flaky.len()
        );
    }

    /// Replaying a case seed must give the same case, and the first case of a
    /// run is the master seed itself.
    #[pg_test]
    fn fuzz_cases_are_reproducible() {
        for seed in [0, 1, 42, u64::MAX] {
            assert_eq!(case_seed(seed, 0), seed);
            let (a, b) = (Case::generate(seed), Case::generate(seed));
            assert_eq!((a.query, a.settings), (b.query, b.settings));
        }
        assert_ne!(case_seed(7, 1), case_seed(7, 2));
        assert_ne!(case_seed(7, 1), case_seed(8, 1));
    }
}
