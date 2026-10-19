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
//! * `PG_OTEL_FUZZ_TIMEOUT_MS` (default 5000): statement timeout of a case.
//!
//! Case `i` runs with `case_seed(master, i)`; case 0 uses the master seed
//! itself, so `PG_OTEL_FUZZ_SEED=<case seed> PG_OTEL_FUZZ_CASES=1` replays any
//! reported case. Lines starting with `FUZZ` are meant to be grepped.
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
    time::{Duration, Instant},
};

use opentelemetry_sdk::trace::SpanData;
use pgrx::{pg_sys, prelude::*};

use crate::postgres::capture;
use compare::compare;
use generate::Case;

unsafe extern "C" {
    fn enable_timeout_after(id: i32, delay_ms: i32);
    fn disable_timeout(id: i32, keep_indicator: bool);
}

/// `STATEMENT_TIMEOUT` in `TimeoutId` (utils/timeout.h; the same in 18 and 19).
/// Armed by hand because `SET [LOCAL] statement_timeout` only takes effect for
/// the next top-level statement, and the test body is a single one.
const STATEMENT_TIMEOUT: i32 = 3;

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

/// Written to the server log, so it carries the session prefix and survives a
/// crash of the backend. `TMSG: ` makes `cargo pgrx test` echo it live.
fn log_line(message: &str) {
    log!("TMSG: FUZZ {message}");
}

/// Runs `f` in a subtransaction that is always rolled back, and catches any
/// error it raises.
fn rolled_back<T>(f: impl FnOnce() -> T) -> Result<T, String> {
    // SAFETY: the pattern PL/pgSQL and pgrx-bench use for a block with an
    // exception handler: remember the contexts, run, and restore them before
    // rolling the subtransaction back.
    unsafe {
        let memory_context = pg_sys::CurrentMemoryContext;
        let resource_owner = pg_sys::CurrentResourceOwner;
        pg_sys::BeginInternalSubTransaction(c"pg_otel_fuzz".as_ptr());
        let result = PgTryBuilder::new(AssertUnwindSafe(|| Ok(f())))
            .catch_others(|error| {
                Err(match error {
                    pgrx::pg_sys::panic::CaughtError::PostgresError(report)
                    | pgrx::pg_sys::panic::CaughtError::ErrorReport(report)
                    | pgrx::pg_sys::panic::CaughtError::RustPanic { ereport: report, .. } => {
                        format!("{:?}: {}", report.sql_error_code(), report.message())
                    }
                })
            })
            .execute();
        disable_timeout(STATEMENT_TIMEOUT, false);
        pg_sys::MemoryContextSwitchTo(memory_context);
        pg_sys::RollbackAndReleaseCurrentSubTransaction();
        pg_sys::MemoryContextSwitchTo(memory_context);
        pg_sys::CurrentResourceOwner = resource_owner;
        result
    }
}

/// What running one case produced.
enum Outcome {
    Compared(compare::Comparison),
    /// The query failed with plugin on and off: a generator problem.
    GeneratorError(String),
    /// Timed out with plugin on and off: a generator problem.
    GeneratorTimeout,
    /// Failed or timed out only with the plugin on.
    PluginOnly(String),
}

fn explain(case: &Case, tracing: bool, timeout_ms: u64) -> Result<(serde_json::Value, Vec<SpanData>), String> {
    rolled_back(|| {
        for (name, value) in &case.settings {
            Spi::run(&format!("SET LOCAL {name} = '{value}'")).unwrap();
        }
        let min_duration = if tracing { 0 } else { -1 };
        Spi::run(&format!("SET LOCAL pg_otel.min_duration_ms = {min_duration}")).unwrap();
        capture::start();
        // SAFETY: plain call that arms a timer of this backend.
        unsafe { enable_timeout_after(STATEMENT_TIMEOUT, i32::try_from(timeout_ms).unwrap_or(i32::MAX)) };
        let result = PgTryBuilder::new(AssertUnwindSafe(|| {
            Spi::get_one::<pgrx::Json>(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", case.query))
        }))
        .catch_others(|error| {
            // Disarm and stop capturing before re-raising for `rolled_back`.
            capture::finish();
            error.rethrow()
        })
        .execute();
        // SAFETY: as above.
        unsafe { disable_timeout(STATEMENT_TIMEOUT, false) };
        let spans = capture::finish();
        let document = result.expect("EXPLAIN failed").expect("EXPLAIN returned no row").0;
        (document, spans.into_iter().map(SpanData::from).collect())
    })
}

fn run_case(case: &Case, timeout_ms: u64) -> Outcome {
    match explain(case, true, timeout_ms) {
        Ok((document, spans)) => Outcome::Compared(compare(&document, &spans)),
        Err(on_error) => match explain(case, false, timeout_ms) {
            Err(off_error) if on_error.starts_with("ERRCODE_QUERY_CANCELED") && off_error.starts_with("ERRCODE_QUERY_CANCELED") => {
                Outcome::GeneratorTimeout
            }
            Err(off_error) => Outcome::GeneratorError(off_error),
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
        if seconds.is_none() && cases.is_none() {
            return;
        }
        let master = env_number("PG_OTEL_FUZZ_SEED")
            .expect("PG_OTEL_FUZZ_SEED is required when fuzzing (a replay uses the case seed)");
        let timeout_ms = env_number("PG_OTEL_FUZZ_TIMEOUT_MS").unwrap_or(5000);
        let deadline = seconds.map(Duration::from_secs);
        log_line(&format!(
            "start master_seed={master} seconds={seconds:?} cases={cases:?} timeout_ms={timeout_ms} pg={}",
            pg_sys::PG_VERSION_NUM
        ));

        Spi::run(generate::SCHEMA).unwrap();
        Spi::run("SET pg_otel.max_plan_spans = 100000").unwrap();

        let started = Instant::now();
        let mut findings: Vec<Finding> = Vec::new();
        let (mut total, mut compared, mut generator_errors, mut generator_timeouts) = (0u64, 0u64, 0u64, 0u64);
        let mut allowed: BTreeMap<String, u64> = BTreeMap::new();
        let mut coverage: BTreeMap<String, u64> = BTreeMap::new();
        let mut error_samples: HashMap<String, (u64, String)> = HashMap::new();

        for index in 0.. {
            if cases.is_some_and(|n| index >= n) || deadline.is_some_and(|d| started.elapsed() >= d) {
                break;
            }
            let seed = case_seed(master, index);
            let case = Case::generate(seed);
            // Before running: if the backend dies, this is the last line.
            log_line(&format!(
                "case master_seed={master} index={index} case_seed={seed} settings=[{}] query={}",
                case.settings_text(),
                case.query
            ));
            total += 1;

            let mut found: Vec<(String, Vec<String>)> = Vec::new();
            match run_case(&case, timeout_ms) {
                Outcome::Compared(comparison) => {
                    compared += 1;
                    for key in comparison.coverage {
                        *coverage.entry(key).or_default() += 1;
                    }
                    for mismatch in comparison.allowed {
                        *allowed.entry(mismatch.signature()).or_default() += 1;
                    }
                    let mut by_signature: BTreeMap<String, Vec<String>> = BTreeMap::new();
                    for mismatch in comparison.mismatches {
                        by_signature.entry(mismatch.signature()).or_default().push(mismatch.detail);
                    }
                    found.extend(by_signature);
                }
                Outcome::GeneratorError(message) => {
                    generator_errors += 1;
                    error_samples.entry(message).or_insert((index, case.query.clone()));
                }
                Outcome::GeneratorTimeout => generator_timeouts += 1,
                Outcome::PluginOnly(message) => {
                    found.push((format!("plugin_only_failure: {message}"), vec![message]));
                }
            }
            for (signature, diff) in found {
                log_line(&format!("MISMATCH index={index} case_seed={seed} signature={signature}"));
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
             generator_timeouts={generator_timeouts} classes={} elapsed_s={elapsed:.1} cases_per_s={:.2}\n",
            findings.len(),
            total as f64 / elapsed.max(1e-9),
        );
        report += &format!("FUZZ allowed {allowed:?}\n");
        report += &format!("FUZZ coverage {coverage:?}\n");
        for (message, (index, query)) in &error_samples {
            report += &format!("FUZZ generator_error index={index} {message} query={query}\n");
        }
        for finding in findings.iter().take(MAX_REPORTED) {
            report += &format!(
                "FUZZ MISMATCH signature=\"{}\" count={} master_seed={master} index={} case_seed={} settings=[{}] query={}\n",
                finding.signature, finding.count, finding.index, finding.case_seed, finding.settings, finding.query
            );
            for line in &finding.diff {
                report += &format!("FUZZ   diff {line}\n");
            }
        }
        if findings.len() > MAX_REPORTED {
            report += &format!("FUZZ ... and {} more classes\n", findings.len() - MAX_REPORTED);
        }
        log_line(&report);
        assert!(findings.is_empty(), "\n{report}");
        eprintln!("{report}");
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
