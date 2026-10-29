//! End-to-end tests: a PostgreSQL server with `pg_otel` preloaded exports to a
//! real OpenTelemetry collector, whose file exporter we read back.
//!
//! The tests do not start anything themselves. They are `#[ignore]`d and need
//! the environment described in `scripts/integration-tests.sh`:
//!
//! - `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`: a superuser connection to the
//!   server (defaults: `localhost`, `5432`, `postgres`, `postgres`).
//! - `COLLECTOR_OUT_DIR`: directory the collector writes `grpc.jsonl` and
//!   `http.jsonl` to.
//! - `COLLECTOR_HOST`: how the *server* reaches the collector (default
//!   `localhost`).

use std::{
    env, fs,
    path::PathBuf,
    sync::{Mutex, MutexGuard},
    thread::sleep,
    time::{Duration, Instant},
};

use postgres::{Client, NoTls};
use serde_json::Value;

const TIMEOUT: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(250);

/// The tests reconfigure the one server, so they must not overlap.
static SERVER: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy)]
enum Transport {
    Grpc,
    Http,
}

impl Transport {
    fn protocol(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::Http => "http/protobuf",
        }
    }

    fn endpoint(self) -> String {
        let host = env::var("COLLECTOR_HOST").unwrap_or_else(|_| "localhost".into());
        match self {
            Self::Grpc => format!("http://{host}:4317"),
            Self::Http => format!("http://{host}:4318/v1/traces"),
        }
    }

    /// File the collector writes this transport's traces to.
    fn file(self) -> PathBuf {
        let dir = env::var("COLLECTOR_OUT_DIR").expect("COLLECTOR_OUT_DIR is not set");
        PathBuf::from(dir).join(match self {
            Self::Grpc => "grpc.jsonl",
            Self::Http => "http.jsonl",
        })
    }
}

struct Server {
    client: Client,
    _lock: MutexGuard<'static, ()>,
}

impl Server {
    /// Connects and points the exporter at `transport`.
    fn connect(transport: Transport) -> Self {
        let lock = SERVER.lock().unwrap_or_else(|e| e.into_inner());
        let var = |name: &str, default: &str| env::var(name).unwrap_or_else(|_| default.into());
        let mut client = Client::configure()
            .host(&var("PGHOST", "localhost"))
            .port(var("PGPORT", "5432").parse().expect("PGPORT"))
            .user(&var("PGUSER", "postgres"))
            .dbname(&var("PGDATABASE", "postgres"))
            .password(var("PGPASSWORD", ""))
            .connect(NoTls)
            .expect("cannot connect to PostgreSQL");
        // ALTER SYSTEM cannot share an implicit transaction with other statements.
        for sql in [
            format!(
                "ALTER SYSTEM SET pg_otel.otlp_protocol = '{}'",
                transport.protocol()
            ),
            format!(
                "ALTER SYSTEM SET pg_otel.otlp_endpoint = '{}'",
                transport.endpoint()
            ),
            "SELECT pg_reload_conf()".to_string(),
        ] {
            client.batch_execute(&sql).unwrap();
        }
        Self {
            client,
            _lock: lock,
        }
    }

    /// Runs one traced statement as a child of `trace_id`.
    fn traced_query(&mut self, trace_id: &str, min_duration_ms: i32) {
        self.client
            .batch_execute(&format!(
                "BEGIN;
                 SET LOCAL pg_otel.min_duration_ms = {min_duration_ms};
                 SET LOCAL pg_otel.traceparent = '00-{trace_id}-00f067aa0ba902b7-01';
                 SELECT count(*) FROM generate_series(1, 100) AS g WHERE g > 10;
                 COMMIT;"
            ))
            .unwrap();
    }
}

/// A random trace id, so tests (and reruns) can find their own spans.
fn new_trace_id() -> String {
    (0..16)
        .map(|_| format!("{:02x}", fastrand::u8(..)))
        .collect()
}

/// One exported span together with the resource it came from.
struct Span {
    json: Value,
    resource: Value,
}

impl Span {
    fn str(&self, key: &str) -> &str {
        self.json[key].as_str().unwrap_or_default()
    }

    fn attr(&self, key: &str) -> Option<&Value> {
        attribute(&self.json["attributes"], key)
    }
}

fn attribute<'a>(attributes: &'a Value, key: &str) -> Option<&'a Value> {
    attributes
        .as_array()?
        .iter()
        .find(|a| a["key"] == key)
        .map(|a| &a["value"])
}

/// All spans of `trace_id` the collector has written so far.
fn read_spans(transport: Transport, trace_id: &str) -> Vec<Span> {
    let Ok(content) = fs::read_to_string(transport.file()) else {
        return Vec::new();
    };
    let mut spans = Vec::new();
    for line in content.lines().filter(|l| !l.is_empty()) {
        let request: Value = serde_json::from_str(line).expect("collector wrote invalid JSON");
        for rs in request["resourceSpans"].as_array().into_iter().flatten() {
            for ss in rs["scopeSpans"].as_array().into_iter().flatten() {
                for span in ss["spans"].as_array().into_iter().flatten() {
                    if span["traceId"] == trace_id {
                        spans.push(Span {
                            json: span.clone(),
                            resource: rs["resource"].clone(),
                        });
                    }
                }
            }
        }
    }
    spans
}

/// Runs the traced query until its spans show up at the collector. Repeating
/// covers the moment after `pg_reload_conf()` in which the exporter may still
/// use the previous settings.
fn export_and_collect(server: &mut Server, transport: Transport, trace_id: &str) -> Vec<Span> {
    let start = Instant::now();
    loop {
        server.traced_query(trace_id, 0);
        sleep(POLL * 2);
        let spans = read_spans(transport, trace_id);
        if spans.iter().any(|s| s.attr("db.operation.name").is_some()) {
            return spans;
        }
        assert!(
            start.elapsed() < TIMEOUT,
            "no spans for {trace_id} after {TIMEOUT:?}"
        );
    }
}

fn check_export(transport: Transport) {
    let mut server = Server::connect(transport);
    let trace_id = new_trace_id();
    let spans = export_and_collect(&mut server, transport, &trace_id);

    let query = spans
        .iter()
        .find(|s| s.attr("db.operation.name").is_some())
        .unwrap();
    assert_eq!(
        query.attr("db.operation.name").unwrap()["stringValue"],
        "SELECT"
    );
    assert_eq!(
        query.attr("db.system.name").unwrap()["stringValue"],
        "postgresql"
    );
    assert!(
        query.attr("db.query.text").unwrap()["stringValue"]
            .as_str()
            .unwrap()
            .contains("generate_series"),
    );
    // The traceparent made the statement a child of the caller's span.
    assert_eq!(query.str("parentSpanId"), "00f067aa0ba902b7");

    // Plan node spans hang below the query span, all within the same trace.
    let nodes: Vec<_> = spans
        .iter()
        .filter(|s| s.attr("postgresql.plan.node_type").is_some())
        .collect();
    assert!(!nodes.is_empty(), "no plan node spans exported");
    let ids: Vec<_> = spans.iter().map(|s| s.str("spanId")).collect();
    for node in &nodes {
        assert!(
            ids.contains(&node.str("parentSpanId")),
            "orphaned plan node span"
        );
    }

    let service_name = attribute(&query.resource["attributes"], "service.name").unwrap();
    assert_eq!(service_name["stringValue"], "postgresql");
    assert!(attribute(&query.resource["attributes"], "service.framework.version").is_some());
}

#[test]
#[ignore = "needs PostgreSQL and a collector, see scripts/integration-tests.sh"]
fn exports_spans_over_grpc() {
    check_export(Transport::Grpc);
}

#[test]
#[ignore = "needs PostgreSQL and a collector, see scripts/integration-tests.sh"]
fn exports_spans_over_http() {
    check_export(Transport::Http);
}

#[test]
#[ignore = "needs PostgreSQL and a collector, see scripts/integration-tests.sh"]
fn exports_nothing_when_disabled() {
    for transport in [Transport::Grpc, Transport::Http] {
        let mut server = Server::connect(transport);
        let disabled = new_trace_id();
        let enabled = new_trace_id();
        server.traced_query(&disabled, -1);
        // A traced statement after the disabled one proves the exporter is
        // working, so the absence of the first is meaningful.
        export_and_collect(&mut server, transport, &enabled);
        assert!(read_spans(transport, &disabled).is_empty());
    }
}
