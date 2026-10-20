-- Run this once per database after installing pg_otel (and again after
-- upgrading it): `psql -d <database> -f sql/setup.sql`. It is safe to repeat.
--
-- The library itself only needs `shared_preload_libraries`. These functions
-- are optional helpers to read its counters from SQL. The definitions match
-- what `cargo pgrx schema` generates, with `MODULE_PATHNAME` (substituted
-- only inside extension scripts) spelled out.

-- Spans dropped since server start because the queue was full.
CREATE OR REPLACE FUNCTION "pg_otel_dropped_spans"() RETURNS bigint /* i64 */
STRICT
LANGUAGE c /* Rust */
AS '$libdir/pg_otel', 'pg_otel_dropped_spans_wrapper';

-- Totals measured while `pg_otel.collection_timing` is on.
CREATE OR REPLACE FUNCTION "pg_otel_collection_stats"() RETURNS TABLE (
	"statements" bigint,  /* i64 */
	"build_ns" bigint,  /* i64 */
	"encode_ns" bigint,  /* i64 */
	"publish_ns" bigint  /* i64 */
)
STRICT
LANGUAGE c /* Rust */
AS '$libdir/pg_otel', 'pg_otel_collection_stats_wrapper';

-- Example configuration for a local test instance. Replace the placeholders
-- or delete this part before running the file anywhere else.
-- Never commit real credentials to this file.
ALTER SYSTEM SET pg_otel.otlp_endpoint = 'https://<COLLECTOR_HOST>:<PORT>';
ALTER SYSTEM SET pg_otel.otlp_protocol = 'grpc';
ALTER SYSTEM SET pg_otel.otlp_timeout_ms = '5000';
ALTER SYSTEM SET pg_otel.otlp_authorization = '<AUTHORIZATION_HEADER_VALUE>';
ALTER SYSTEM SET pg_otel.otlp_ca_certificate = '/path/to/ca-certificate.pem';
ALTER SYSTEM SET pg_otel.min_duration_ms = '0';

SELECT pg_reload_conf();
