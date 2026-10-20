-- Update script for pg_otel 0.2.0 -> 0.3.0: adds the collection timing totals.
-- The definition matches what `cargo pgrx schema` generates for 0.3.0.
CREATE FUNCTION "pg_otel_collection_stats"() RETURNS TABLE (
	"statements" bigint,  /* i64 */
	"build_ns" bigint,  /* i64 */
	"encode_ns" bigint,  /* i64 */
	"publish_ns" bigint  /* i64 */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pg_otel_collection_stats_wrapper';
