-- Example configuration for a local test instance, not part of `setup.sql`
-- because it would write the placeholders to `postgresql.auto.conf`. Replace
-- the placeholders or delete this part before running the file anywhere else.
-- Never commit real credentials to this file.

ALTER SYSTEM SET pg_otel.otlp_endpoint = 'https://<COLLECTOR_HOST>:<PORT>';
ALTER SYSTEM SET pg_otel.otlp_protocol = 'grpc';
ALTER SYSTEM SET pg_otel.otlp_timeout_ms = '5000';
ALTER SYSTEM SET pg_otel.otlp_authorization = '<AUTHORIZATION_HEADER_VALUE>';
ALTER SYSTEM SET pg_otel.otlp_ca_certificate = '/path/to/ca-certificate.pem';
ALTER SYSTEM SET pg_otel.min_duration_ms = '0';

SELECT pg_reload_conf();
