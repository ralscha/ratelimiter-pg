CREATE TABLE IF NOT EXISTS {{schema}}.rate_limit_buckets (
	bucket_key TEXT PRIMARY KEY,
	tokens DOUBLE PRECISION NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rlb_updated_at ON {{schema}}.rate_limit_buckets (updated_at);