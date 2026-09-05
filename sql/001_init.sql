CREATE TABLE IF NOT EXISTS {{schema}}.rate_limit_buckets (
	bucket_key TEXT PRIMARY KEY,
	tokens DOUBLE PRECISION NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rlb_updated_at ON {{schema}}.rate_limit_buckets (updated_at);

CREATE OR REPLACE FUNCTION {{schema}}.check_rate_limit(
	p_key TEXT,
	p_capacity DOUBLE PRECISION,
	p_refill_per_second DOUBLE PRECISION,
	p_cost_per_request DOUBLE PRECISION,
	p_deny_retry_floor_ms BIGINT
)
RETURNS TABLE (
	out_allowed BOOLEAN,
	out_tokens_left DOUBLE PRECISION,
	out_retry_after_ms BIGINT
)
LANGUAGE plpgsql
AS {{delimiter}}
DECLARE
	now_ts TIMESTAMPTZ := clock_timestamp();
	stored_tokens DOUBLE PRECISION;
	stored_updated_at TIMESTAMPTZ;
	initial_tokens_numeric NUMERIC;
	initial_tokens DOUBLE PRECISION;
	replenished_numeric NUMERIC;
	replenished DOUBLE PRECISION;
	remaining_numeric NUMERIC;
	new_updated_at TIMESTAMPTZ;
BEGIN
	initial_tokens_numeric := p_capacity::NUMERIC - p_cost_per_request::NUMERIC;
	IF initial_tokens_numeric > 0 AND initial_tokens_numeric < 1e-307::NUMERIC THEN
		initial_tokens := 0;
	ELSE
		initial_tokens := initial_tokens_numeric::DOUBLE PRECISION;
	END IF;

	LOOP
		INSERT INTO {{schema}}.rate_limit_buckets (bucket_key, tokens, updated_at)
		VALUES (p_key, initial_tokens, now_ts)
		ON CONFLICT (bucket_key) DO NOTHING;

		IF FOUND THEN
			out_allowed := TRUE;
			out_tokens_left := initial_tokens;
			out_retry_after_ms := 0;
			RETURN NEXT;
			RETURN;
		END IF;

		SELECT b.tokens, b.updated_at
		INTO stored_tokens, stored_updated_at
		FROM {{schema}}.rate_limit_buckets AS b
		WHERE b.bucket_key = p_key
		FOR UPDATE;

		EXIT WHEN FOUND;
	END LOOP;

	-- A request can wait for the row lock. Read the clock after acquiring it and
	-- never move updated_at backwards if lock waiters are served out of order.
	now_ts := GREATEST(clock_timestamp(), stored_updated_at);
	-- Use NUMERIC for intermediate calculations so very small refill rates do
	-- not trigger PostgreSQL floating-point underflow and very large waits do
	-- not overflow before they are capped.
	replenished_numeric := LEAST(
		p_capacity::NUMERIC,
		stored_tokens::NUMERIC
			+ GREATEST(EXTRACT(EPOCH FROM (now_ts - stored_updated_at)), 0)
				* p_refill_per_second::NUMERIC
	);
	IF replenished_numeric > 0 AND replenished_numeric < 1e-307::NUMERIC THEN
		replenished := 0;
		new_updated_at := stored_updated_at;
	ELSE
		replenished := replenished_numeric::DOUBLE PRECISION;
		new_updated_at := now_ts;
	END IF;
	out_allowed := replenished_numeric >= p_cost_per_request::NUMERIC;

	IF out_allowed THEN
		remaining_numeric := replenished_numeric - p_cost_per_request::NUMERIC;
		IF remaining_numeric > 0 AND remaining_numeric < 1e-307::NUMERIC THEN
			out_tokens_left := 0;
		ELSE
			out_tokens_left := remaining_numeric::DOUBLE PRECISION;
		END IF;
		out_retry_after_ms := 0;
	ELSE
		out_tokens_left := replenished;
		out_retry_after_ms := GREATEST(
			LEAST(
				CEIL(
					((p_cost_per_request::NUMERIC - replenished_numeric)
						/ p_refill_per_second::NUMERIC) * 1000
				),
				9223372036854::NUMERIC
			)::BIGINT,
			p_deny_retry_floor_ms
		);
	END IF;

	UPDATE {{schema}}.rate_limit_buckets
	SET tokens = out_tokens_left, updated_at = new_updated_at
	WHERE bucket_key = p_key;

	RETURN NEXT;
END;
{{delimiter}};
