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
AS $$
DECLARE
	now_ts TIMESTAMPTZ := statement_timestamp();
BEGIN
	RETURN QUERY
	INSERT INTO {{schema}}.rate_limit_buckets AS b (bucket_key, tokens, updated_at)
	VALUES (
		p_key,
		p_capacity - p_cost_per_request,
		now_ts
	)
	ON CONFLICT (bucket_key) DO UPDATE
	SET
		tokens = CASE
			WHEN LEAST(
				p_capacity,
				b.tokens + GREATEST(EXTRACT(EPOCH FROM (now_ts - b.updated_at)), 0) * p_refill_per_second
			) >= p_cost_per_request THEN LEAST(
				p_capacity,
				b.tokens + GREATEST(EXTRACT(EPOCH FROM (now_ts - b.updated_at)), 0) * p_refill_per_second
			) - p_cost_per_request
			ELSE LEAST(
				p_capacity,
				b.tokens + GREATEST(EXTRACT(EPOCH FROM (now_ts - b.updated_at)), 0) * p_refill_per_second
			)
		END,
		updated_at = now_ts
	RETURNING WITH (OLD AS old_row, NEW AS new_row)
		CASE
			WHEN old_row.bucket_key IS NULL THEN TRUE
			ELSE LEAST(
				p_capacity,
				old_row.tokens + GREATEST(EXTRACT(EPOCH FROM (now_ts - old_row.updated_at)), 0) * p_refill_per_second
			) >= p_cost_per_request
		END AS out_allowed,
		new_row.tokens AS out_tokens_left,
		CASE
			WHEN old_row.bucket_key IS NULL THEN 0::BIGINT
			WHEN LEAST(
				p_capacity,
				old_row.tokens + GREATEST(EXTRACT(EPOCH FROM (now_ts - old_row.updated_at)), 0) * p_refill_per_second
			) >= p_cost_per_request THEN 0::BIGINT
			ELSE GREATEST(
				CEIL((
					(
						p_cost_per_request - LEAST(
							p_capacity,
							old_row.tokens + GREATEST(EXTRACT(EPOCH FROM (now_ts - old_row.updated_at)), 0) * p_refill_per_second
						)
					) / p_refill_per_second
				) * 1000.0)::BIGINT,
				p_deny_retry_floor_ms
			)
		END AS out_retry_after_ms;
END;
$$;