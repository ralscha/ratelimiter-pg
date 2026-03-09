package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RateLimiter applies token-bucket decisions backed by PostgreSQL state.
type RateLimiter struct {
	DB            *pgxpool.Pool
	Schema        string
	DefaultConfig BucketConfig
}

// New constructs a RateLimiter with an optional default bucket config.
func New(db *pgxpool.Pool, schema string, defaultConfig BucketConfig) *RateLimiter {
	return &RateLimiter{
		DB:            db,
		Schema:        schema,
		DefaultConfig: defaultConfig,
	}
}

// BucketConfig describes one logical bucket configuration.
type BucketConfig struct {
	Capacity        float64
	RefillPerSecond float64
	CostPerRequest  float64
	DenyRetryFloor  time.Duration
}

// Decision is the result of evaluating one request against a bucket.
type Decision struct {
	Allowed    bool
	TokensLeft float64
	RetryAfter time.Duration
}

var (
	errInvalidBucketConfig = errors.New("invalid bucket config")
	errEmptyBucketKey      = errors.New("at least one bucket key is required")
)

// Init prepares the limiter for use by applying embedded migrations and
// verifying schema compatibility.
//
// Most callers should use Init as their single startup hook.
func (r *RateLimiter) Init(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	version, err := r.schemaVersion(ctx)
	switch {
	case err == nil && version == currentSchemaVersion:
		return nil
	case err == nil && version > currentSchemaVersion:
		return fmt.Errorf("%w: database is at version %d, library supports %d", errSchemaTooNew, version, currentSchemaVersion)
	case err != nil && !errors.Is(err, errSchemaNotInstalled):
		return err
	}

	return r.install(ctx)
}

// Allow checks whether one request is allowed for key using the limiter's default config.
func (r *RateLimiter) Allow(ctx context.Context, key string) (Decision, error) {
	return r.AllowWithConfig(ctx, key, r.DefaultConfig)
}

// AllowWithConfig checks whether one request is allowed for key using cfg.
func (r *RateLimiter) AllowWithConfig(ctx context.Context, key string, cfg BucketConfig) (Decision, error) {
	normalized, err := normalizeBucketKey(key)
	if err != nil {
		return Decision{}, err
	}
	if err := validateBucketConfig(cfg); err != nil {
		return Decision{}, err
	}
	if err := r.validate(); err != nil {
		return Decision{}, err
	}

	var allowed bool
	var tokensLeft float64
	var retryAfterMS int64
	err = r.DB.QueryRow(ctx, fmt.Sprintf(`
SELECT out_allowed AS allowed,
	out_tokens_left AS tokens_left,
	out_retry_after_ms AS retry_after_ms
FROM %s($1, $2, $3, $4, $5)
`, r.schemaQualifiedIdentifier("check_rate_limit")), normalized, cfg.Capacity, cfg.RefillPerSecond, cfg.CostPerRequest, denyRetryFloorMillis(cfg.DenyRetryFloor)).Scan(&allowed, &tokensLeft, &retryAfterMS)
	if err != nil {
		return Decision{}, fmt.Errorf("check rate limit: %w", err)
	}

	return Decision{
		Allowed:    allowed,
		TokensLeft: tokensLeft,
		RetryAfter: time.Duration(retryAfterMS) * time.Millisecond,
	}, nil
}

// DeleteStaleBuckets removes bucket rows untouched for longer than ttl.
func (r *RateLimiter) DeleteStaleBuckets(ctx context.Context, ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, errors.New("ttl must be > 0")
	}
	if err := r.validate(); err != nil {
		return 0, err
	}
	result, err := r.DB.Exec(ctx, fmt.Sprintf(`
DELETE FROM %s
WHERE updated_at < statement_timestamp() - ($1 * INTERVAL '1 millisecond')
`, r.schemaQualifiedIdentifier("rate_limit_buckets")), ttl.Milliseconds())
	if err != nil {
		return 0, fmt.Errorf("cleanup stale buckets: %w", err)
	}
	return result.RowsAffected(), nil
}

func denyRetryFloorMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	ms := d.Milliseconds()
	if ms == 0 {
		return 1
	}
	return ms
}

func validateBucketConfig(cfg BucketConfig) error {
	if cfg.Capacity <= 0 || cfg.RefillPerSecond <= 0 || cfg.CostPerRequest <= 0 || cfg.CostPerRequest > cfg.Capacity {
		return errInvalidBucketConfig
	}
	return nil
}

func normalizeBucketKey(key string) (string, error) {
	normalized := strings.TrimSpace(key)
	if normalized == "" {
		return "", errEmptyBucketKey
	}
	return normalized, nil
}
