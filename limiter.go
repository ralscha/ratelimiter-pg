package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
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

// Validate reports whether the bucket configuration can be used by the limiter.
func (cfg BucketConfig) Validate() error {
	return validateBucketConfig(cfg)
}

// Decision is the result of evaluating one request against a bucket.
type Decision struct {
	Allowed    bool
	TokensLeft float64
	RetryAfter time.Duration
}

var (
	// ErrInvalidBucketConfig indicates that a bucket configuration contains an
	// invalid capacity, refill rate, request cost, or retry floor.
	ErrInvalidBucketConfig = errors.New("invalid bucket config")
	// ErrEmptyBucketKey indicates that a bucket key is empty after trimming
	// leading and trailing whitespace.
	ErrEmptyBucketKey = errors.New("bucket key is empty")
	// ErrInvalidTTL indicates that a cleanup TTL is not positive.
	ErrInvalidTTL = errors.New("ttl must be > 0")
)

// maxRetryAfterMillis is the largest whole-millisecond duration representable
// by time.Duration. PostgreSQL calculations are capped to this value.
const maxRetryAfterMillis int64 = 9_223_372_036_854

const maxRetryAfter = time.Duration(maxRetryAfterMillis) * time.Millisecond

// PostgreSQL reports floating-point underflow below approximately 1e-307,
// even though Go float64 values can represent smaller subnormal numbers.
const minPostgresFloat = 1e-307

// Init prepares the limiter for use by applying embedded migrations and
// verifying schema compatibility.
//
// Most callers should use Init as their single startup hook.
func (r *RateLimiter) Init(ctx context.Context) error {
	if err := r.validate(); err != nil {
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
	if err := cfg.Validate(); err != nil {
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

// DeleteBucket removes the state for key. The next request for the key starts
// with a full bucket. It reports whether a bucket existed.
func (r *RateLimiter) DeleteBucket(ctx context.Context, key string) (bool, error) {
	normalized, err := normalizeBucketKey(key)
	if err != nil {
		return false, err
	}
	if err := r.validate(); err != nil {
		return false, err
	}

	result, err := r.DB.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE bucket_key = $1`, r.schemaQualifiedIdentifier("rate_limit_buckets")), normalized)
	if err != nil {
		return false, fmt.Errorf("delete bucket: %w", err)
	}
	return result.RowsAffected() > 0, nil
}

// DeleteStaleBuckets removes bucket rows untouched for longer than ttl.
func (r *RateLimiter) DeleteStaleBuckets(ctx context.Context, ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, ErrInvalidTTL
	}
	if err := r.validate(); err != nil {
		return 0, err
	}
	result, err := r.DB.Exec(ctx, fmt.Sprintf(`
DELETE FROM %s
WHERE updated_at < statement_timestamp() - ($1 * INTERVAL '1 microsecond')
`, r.schemaQualifiedIdentifier("rate_limit_buckets")), durationToCeilMicroseconds(ttl))
	if err != nil {
		return 0, fmt.Errorf("cleanup stale buckets: %w", err)
	}
	return result.RowsAffected(), nil
}

func denyRetryFloorMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	ms := int64(d / time.Millisecond)
	if d%time.Millisecond != 0 {
		ms++
	}
	return ms
}

func durationToCeilMicroseconds(d time.Duration) int64 {
	micros := int64(d / time.Microsecond)
	if d%time.Microsecond != 0 {
		micros++
	}
	return micros
}

func validateBucketConfig(cfg BucketConfig) error {
	if !isPostgresPositiveFloat(cfg.Capacity) {
		return fmt.Errorf("%w: capacity must be finite and >= %g", ErrInvalidBucketConfig, minPostgresFloat)
	}
	if !isPostgresPositiveFloat(cfg.RefillPerSecond) {
		return fmt.Errorf("%w: refill per second must be finite and >= %g", ErrInvalidBucketConfig, minPostgresFloat)
	}
	if !isPostgresPositiveFloat(cfg.CostPerRequest) {
		return fmt.Errorf("%w: cost per request must be finite and >= %g", ErrInvalidBucketConfig, minPostgresFloat)
	}
	if cfg.CostPerRequest > cfg.Capacity {
		return fmt.Errorf("%w: cost per request must not exceed capacity", ErrInvalidBucketConfig)
	}
	if cfg.DenyRetryFloor < 0 || cfg.DenyRetryFloor > maxRetryAfter {
		return fmt.Errorf("%w: deny retry floor must be between 0 and %s", ErrInvalidBucketConfig, maxRetryAfter)
	}
	return nil
}

func isPostgresPositiveFloat(value float64) bool {
	return value >= minPostgresFloat && !math.IsInf(value, 0) && !math.IsNaN(value)
}

func normalizeBucketKey(key string) (string, error) {
	normalized := strings.TrimSpace(key)
	if normalized == "" {
		return "", ErrEmptyBucketKey
	}
	return normalized, nil
}
