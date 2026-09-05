package ratelimit

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestRateLimiterAllow_RejectsImpossibleCost(t *testing.T) {
	limiter := &RateLimiter{}

	_, err := limiter.AllowWithConfig(context.Background(), "user:a", BucketConfig{
		Capacity:        1,
		RefillPerSecond: 1,
		CostPerRequest:  2,
		DenyRetryFloor:  time.Second,
	})
	if !errors.Is(err, ErrInvalidBucketConfig) {
		t.Fatalf("AllowWithConfig error = %v, want %v", err, ErrInvalidBucketConfig)
	}
}

func TestRateLimiterAllow_RejectsEmptyKey(t *testing.T) {
	limiter := &RateLimiter{}

	_, err := limiter.AllowWithConfig(context.Background(), "   ", BucketConfig{
		Capacity:        1,
		RefillPerSecond: 1,
		CostPerRequest:  1,
		DenyRetryFloor:  time.Second,
	})
	if !errors.Is(err, ErrEmptyBucketKey) {
		t.Fatalf("AllowWithConfig error = %v, want %v", err, ErrEmptyBucketKey)
	}
}

func TestRateLimiterAllow_RejectsInvalidDefaultConfig(t *testing.T) {
	limiter := &RateLimiter{DefaultConfig: BucketConfig{Capacity: 1, RefillPerSecond: 1, CostPerRequest: 2}}

	_, err := limiter.Allow(context.Background(), "user:a")
	if !errors.Is(err, ErrInvalidBucketConfig) {
		t.Fatalf("Allow error = %v, want %v", err, ErrInvalidBucketConfig)
	}
}

func TestRateLimiterDeleteStaleBuckets_RejectsNonPositiveTTL(t *testing.T) {
	limiter := &RateLimiter{}

	if _, err := limiter.DeleteStaleBuckets(context.Background(), 0); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("DeleteStaleBuckets error = %v, want ttl validation error", err)
	}
}

func TestRateLimiterDeleteBucket_RejectsEmptyKey(t *testing.T) {
	limiter := &RateLimiter{}

	if _, err := limiter.DeleteBucket(context.Background(), "\t"); !errors.Is(err, ErrEmptyBucketKey) {
		t.Fatalf("DeleteBucket error = %v, want %v", err, ErrEmptyBucketKey)
	}
}

func TestNew(t *testing.T) {
	cfg := BucketConfig{Capacity: 5, RefillPerSecond: 1, CostPerRequest: 1, DenyRetryFloor: time.Second}
	limiter := New(nil, " tenant_limits ", cfg)

	if limiter == nil {
		t.Fatal("New returned nil")
	}
	if limiter.Schema != " tenant_limits " {
		t.Fatalf("Schema = %q, want %q", limiter.Schema, " tenant_limits ")
	}
	if limiter.DefaultConfig != cfg {
		t.Fatalf("DefaultConfig = %#v, want %#v", limiter.DefaultConfig, cfg)
	}
}

func TestRateLimiterInit_RejectsNilDB(t *testing.T) {
	limiter := &RateLimiter{}

	err := limiter.Init(context.Background())
	if !errors.Is(err, ErrNilDB) {
		t.Fatalf("Init error = %v, want %v", err, ErrNilDB)
	}
}

func TestRateLimiterSchemaName_DefaultAndCustom(t *testing.T) {
	if got := (&RateLimiter{}).schemaName(); got != defaultSchemaName {
		t.Fatalf("default schema name = %q, want %q", got, defaultSchemaName)
	}

	if got := (&RateLimiter{Schema: " tenant_limits "}).schemaName(); got != "tenant_limits" {
		t.Fatalf("custom schema name = %q, want %q", got, "tenant_limits")
	}
}

func TestRateLimiterRenderMigrationSQL(t *testing.T) {
	limiter := &RateLimiter{Schema: `tenant"limits`}

	got := limiter.renderMigrationSQL("CREATE TABLE {{schema}}.items(id INT)")
	want := `CREATE TABLE "tenant""limits".items(id INT)`
	if got != want {
		t.Fatalf("renderMigrationSQL() = %q, want %q", got, want)
	}
}

func TestRateLimiterRenderMigrationSQL_AvoidsDollarQuoteCollision(t *testing.T) {
	limiter := &RateLimiter{Schema: `tenant$ratelimiter_0$`}
	template := "CREATE FUNCTION {{schema}}.f() RETURNS void AS {{delimiter}}\nBEGIN\nEND;\n{{delimiter}} LANGUAGE plpgsql;"

	got := limiter.renderMigrationSQL(template)
	want := "CREATE FUNCTION \"tenant$ratelimiter_0$\".f() RETURNS void AS $ratelimiter_1$\nBEGIN\nEND;\n$ratelimiter_1$ LANGUAGE plpgsql;"
	if got != want {
		t.Fatalf("renderMigrationSQL() = %q, want %q", got, want)
	}
}

func TestDenyRetryFloorMillis(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want int64
	}{
		{name: "non-positive", in: 0, want: 0},
		{name: "negative", in: -1 * time.Millisecond, want: 0},
		{name: "sub-millisecond rounds up", in: 500 * time.Microsecond, want: 1},
		{name: "partial milliseconds round up", in: 1500 * time.Microsecond, want: 2},
		{name: "multi-millisecond preserved", in: 25 * time.Millisecond, want: 25},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := denyRetryFloorMillis(tt.in); got != tt.want {
				t.Fatalf("denyRetryFloorMillis(%s) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestDurationToCeilMicroseconds(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want int64
	}{
		{in: time.Nanosecond, want: 1},
		{in: time.Microsecond, want: 1},
		{in: 1500 * time.Nanosecond, want: 2},
		{in: time.Millisecond, want: 1000},
	}

	for _, tt := range tests {
		if got := durationToCeilMicroseconds(tt.in); got != tt.want {
			t.Errorf("durationToCeilMicroseconds(%s) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestBucketConfigValidate(t *testing.T) {
	valid := BucketConfig{Capacity: 5, RefillPerSecond: 1, CostPerRequest: 1}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	tests := []struct {
		name string
		cfg  BucketConfig
	}{
		{name: "NaN capacity", cfg: BucketConfig{Capacity: math.NaN(), RefillPerSecond: 1, CostPerRequest: 1}},
		{name: "infinite capacity", cfg: BucketConfig{Capacity: math.Inf(1), RefillPerSecond: 1, CostPerRequest: 1}},
		{name: "NaN refill", cfg: BucketConfig{Capacity: 1, RefillPerSecond: math.NaN(), CostPerRequest: 1}},
		{name: "infinite refill", cfg: BucketConfig{Capacity: 1, RefillPerSecond: math.Inf(1), CostPerRequest: 1}},
		{name: "subnormal refill", cfg: BucketConfig{Capacity: 1, RefillPerSecond: math.SmallestNonzeroFloat64, CostPerRequest: 1}},
		{name: "NaN cost", cfg: BucketConfig{Capacity: 1, RefillPerSecond: 1, CostPerRequest: math.NaN()}},
		{name: "infinite cost", cfg: BucketConfig{Capacity: math.MaxFloat64, RefillPerSecond: 1, CostPerRequest: math.Inf(1)}},
		{name: "negative retry floor", cfg: BucketConfig{Capacity: 1, RefillPerSecond: 1, CostPerRequest: 1, DenyRetryFloor: -time.Nanosecond}},
		{name: "unrepresentable retry floor", cfg: BucketConfig{Capacity: 1, RefillPerSecond: 1, CostPerRequest: 1, DenyRetryFloor: maxRetryAfter + time.Nanosecond}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); !errors.Is(err, ErrInvalidBucketConfig) {
				t.Fatalf("Validate() error = %v, want %v", err, ErrInvalidBucketConfig)
			}
		})
	}
}
