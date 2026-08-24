package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	sharedSetupOnce sync.Once
	sharedLimiter   *RateLimiter
	sharedPool      *pgxpool.Pool
	sharedContainer *postgres.PostgresContainer
	sharedSetupErr  error
	sharedSkipMsg   string
)

func TestMain(m *testing.M) {
	code := m.Run()

	if sharedPool != nil {
		sharedPool.Close()
	}

	if sharedContainer != nil {
		_ = sharedContainer.Terminate(context.Background())
	}

	os.Exit(code)
}

func setupTestLimiter(t *testing.T) *RateLimiter {
	t.Helper()

	sharedSetupOnce.Do(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				sharedSkipMsg = fmt.Sprintf("skipping integration test: docker unavailable: %v", recovered)
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		container, err := postgres.Run(
			ctx,
			"postgres:18-alpine",
			postgres.WithDatabase("ratelimit"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)),
		)
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				sharedSkipMsg = "skipping integration test: timed out starting PostgreSQL container"
				return
			}
			sharedSkipMsg = "skipping integration test: could not start PostgreSQL container: " + err.Error()
			return
		}

		dsn, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			sharedSetupErr = err
			_ = container.Terminate(context.Background())
			return
		}

		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			sharedSetupErr = err
			_ = container.Terminate(context.Background())
			return
		}

		limiter := &RateLimiter{DB: pool}
		if err := limiter.Init(ctx); err != nil {
			sharedSetupErr = err
			pool.Close()
			_ = container.Terminate(context.Background())
			return
		}

		sharedContainer = container
		sharedPool = pool
		sharedLimiter = limiter
	})

	if sharedSkipMsg != "" {
		t.Skip(sharedSkipMsg)
	}

	if sharedSetupErr != nil {
		t.Fatalf("setup integration test limiter: %v", sharedSetupErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := sharedLimiter.DB.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", sharedLimiter.schemaQualifiedIdentifier("rate_limit_buckets")))
	if err != nil {
		t.Fatalf("reset buckets table: %v", err)
	}

	return sharedLimiter
}

func TestRateLimiterInit_Idempotent(t *testing.T) {
	limiter := setupTestLimiter(t)
	ctx := context.Background()

	if err := limiter.Init(ctx); err != nil {
		t.Fatalf("first init: %v", err)
	}
	if err := limiter.Init(ctx); err != nil {
		t.Fatalf("second init: %v", err)
	}

	version, err := limiter.schemaVersion(ctx)
	if err != nil {
		t.Fatalf("schema version: %v", err)
	}
	if version != currentSchemaVersionValue() {
		t.Fatalf("schema version = %d, want %d", version, currentSchemaVersionValue())
	}
}

func TestRateLimiterInit_CustomSchema(t *testing.T) {
	base := setupTestLimiter(t)
	ctx := context.Background()

	limiter := &RateLimiter{DB: base.DB, Schema: "ratelimit_custom"}
	if err := limiter.Init(ctx); err != nil {
		t.Fatalf("init custom schema: %v", err)
	}

	decision, err := limiter.AllowWithConfig(ctx, "custom:user", BucketConfig{
		Capacity:        1,
		RefillPerSecond: 1,
		CostPerRequest:  1,
		DenyRetryFloor:  time.Second,
	})
	if err != nil {
		t.Fatalf("allow in custom schema: %v", err)
	}
	if !decision.Allowed {
		t.Fatalf("expected first request in custom schema to be allowed")
	}

	var exists bool
	err = limiter.DB.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE bucket_key = 'custom:user')", limiter.schemaQualifiedIdentifier("rate_limit_buckets"))).Scan(&exists)
	if err != nil {
		t.Fatalf("query custom schema bucket: %v", err)
	}
	if !exists {
		t.Fatalf("expected custom schema bucket row to exist")
	}

	_, _ = limiter.DB.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", limiter.schemaQualifiedIdentifier("rate_limit_buckets")))
}

func TestRateLimiterAllow_DeniesWhenBucketExhausted(t *testing.T) {
	limiter := setupTestLimiter(t)
	ctx := context.Background()

	cfg := BucketConfig{
		Capacity:        2,
		RefillPerSecond: 1,
		CostPerRequest:  1,
		DenyRetryFloor:  time.Second,
	}
	key := "user:alice"

	first, err := limiter.AllowWithConfig(ctx, key, cfg)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if !first.Allowed {
		t.Fatalf("first call should be allowed")
	}
	if first.TokensLeft < 0.99 || first.TokensLeft > 1.0 {
		t.Fatalf("expected first call tokens_left near 1.0, got %f", first.TokensLeft)
	}

	second, err := limiter.AllowWithConfig(ctx, key, cfg)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !second.Allowed {
		t.Fatalf("second call should be allowed")
	}
	if second.TokensLeft < 0 || second.TokensLeft >= cfg.CostPerRequest {
		t.Fatalf("expected second call tokens_left to stay below one full token, got %f", second.TokensLeft)
	}

	third, err := limiter.AllowWithConfig(ctx, key, cfg)
	if err != nil {
		t.Fatalf("third call: %v", err)
	}
	if third.Allowed {
		t.Fatalf("third call should be denied")
	}
	if third.RetryAfter < time.Second {
		t.Fatalf("expected retry >= 1s, got %s", third.RetryAfter)
	}
	if third.TokensLeft < 0 || third.TokensLeft >= cfg.CostPerRequest {
		t.Fatalf("expected denied call tokens_left to remain below one full token, got %f", third.TokensLeft)
	}
}

func TestRateLimiterAllow_ConcurrentFirstHitSingleAllow(t *testing.T) {
	limiter := setupTestLimiter(t)
	ctx := context.Background()

	cfg := BucketConfig{
		Capacity:        1,
		RefillPerSecond: 0.001,
		CostPerRequest:  1,
		DenyRetryFloor:  10 * time.Millisecond,
	}

	const workers = 20

	var wg sync.WaitGroup
	var allowedCount atomic.Int32
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			decision, err := limiter.AllowWithConfig(ctx, "concurrent:key", cfg)
			if err != nil {
				t.Errorf("allow error: %v", err)
				return
			}
			if decision.Allowed {
				allowedCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := allowedCount.Load(); got != 1 {
		t.Fatalf("expected exactly one allowed request, got %d", got)
	}
}

func TestRateLimiterAllow_ConcurrentFirstHitAlwaysReturnsDecision(t *testing.T) {
	limiter := setupTestLimiter(t)
	ctx := context.Background()

	cfg := BucketConfig{
		Capacity:        1,
		RefillPerSecond: 0.001,
		CostPerRequest:  1,
		DenyRetryFloor:  10 * time.Millisecond,
	}

	const (
		iterations = 40
		workers    = 24
	)

	for i := range iterations {
		key := fmt.Sprintf("race:key:%d", i)

		var wg sync.WaitGroup
		results := make(chan Decision, workers)
		errs := make(chan error, workers)

		wg.Add(workers)
		for range workers {
			go func() {
				defer wg.Done()

				decision, err := limiter.AllowWithConfig(ctx, key, cfg)
				if err != nil {
					errs <- err
					return
				}

				results <- decision
			}()
		}

		wg.Wait()
		close(results)
		close(errs)

		for err := range errs {
			t.Fatalf("iteration %d: allow error: %v", i, err)
		}

		count := 0
		allowedCount := 0
		for decision := range results {
			count++
			if decision.Allowed {
				allowedCount++
			}
		}

		if count != workers {
			t.Fatalf("iteration %d: expected %d decisions, got %d", i, workers, count)
		}
		if allowedCount != 1 {
			t.Fatalf("iteration %d: expected exactly one allowed request, got %d", i, allowedCount)
		}
	}
}

func TestRateLimiterAllow_NormalizesKey(t *testing.T) {
	limiter := setupTestLimiter(t)
	ctx := context.Background()

	cfg := BucketConfig{
		Capacity:        1,
		RefillPerSecond: 1,
		CostPerRequest:  1,
		DenyRetryFloor:  time.Second,
	}

	first, err := limiter.AllowWithConfig(ctx, "  user:a  ", cfg)
	if err != nil {
		t.Fatalf("first allow: %v", err)
	}
	if !first.Allowed {
		t.Fatalf("first normalized request should be allowed")
	}

	second, err := limiter.AllowWithConfig(ctx, "user:a", cfg)
	if err != nil {
		t.Fatalf("second allow: %v", err)
	}
	if second.Allowed {
		t.Fatalf("expected second request against normalized key to be denied")
	}
}

func TestRateLimiterAllow_UsesDefaultConfig(t *testing.T) {
	ctx := context.Background()
	limiter := setupTestLimiter(t)
	limiter.DefaultConfig = BucketConfig{
		Capacity:        1,
		RefillPerSecond: 0.001,
		CostPerRequest:  1,
		DenyRetryFloor:  10 * time.Millisecond,
	}

	first, err := limiter.Allow(ctx, "default:user")
	if err != nil {
		t.Fatalf("first allow: %v", err)
	}
	if !first.Allowed {
		t.Fatal("expected first default-config request to be allowed")
	}

	second, err := limiter.Allow(ctx, "default:user")
	if err != nil {
		t.Fatalf("second allow: %v", err)
	}
	if second.Allowed {
		t.Fatal("expected second default-config request to be denied")
	}
}

func TestRateLimiterAllowWithConfig_OverridesDefault(t *testing.T) {
	ctx := context.Background()
	limiter := setupTestLimiter(t)
	limiter.DefaultConfig = BucketConfig{
		Capacity:        5,
		RefillPerSecond: 1,
		CostPerRequest:  1,
		DenyRetryFloor:  time.Second,
	}
	override := BucketConfig{
		Capacity:        1,
		RefillPerSecond: 0.001,
		CostPerRequest:  1,
		DenyRetryFloor:  10 * time.Millisecond,
	}

	first, err := limiter.AllowWithConfig(ctx, "override:user", override)
	if err != nil {
		t.Fatalf("first allow: %v", err)
	}
	if !first.Allowed {
		t.Fatal("expected first override-config request to be allowed")
	}

	second, err := limiter.AllowWithConfig(ctx, "override:user", override)
	if err != nil {
		t.Fatalf("second allow: %v", err)
	}
	if second.Allowed {
		t.Fatal("expected second override-config request to be denied")
	}
}

func TestRateLimiterDeleteStaleBuckets_RemovesOnlyExpiredRows(t *testing.T) {
	limiter := setupTestLimiter(t)
	ctx := context.Background()

	_, err := limiter.DB.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s(bucket_key, tokens, updated_at)
VALUES
	('stale:key', 1, statement_timestamp() - INTERVAL '2 hours'),
	('fresh:key', 1, statement_timestamp() - INTERVAL '5 minutes')
`, limiter.schemaQualifiedIdentifier("rate_limit_buckets")))
	if err != nil {
		t.Fatalf("seed buckets: %v", err)
	}

	removed, err := limiter.DeleteStaleBuckets(ctx, time.Hour)
	if err != nil {
		t.Fatalf("delete stale buckets: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removed bucket, got %d", removed)
	}

	var staleExists bool
	err = limiter.DB.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE bucket_key = 'stale:key')", limiter.schemaQualifiedIdentifier("rate_limit_buckets"))).Scan(&staleExists)
	if err != nil {
		t.Fatalf("query stale bucket: %v", err)
	}
	if staleExists {
		t.Fatal("expected stale bucket to be deleted")
	}

	var freshExists bool
	err = limiter.DB.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE bucket_key = 'fresh:key')", limiter.schemaQualifiedIdentifier("rate_limit_buckets"))).Scan(&freshExists)
	if err != nil {
		t.Fatalf("query fresh bucket: %v", err)
	}
	if !freshExists {
		t.Fatal("expected fresh bucket to remain")
	}
}
