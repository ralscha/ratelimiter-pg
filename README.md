# ratelimiter-pg

`github.com/ralscha/ratelimiter-pg` is a PostgreSQL-backed token-bucket rate-limiting library for Go.

It stores bucket state in PostgreSQL, evaluates each request with one stored function call, and keeps the public API intentionally small.

## Install

```bash
go get github.com/ralscha/ratelimiter-pg
```

## Quick start

Call `Init` once during application startup. It is the library's single bootstrap method and prepares the schema for use.

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
	ratelimit "github.com/ralscha/ratelimiter-pg"
)

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/app?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	limiter := &ratelimit.RateLimiter{DB: db, Schema: "public"}

	if err := limiter.Init(ctx); err != nil {
		log.Fatal(err)
	}

	decision, err := limiter.Allow(ctx, "login:user:alice", ratelimit.BucketConfig{
		Capacity:        5,
		RefillPerSecond: 1.0 / 60.0,
		CostPerRequest:  1,
		DenyRetryFloor:  time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("allowed=%t tokens_left=%.2f retry_after=%s", decision.Allowed, decision.TokensLeft, decision.RetryAfter)
}
```

Minimal request flow:

1. Open a PostgreSQL connection pool.
2. Construct `RateLimiter` with the pool and schema name.
3. Call `Init` once during startup.
4. Call `Allow` for each key you want to throttle.

## Public API

- `RateLimiter` holds the PostgreSQL pool and target schema.
- `BucketConfig` defines capacity, refill rate, cost, and deny retry floor.
- `Decision` reports whether a request was allowed, how many tokens remain, and when to retry.
- `(*RateLimiter).Init` prepares the limiter for use.
- `(*RateLimiter).Allow` evaluates one key and returns a `Decision`.
- `(*RateLimiter).DeleteStaleBuckets` deletes untouched buckets older than a TTL.

## Schema management

`Init` is the only schema/bootstrap method exposed by the library.

- `(*RateLimiter).Init` checks the current schema state and applies pending migrations when needed.
- On a fresh database, `Init` creates the limiter objects and installs the embedded schema.
- On an existing but outdated database, `Init` upgrades the limiter schema to the version required by the library.
- On a database that is already current, `Init` returns without applying changes.
- Set `RateLimiter.Schema` when you want the limiter objects in a schema other than `public`.

The embedded SQL files live under `sql/`:

```text
001_init.sql
002_check_rate_limit_v1.sql
```

## Examples

Runnable examples live under `examples/`:

- `examples/basic` shows the smallest end-to-end limiter setup.
- `examples/http-login` shows a login endpoint that returns `Retry-After` when throttled.
- `examples/cleanup` shows how to delete stale bucket rows after the schema is already installed.

All examples use these environment variables when present:

- `DATABASE_URL` for the PostgreSQL connection string.
- `DB_SCHEMA` for a non-default schema name.
- `LISTEN_ADDR` for the HTTP example.
- `STALE_TTL` for the cleanup example.

Run them with:

```bash
go run ./examples/basic
go run ./examples/http-login
go run ./examples/cleanup
```

## Key design

The limiter is generic. It accepts any non-empty string key chosen by the caller.

Examples:

```text
login:user:alice
endpoint:read_issues
db:read_table_query
tenant:acme:write
```

The library does not interpret key structure. It only trims leading and trailing whitespace.

That makes it suitable for per-user login throttling, per-tenant quotas, per-endpoint limits, or any other string-addressable bucket strategy chosen by the application.

## How it works

The PostgreSQL function `check_rate_limit` validates the request configuration, normalizes the key, replenishes tokens lazily from elapsed time, and atomically applies the allow-or-deny decision through one `INSERT ... ON CONFLICT ... DO UPDATE ... RETURNING` statement.

Because the decision is stored and computed in PostgreSQL, competing requests for the same bucket serialize on the same row instead of relying on in-process memory or distributed locks.

For denied requests it computes:

```text
retry_ms = ceil((cost_per_request - replenished) / refill_per_second * 1000)
```

The deny retry floor is then applied so very small retry values still surface as a visible delay.

## Database objects

`Init` creates a bucket table and stored function in the configured schema:

```sql
CREATE TABLE public.rate_limit_buckets (
    bucket_key  TEXT PRIMARY KEY,
    tokens      DOUBLE PRECISION NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_rlb_updated_at ON public.rate_limit_buckets (updated_at);
```

The `updated_at` index supports `DeleteStaleBuckets`, which removes rows that have not been touched for a configurable TTL.

## Status codes and retries

The library itself is transport-agnostic. It returns a `Decision` with `Allowed`, `TokensLeft`, and `RetryAfter`, and the caller decides how that maps to HTTP responses, gRPC errors, CLI behavior, or background job scheduling.
