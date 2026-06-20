# ratelimiter-pg

`github.com/ralscha/ratelimiter-pg` is a PostgreSQL-backed token-bucket rate-limiting library for Go.

It stores bucket state in PostgreSQL and evaluates each request with one stored function call.

## Install

```bash
go get github.com/ralscha/ratelimiter-pg
```

## Requirements

- Go 1.26 or newer, matching the module's `go` directive.
- PostgreSQL 18 or newer with PL/pgSQL enabled. The embedded migration uses PostgreSQL 18 `RETURNING WITH (OLD AS ..., NEW AS ...)` syntax.

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

	limiter := ratelimit.New(db, "public", ratelimit.BucketConfig{
		Capacity:        5,
		RefillPerSecond: 1.0 / 60.0,
		CostPerRequest:  1,
		DenyRetryFloor:  time.Second,
	})

	if err := limiter.Init(ctx); err != nil {
		log.Fatal(err)
	}

	decision, err := limiter.Allow(ctx, "login:user:alice")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("allowed=%t tokens_left=%.2f retry_after=%s", decision.Allowed, decision.TokensLeft, decision.RetryAfter)
}
```

When one request needs different settings than the limiter default, call `AllowWithConfig`:

```go
decision, err := limiter.AllowWithConfig(ctx, "login:user:alice", ratelimit.BucketConfig{
	Capacity:        10,
	RefillPerSecond: 1,
	CostPerRequest:  2,
	DenyRetryFloor:  time.Second,
})
```

Minimal request flow:

1. Open a PostgreSQL connection pool.
2. Construct `RateLimiter` with the pool and default bucket config. Leave `Schema` empty to use `public`, or set it to target a different schema.
3. Call `Init` once during startup.
4. Call `Allow` for each key you want to throttle, or `AllowWithConfig` when one call needs a different bucket config.

## Public API

- `New` constructs a `RateLimiter` with a PostgreSQL pool, target schema, and default bucket config.
- `RateLimiter` holds the PostgreSQL pool, target schema, and default bucket config. An empty `Schema` value defaults to `public`.
- `RateLimiter.DefaultConfig` is the bucket config used by `(*RateLimiter).Allow`.
- `BucketConfig` defines capacity, refill rate, cost, and deny retry floor. `Capacity`, `RefillPerSecond`, and `CostPerRequest` must be `> 0`, and `CostPerRequest` must not exceed `Capacity`.
- `Decision` reports whether a request was allowed, how many tokens remain, and when to retry.
- `(*RateLimiter).Init` prepares the limiter for use.
- `(*RateLimiter).Allow` evaluates one key with the limiter's default bucket config. It trims leading and trailing whitespace from the key and rejects an empty result.
- `(*RateLimiter).AllowWithConfig` evaluates one key with a call-specific bucket config override.
- `(*RateLimiter).DeleteStaleBuckets` deletes untouched buckets older than a TTL.

## Schema management

`Init` is the only schema/bootstrap method exposed by the library.

- `(*RateLimiter).Init` checks the current schema state and applies pending migrations when needed.
- On a fresh database, `Init` creates the limiter objects and installs the embedded schema.
- On an existing but outdated database, `Init` upgrades the limiter schema to the version required by the library.
- If the database schema version is newer than the library supports, `Init` returns an error instead of downgrading or modifying it.
- On a database that is already current, `Init` returns without applying changes.
- Set `RateLimiter.Schema` when you want the limiter objects in a schema other than `public`.

## Examples

Runnable examples live under `examples/`:

- `examples/basic` shows the smallest end-to-end limiter setup.
- `examples/http-login` shows a login endpoint that returns `Retry-After` when throttled.
- `examples/cleanup` shows how to delete stale bucket rows. Like the other examples, it still calls `Init` during startup.

All examples use these environment variables when present:

- `DATABASE_URL` for the PostgreSQL connection string.
- `DB_SCHEMA` for a non-default schema name.
- `LISTEN_ADDR` for the HTTP example.
- `STALE_TTL` for the cleanup example.

If unset, the examples default to the PostgreSQL settings from `docker-compose.yml`:

- `DATABASE_URL=postgres://ratelimit:ratelimit@localhost:5432/ratelimit?sslmode=disable`
- `DB_SCHEMA=public`
- `LISTEN_ADDR=:8080`
- `STALE_TTL=24h`

Run them with:

```bash
go run ./examples/basic
go run ./examples/http-login
go run ./examples/cleanup
```

## Development

Common commands:

```bash
go fmt ./...
go vet ./...
go test ./...
```

With [Task](https://taskfile.dev/) installed, the same checks are available as:

```bash
task format
task vet
task test
```

Use `task pg-up` to start the local PostgreSQL container used by the examples, and `task pg-reset` when you want a clean database volume.

## Key design

The limiter is generic. It accepts any non-empty string key chosen by the caller.

Examples:

```text
login:user:alice
endpoint:read_issues
db:read_table_query
tenant:acme:write
```

The library does not interpret key structure or normalize case. It only trims leading and trailing whitespace.

That makes it suitable for per-user login throttling, per-tenant quotas, per-endpoint limits, or any other string-addressable bucket strategy chosen by the application.

## How it works

`(*RateLimiter).Allow` validates `RateLimiter.DefaultConfig`, trims leading and trailing whitespace from the key, and then calls the PostgreSQL function `check_rate_limit`.

Use `(*RateLimiter).AllowWithConfig` when a specific request should override that default configuration.

That function replenishes tokens lazily from elapsed time and atomically applies the allow-or-deny decision through one PostgreSQL 18 `INSERT ... ON CONFLICT ... DO UPDATE ... RETURNING WITH (OLD AS ..., NEW AS ...)` statement.

Because the decision is stored and computed in PostgreSQL, competing requests for the same bucket serialize on the same row instead of relying on in-process memory or distributed locks.

For denied requests it computes:

```text
retry_ms = ceil((cost_per_request - replenished) / refill_per_second * 1000)
```

The deny retry floor is then applied so very small retry values still surface as a visible delay.

## Database objects

`Init` creates these objects in the configured schema:

```sql
CREATE TABLE public.rate_limit_schema_migrations (
	version     BIGINT PRIMARY KEY,
	name        TEXT NOT NULL,
	applied_at  TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE public.rate_limit_buckets (
	bucket_key  TEXT PRIMARY KEY,
	tokens      DOUBLE PRECISION NOT NULL,
	updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_rlb_updated_at ON public.rate_limit_buckets (updated_at);

CREATE OR REPLACE FUNCTION public.check_rate_limit(...)
```

The `rate_limit_schema_migrations` table records which embedded migrations have been applied.

The `updated_at` index supports `DeleteStaleBuckets`, which removes rows that have not been touched for a configurable TTL.

## Status codes and retries

The library is transport-agnostic. It returns a `Decision` with `Allowed`, `TokensLeft`, and `RetryAfter`, and the caller decides how that maps to HTTP responses, gRPC errors, CLI behavior, or background job scheduling.
