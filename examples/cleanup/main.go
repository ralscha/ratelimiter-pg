package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	ratelimit "github.com/ralscha/ratelimiter-pg"
)

func main() {
	ctx := context.Background()

	dsn := getenv("DATABASE_URL", "postgres://ratelimit:ratelimit@localhost:5432/ratelimit?sslmode=disable")
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer db.Close()

	limiter := &ratelimit.RateLimiter{DB: db, Schema: getenv("DB_SCHEMA", "public")}
	if err := limiter.Init(ctx); err != nil {
		log.Fatalf("init limiter: %v", err)
	}

	ttl, err := time.ParseDuration(getenv("STALE_TTL", "24h"))
	if err != nil || ttl <= 0 {
		log.Fatalf("invalid STALE_TTL")
	}

	removed, err := limiter.DeleteStaleBuckets(ctx, ttl)
	if err != nil {
		log.Fatalf("cleanup: %v", err)
	}

	log.Printf("removed %d stale buckets older than %s", removed, ttl)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
