package main

import (
	"context"
	"fmt"
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

	decision, err := limiter.Allow(ctx, "login:user:alice", ratelimit.BucketConfig{
		Capacity:        5,
		RefillPerSecond: 1.0 / 60.0,
		CostPerRequest:  1,
		DenyRetryFloor:  time.Second,
	})
	if err != nil {
		log.Fatalf("allow: %v", err)
	}

	fmt.Printf("allowed=%t tokens_left=%.2f retry_after=%s\n", decision.Allowed, decision.TokensLeft, decision.RetryAfter)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
