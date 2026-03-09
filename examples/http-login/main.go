package main

import (
	"context"
	"log"
	"net/http"
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

	handler := &loginHandler{
		Limiter: limiter,
		Config: ratelimit.BucketConfig{
			Capacity:        5,
			RefillPerSecond: 1.0 / 60.0,
			CostPerRequest:  1,
			DenyRetryFloor:  time.Second,
		},
	}

	mux := http.NewServeMux()
	mux.Handle("/login", handler)

	server := &http.Server{
		Addr:              getenv("LISTEN_ADDR", ":8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	log.Printf("listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server: %v", err)
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
