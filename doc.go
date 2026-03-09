// Package ratelimit provides a PostgreSQL-backed token-bucket rate limiter.
//
// The library keeps bucket state in PostgreSQL and evaluates each request with
// a single stored function call. Call (*RateLimiter).Init during application
// startup to prepare or upgrade the schema before serving requests.
package ratelimit
