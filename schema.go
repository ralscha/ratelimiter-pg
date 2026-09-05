package ratelimit

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strings"
)

const currentSchemaVersion int64 = 1

const defaultSchemaName = "public"

const createSchemaMigrationsTableSQLTemplate = `
CREATE TABLE IF NOT EXISTS %s (
	version BIGINT PRIMARY KEY,
	name TEXT NOT NULL,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()
);
`

type schemaMigration struct {
	version int64
	name    string
	file    string
}

var schemaMigrations = []schemaMigration{
	{version: 1, name: "init", file: "sql/001_init.sql"},
}

var (
	// ErrNilDB indicates that a limiter has no PostgreSQL pool.
	ErrNilDB = errors.New("rate limiter DB is nil")
	// ErrInvalidSchema indicates that a schema name cannot be represented as a
	// PostgreSQL identifier.
	ErrInvalidSchema = errors.New("rate limiter schema is invalid")
	// ErrSchemaTooNew indicates that the database was initialized by a newer library version.
	ErrSchemaTooNew = errors.New("rate limiter schema is newer than this library")
)

//go:embed sql/*.sql
var migrationFiles embed.FS

func (r *RateLimiter) install(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	tx, err := r.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin schema initialization: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	lockName := "github.com/ralscha/ratelimiter-pg:" + r.schemaName()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lockName); err != nil {
		return fmt.Errorf("lock schema initialization: %w", err)
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", r.quotedSchemaName())); err != nil {
		return fmt.Errorf("ensure schema %q: %w", r.schemaName(), err)
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf(createSchemaMigrationsTableSQLTemplate, r.schemaQualifiedIdentifier("rate_limit_schema_migrations"))); err != nil {
		return fmt.Errorf("ensure schema migrations table: %w", err)
	}

	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT version FROM %s", r.schemaQualifiedIdentifier("rate_limit_schema_migrations")))
	if err != nil {
		return fmt.Errorf("list applied migrations: %w", err)
	}

	applied := make(map[int64]bool, len(schemaMigrations))
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			rows.Close()
			return fmt.Errorf("scan applied migration version: %w", err)
		}
		if version > currentSchemaVersion {
			rows.Close()
			return fmt.Errorf("%w: database is at version %d, library supports %d", ErrSchemaTooNew, version, currentSchemaVersion)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate applied migrations: %w", err)
	}
	rows.Close()

	replay := false
	for _, migration := range schemaMigrations {
		if !applied[migration.version] {
			replay = true
		}
		if !replay {
			continue
		}

		body, err := fs.ReadFile(migrationFiles, migration.file)
		if err != nil {
			return fmt.Errorf("read migration %d (%s): %w", migration.version, migration.name, err)
		}
		rendered := r.renderMigrationSQL(string(body))

		if _, err := tx.Exec(ctx, rendered); err != nil {
			return fmt.Errorf("apply migration %d (%s): %w", migration.version, migration.name, err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (version, name)
VALUES ($1, $2)
ON CONFLICT (version) DO NOTHING
`, r.schemaQualifiedIdentifier("rate_limit_schema_migrations")), migration.version, migration.name); err != nil {
			return fmt.Errorf("record migration %d (%s): %w", migration.version, migration.name, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit schema initialization: %w", err)
	}
	return nil
}

func (r *RateLimiter) validate() error {
	if r == nil || r.DB == nil {
		return ErrNilDB
	}
	if strings.ContainsRune(r.schemaName(), 0) {
		return ErrInvalidSchema
	}
	return nil
}

func (r *RateLimiter) schemaName() string {
	if r == nil {
		return defaultSchemaName
	}

	name := strings.TrimSpace(r.Schema)
	if name == "" {
		return defaultSchemaName
	}
	return name
}

func (r *RateLimiter) quotedSchemaName() string {
	return quoteIdentifier(r.schemaName())
}

func (r *RateLimiter) schemaQualifiedIdentifier(name string) string {
	return r.quotedSchemaName() + "." + quoteIdentifier(name)
}

func (r *RateLimiter) renderMigrationSQL(sql string) string {
	delimiter := ""
	for suffix := 0; ; suffix++ {
		candidate := fmt.Sprintf("$ratelimiter_%d$", suffix)
		if !strings.Contains(sql, candidate) && !strings.Contains(r.schemaName(), candidate) {
			delimiter = candidate
			break
		}
	}

	rendered := strings.ReplaceAll(sql, "{{delimiter}}", delimiter)
	return strings.ReplaceAll(rendered, "{{schema}}", r.quotedSchemaName())
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
