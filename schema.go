package ratelimit

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

const currentSchemaVersion int64 = 2

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
	{version: 2, name: "check_rate_limit_v1", file: "sql/002_check_rate_limit_v1.sql"},
}

var (
	errNilDB              = errors.New("rate limiter DB is nil")
	errInvalidSchema      = errors.New("rate limiter schema is invalid")
	errSchemaNotInstalled = errors.New("rate limiter schema not installed")
	errSchemaOutdated     = errors.New("rate limiter schema is outdated")
	errSchemaTooNew       = errors.New("rate limiter schema is newer than this library")
)

//go:embed sql/*.sql
var migrationFiles embed.FS

func currentSchemaVersionValue() int64 {
	return currentSchemaVersion
}

func (r *RateLimiter) install(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	if _, err := r.DB.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", r.quotedSchemaName())); err != nil {
		return fmt.Errorf("ensure schema %q: %w", r.schemaName(), err)
	}

	if _, err := r.DB.Exec(ctx, fmt.Sprintf(createSchemaMigrationsTableSQLTemplate, r.schemaQualifiedIdentifier("rate_limit_schema_migrations"))); err != nil {
		return fmt.Errorf("ensure schema migrations table: %w", err)
	}

	applied, err := r.appliedMigrationVersions(ctx)
	if err != nil {
		return err
	}

	for _, migration := range schemaMigrations {
		if applied[migration.version] {
			continue
		}

		body, err := fs.ReadFile(migrationFiles, migration.file)
		if err != nil {
			return fmt.Errorf("read migration %d (%s): %w", migration.version, migration.name, err)
		}
		rendered := r.renderMigrationSQL(string(body))

		tx, err := r.DB.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %d (%s): %w", migration.version, migration.name, err)
		}

		if _, err := tx.Exec(ctx, rendered); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %d (%s): %w", migration.version, migration.name, err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (version, name)
VALUES ($1, $2)
ON CONFLICT (version) DO NOTHING
`, r.schemaQualifiedIdentifier("rate_limit_schema_migrations")), migration.version, migration.name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %d (%s): %w", migration.version, migration.name, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %d (%s): %w", migration.version, migration.name, err)
		}
	}

	return r.checkSchema(ctx)
}

func (r *RateLimiter) checkSchema(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	version, err := r.schemaVersion(ctx)
	if err != nil {
		return err
	}

	switch {
	case version == 0:
		return fmt.Errorf("%w: run migrations through version %d", errSchemaNotInstalled, currentSchemaVersion)
	case version < currentSchemaVersion:
		return fmt.Errorf("%w: database is at version %d, library requires %d", errSchemaOutdated, version, currentSchemaVersion)
	case version > currentSchemaVersion:
		return fmt.Errorf("%w: database is at version %d, library supports %d", errSchemaTooNew, version, currentSchemaVersion)
	default:
		return nil
	}
}

func (r *RateLimiter) schemaVersion(ctx context.Context) (int64, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}

	var version int64
	err := r.DB.QueryRow(ctx, fmt.Sprintf(`
SELECT COALESCE(MAX(version), 0)
FROM %s
`, r.schemaQualifiedIdentifier("rate_limit_schema_migrations"))).Scan(&version)
	if err != nil {
		if isUndefinedTable(err) {
			return 0, fmt.Errorf("%w: migration metadata table is missing", errSchemaNotInstalled)
		}
		return 0, fmt.Errorf("read schema version: %w", err)
	}

	return version, nil
}

func (r *RateLimiter) appliedMigrationVersions(ctx context.Context) (map[int64]bool, error) {
	rows, err := r.DB.Query(ctx, fmt.Sprintf("SELECT version FROM %s", r.schemaQualifiedIdentifier("rate_limit_schema_migrations")))
	if err != nil {
		return nil, fmt.Errorf("list applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64]bool, len(schemaMigrations))
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan applied migration version: %w", err)
		}
		applied[version] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}

	return applied, nil
}

func (r *RateLimiter) validate() error {
	if r == nil || r.DB == nil {
		return errNilDB
	}
	if strings.ContainsRune(r.schemaName(), 0) {
		return errInvalidSchema
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
	return strings.ReplaceAll(sql, "{{schema}}", r.quotedSchemaName())
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}
