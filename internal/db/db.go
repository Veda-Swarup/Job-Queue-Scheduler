// Package db handles the Postgres connection pool used by every component
// (enqueue API, workers, reaper, scheduler). Everything shares one pool
// setup so connection settings stay consistent across the project.
package db

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// New creates a pgx connection pool using DATABASE_URL from the environment,
// falling back to sane local-dev defaults that match docker-compose.yml.
func New(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://jobqueue:jobqueue@localhost:5432/jobqueue?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("creating pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	return pool, nil
}
