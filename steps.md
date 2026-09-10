# jqs — Build Steps Log

## Step 1: Postgres + project skeleton — DONE

- Created project folder `jqs`
- `go mod init jobqueue` (module name kept as `jobqueue` regardless of folder name — folder name and module name are independent)
- Created skeleton folders:
  ```
  cmd/api
  cmd/worker
  internal/db
  internal/job
  migrations
  ```
- `docker-compose.yml` at project root, single Postgres service:
  ```yaml
  services:
    postgres:
      image: postgres:16
      container_name: jobqueue-postgres
      environment:
        POSTGRES_USER: jobqueue
        POSTGRES_PASSWORD: jobqueue
        POSTGRES_DB: jobqueue
      ports:
        - "5432:5432"
      volumes:
        - jobqueue-data:/var/lib/postgresql/data
      healthcheck:
        test: ["CMD-SHELL", "pg_isready -U jobqueue"]
        interval: 5s
        timeout: 3s
        retries: 5

  volumes:
    jobqueue-data:
  ```
- Verified connection using Postgres inside Docker (no local `psql` needed):
  ```bash
  docker compose up -d
  docker compose exec postgres psql -U jobqueue
  ```
- Confirmed a named volume (`jqs_jobqueue-data`) was created automatically — data persists across `docker compose down` / `up -d`, only `docker compose down -v` destroys it.

## Step 2: `jobs` table + index — DONE

Run inside the `psql` session from step 1:

```sql
CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL DEFAULT 'default',
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 0,
    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    locked_by TEXT,
    locked_at TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    lock_version INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_jobs_claimable ON jobs (queue_name, priority DESC, run_at)
WHERE status = 'pending';
```

Verified with:
- `\d jobs` — confirmed all 14 columns and the partial index
- Test insert with only `payload` provided — confirmed defaults populate correctly (`status = 'pending'`, `priority = 0`, `attempts = 0`, `max_attempts = 5`, `lock_version = 0`, `queue_name = 'default'`, timestamps set)
- `EXPLAIN SELECT ... WHERE status = 'pending' ...` — checked whether the planner uses `idx_jobs_claimable` (may show `Seq Scan` on a near-empty table — expected, not a problem)
- `INSERT INTO jobs (payload) VALUES (NULL);` — confirmed this correctly fails on the `NOT NULL` constraint

**Note:** schema was typed directly into `psql`, not yet saved as a migration file on disk (e.g. `migrations/0001_init.sql`). Worth doing before moving much further, so the schema has a durable record outside the running container.

Step 3, Part 1: DB Connection Pool (internal/db/db.go)
1. Create the file
From your jqs project root:
mkdir -p internal/db
(Harmless if the folder already exists from step 1.)
Create internal/db/db.go with this content:
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
2. Fetch the pgx dependency
From the project root (where go.mod lives):
go get github.com/jackc/pgx/v5
This downloads the pgx Postgres driver and adds it to go.mod, plus generates a go.sum lock file (auto-managed — don't hand-edit it).
3. Confirm it worked
Open go.mod and check for a line like:
require github.com/jackc/pgx/v5 v5.6.0
4. Sanity-check the code compiles
go build ./...
No output = success. internal/db isn't called from anywhere yet, so this just confirms the package itself compiles cleanly.
Troubleshooting:
package jobqueue/internal/db: no Go files → the file didn't save at the expected path; check internal/db/db.go exists exactly there.
Import error mentioning pgxpool → step 2 (go get) likely didn't complete; rerun it.
Design notes (why it's built this way)
pgxpool, not a single connection: the API and workers will do things concurrently, so a pool that safely hands out/reuses connections across goroutines is needed, rather than one connection everyone contends for.
Ping right after creating the pool: pgxpool.New doesn't connect immediately — it sets the pool up lazily. Ping forces a real connection attempt at startup, so a Postgres-unreachable problem surfaces immediately instead of on your first real query.
DATABASE_URL env var with a fallback: lets you point at a different database later (CI, different credentials) with zero code changes, while still working out of the box against your current Docker setup.
Next up
Once go build ./... runs clean, this piece is done. Next piece: the Enqueue function in internal/job that actually inserts a job row.

