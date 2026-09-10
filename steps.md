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

## Step 3: Enqueue path — NOT STARTED

## Step 4: Atomic claim query (`FOR UPDATE SKIP LOCKED`) — NOT STARTED

## Step 5: Multiple worker processes, prove no duplicate claims — NOT STARTED

## Step 6: Heartbeat + reaper — NOT STARTED

## Step 7: Fencing token (`lock_version`) — NOT STARTED

## Step 8: Retries with backoff + jitter, then DLQ — NOT STARTED