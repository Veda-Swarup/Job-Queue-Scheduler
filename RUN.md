# How to Run JQS

## Prerequisites

- **Go** 1.22+ installed
- **Docker** and **Docker Compose** installed

---

## 1. Start Postgres

```bash
docker compose up -d
```

Verify it's ready:

```bash
docker compose exec postgres pg_isready -U jobqueue
```

You should see: `accepting connections`

> **First time only:** Run the migration to create the `jobs` table:
> ```bash
> docker compose exec postgres psql -U jobqueue -f - < migrations/0001_init.sql
> ```

## 2. Build the binaries

```bash
go build -o bin/api    ./cmd/api
go build -o bin/worker ./cmd/worker
go build -o bin/bench  ./cmd/bench
```

No output means success.

## 3. Start the API server (Terminal 1)

```bash
DATABASE_URL="postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable" ./bin/api
```

You should see:

```
API listening on :8080
```

## 4. Start Worker 1 (Terminal 2)

```bash
DATABASE_URL="postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable" ./bin/worker -id=w1 -concurrency=3
```

You should see:

```
[w1] starting — queue=default concurrency=3
```

## 5. Start Worker 2 (Terminal 3)

```bash
DATABASE_URL="postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable" ./bin/worker -id=w2 -concurrency=3
```

You should see:

```
[w2] starting — queue=default concurrency=3
```

## 6. Enqueue jobs (Terminal 4)

Enqueue 10 test jobs:

```bash
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8080/enqueue \
    -H 'Content-Type: application/json' \
    -d "{\"payload\":{\"task\":\"job-$i\"},\"priority\":$i}"
  echo
done
```

Or enqueue a single job:

```bash
curl -s -X POST http://localhost:8080/enqueue \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"task":"send-email","to":"user@example.com"},"priority":5}'
```

Switch to the worker terminals to watch them claim and process jobs.

## 7. Shutting down

### Stop the workers and API

Press **Ctrl+C** in each terminal (API, w1, w2). Workers will finish in-progress
jobs and print:

```
[w1] shut down cleanly
```

### Stop Postgres

```bash
docker compose down
```

> **Note:** Your data is preserved in a Docker volume. Next time you run
> `docker compose up -d`, all previous jobs are still there.
>
> To **destroy all data** and start fresh:
> ```bash
> docker compose down -v
> ```

---

## Running Tests

Requires Postgres to be running (`docker compose up -d`).

### Run all tests (unit + integration)

```bash
DATABASE_URL="postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable" \
  go test -v -count=1 ./internal/job/
```

`-count=1` disables Go's test caching so every run hits the real database.

You should see all 17 tests pass:

```
--- PASS: TestEnqueueAndClaim (0.02s)
--- PASS: TestComplete (0.01s)
--- PASS: TestComplete_StaleLockVersion (0.01s)
--- PASS: TestFail_RetryWithBackoff (0.01s)
--- PASS: TestFail_DLQ (0.01s)
--- PASS: TestReaper (0.01s)
--- PASS: TestReaper_DoesNotReapFreshJobs (0.01s)
--- PASS: TestHeartbeat (0.06s)
--- PASS: TestHeartbeat_StaleLockVersion (0.01s)
--- PASS: TestConcurrentClaims_ZeroDuplicates (0.05s)
--- PASS: TestClaimPriorityOrder (0.01s)
--- PASS: TestEnqueue_EmptyPayload (0.00s)
--- PASS: TestDelayedJob (0.00s)
--- PASS: TestPow2 (0.00s)
--- PASS: TestBackoffBounds (0.00s)
--- PASS: TestBackoffIsAlwaysNonNegative (0.00s)
--- PASS: TestBackoffCapAt600 (0.00s)
PASS
ok   jqs/internal/job    0.719s
```

### Run only unit tests (no database needed)

```bash
go test -v -run 'TestPow2|TestBackoff' ./internal/job/
```

For more detail on what each test covers, see [`TESTING.md`](TESTING.md).

---

## Running the Benchmark

Requires Postgres to be running. Does **not** require the API or workers to be running — the benchmark tool manages its own enqueue/drain cycle.

```bash
DATABASE_URL="postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable" \
  go run ./cmd/bench -jobs=1000 -levels=1,2,4,8,16
```

This will:
1. Enqueue 1,000 jobs per round into an isolated `bench` queue
2. Drain them at each concurrency level (1, 2, 4, 8, 16 workers)
3. Measure throughput (jobs/sec) and latency (p50, p95, p99)
4. Verify zero duplicate claims after each round
5. Write the report to [`BENCHMARK.md`](BENCHMARK.md)

### Benchmark flags

| Flag | Default | Description |
|------|---------|-------------|
| `-jobs` | `1000` | Number of jobs to enqueue per round |
| `-levels` | `1,2,4,8,16` | Comma-separated concurrency levels to sweep |
| `-queue` | `bench` | Queue name (isolated from the default queue) |

---

## Worker flags

| Flag           | Default       | Description                              |
|----------------|---------------|------------------------------------------|
| `-id`          | hostname-pid  | Unique worker identifier                 |
| `-queue`       | `default`     | Queue name to consume                    |
| `-concurrency` | `4`           | Number of goroutines per process         |
| `-poll`        | `500ms`       | Idle poll interval                       |
| `-heartbeat`   | `10s`         | Heartbeat interval                       |
| `-reaper`      | `15s`         | Reaper scan interval                     |
| `-stale`       | `45s`         | Heartbeat age that marks a job stale     |

---

## Inspecting the database

Connect to Postgres directly:

```bash
docker compose exec postgres psql -U jobqueue
```

Useful queries:

```sql
-- Job status counts
SELECT status, count(*) FROM jobs GROUP BY status;

-- View dead letter queue
SELECT id, payload, attempts, last_error, created_at
FROM jobs WHERE status = 'dead'
ORDER BY created_at DESC;

-- View currently running jobs
SELECT id, locked_by, locked_at, last_heartbeat
FROM jobs WHERE status = 'running';

-- Clear all jobs (start fresh)
TRUNCATE jobs RESTART IDENTITY;
```

---

## Troubleshooting

### `bind: address already in use` on port 8080

Something else is using port 8080. Find and kill it:

```bash
lsof -ti :8080 | xargs kill
```

### `role "jobqueue" does not exist`

A local Postgres on port 5432 is intercepting the connection. Make sure
`docker-compose.yml` maps to port **5433** (`"5433:5432"`) and that your
`DATABASE_URL` uses port **5433**.

### `jobs` table doesn't exist

Run the migration:

```bash
docker compose exec postgres psql -U jobqueue -f - < migrations/0001_init.sql
```

### Tests skip with "cannot ping postgres"

Postgres isn't running. Start it with `docker compose up -d` and wait a few
seconds for the healthcheck to pass.

---

## Quick Reference

| What | Command |
|------|---------|
| Start Postgres | `docker compose up -d` |
| Stop Postgres (keep data) | `docker compose down` |
| Stop Postgres (destroy data) | `docker compose down -v` |
| Build all binaries | `go build -o bin/api ./cmd/api && go build -o bin/worker ./cmd/worker && go build -o bin/bench ./cmd/bench` |
| Run API | `DATABASE_URL="..." ./bin/api` |
| Run worker | `DATABASE_URL="..." ./bin/worker -id=w1 -concurrency=3` |
| Run tests | `DATABASE_URL="..." go test -v -count=1 ./internal/job/` |
| Run benchmark | `DATABASE_URL="..." go run ./cmd/bench` |
| Connect to psql | `docker compose exec postgres psql -U jobqueue` |
