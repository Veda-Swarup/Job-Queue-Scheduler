# JQS — Codebase Guide

This document explains **every file** in the project — what it does, why it exists, and how it connects to the rest of the system. Read this to understand the full codebase without opening the source.

---

## Project Tree

```
jqs/
├── cmd/                          ← Executable entry points (one binary per subdirectory)
│   ├── api/
│   │   └── main.go               ← HTTP server: accepts jobs via POST /enqueue
│   ├── worker/
│   │   └── main.go               ← Worker process: claims, executes, heartbeats, reaps
│   └── bench/
│       └── main.go               ← Benchmark tool: throughput + latency measurement
│
├── internal/                     ← Shared library code (Go convention: not importable externally)
│   ├── db/
│   │   └── db.go                 ← Postgres connection pool (pgxpool)
│   └── job/
│       ├── job.go                ← All queue logic: Enqueue, Claim, Complete, Fail, Heartbeat, Reap
│       ├── job_unit_test.go      ← Unit tests for backoff calculation
│       └── job_integration_test.go ← Integration tests against live Postgres
│
├── migrations/
│   └── 0001_init.sql             ← Schema: CREATE TABLE jobs + partial index
│
├── docker-compose.yml            ← Postgres 16 container config
├── go.mod                        ← Go module definition + dependencies
├── go.sum                        ← Dependency checksums (auto-generated)
├── .gitignore                    ← Files excluded from version control
│
├── DESCRIPTION.md                ← Project overview + mechanisms explained
├── ARCHITECTURE.md               ← Architecture, schema, workflow, design decisions
├── TESTING.md                    ← Test suite documentation + performance results
├── BENCHMARK.md                  ← Auto-generated benchmark report
├── RUN.md                        ← How to build and run everything
├── TODO.md                       ← Remaining work tracker
├── README.md                     ← Project entry point
└── about.md                      ← Extended design rationale
```

---

## Why This Structure?

JQS follows the standard Go project layout:

- **`cmd/`** — Each subdirectory compiles into a separate binary (`go build ./cmd/api`, `go build ./cmd/worker`, `go build ./cmd/bench`). This matters because the API and workers run as independent OS processes.

- **`internal/`** — Shared packages that multiple binaries import. The `internal` directory is special in Go: code inside it **cannot be imported by external modules**. This enforces that `db` and `job` are internal implementation details, not a public library.

- **`migrations/`** — Raw SQL files applied to the database. Not managed by an ORM — the SQL is written by hand so every column and index decision is explicit and explainable.

---

## File-by-File Walkthrough

---

### `internal/db/db.go` — Database Connection Pool

**Purpose:** Creates a shared Postgres connection pool used by every component.

**What it does:**
1. Reads `DATABASE_URL` from the environment (falls back to a localhost default matching `docker-compose.yml`)
2. Creates a `pgxpool.Pool` — a thread-safe connection pool that manages multiple concurrent database connections
3. Calls `pool.Ping()` to verify the database is reachable at startup (fail fast, not on first query)

**Why it exists as a separate package:** Every binary (`api`, `worker`, `bench`) needs a database connection. Centralising the pool setup ensures they all use the same DSN logic and connection configuration. Without this, you'd duplicate the connection code in three `main.go` files.

**Key design decisions:**
- **`pgxpool` (not a single `*pgx.Conn`):** Workers run multiple goroutines that all query the database concurrently. A pool safely hands out and reuses connections across goroutines. A single connection would force all goroutines to serialize their queries.
- **`pool.Ping()` at startup:** `pgxpool.New()` is lazy — it doesn't actually connect until the first query. `Ping()` forces an immediate connection attempt so a "database unreachable" error surfaces at startup, not buried in the first user request.
- **Environment variable with fallback:** `DATABASE_URL` lets you point at a different database (CI, staging) with zero code changes, while still working out of the box for local development.

---

### `internal/job/job.go` — The Core Queue Logic

**Purpose:** Contains every database operation the queue needs. This is the most important file in the project.

**What it defines:**

#### Types

**`Job` struct** (lines 16–29) — A Go struct that mirrors one row from the `jobs` table. Every field maps to a column. `LockedBy` and `LastError` are `*string` (pointer-to-string) because the database columns are nullable — Go can't scan `NULL` into a plain `string`, but `*string` can hold `nil`.

**`EnqueueParams` struct** (lines 33–39) — The set of fields a caller can provide when submitting a job. Fields with zero values are omitted from the INSERT, letting the database apply its column defaults. `RunAt` is `*time.Time` (nil = "use database default of `now()`").

#### Functions

**`Enqueue()`** (lines 42–90) — Inserts a new job row. Uses a `switch` to build the INSERT dynamically based on which optional fields are set. This avoids sending Go zero-values that would override database defaults (e.g., sending `max_attempts = 0` would override the database default of 5).

**`Claim()`** (lines 98–139) — The most important function. Executes the atomic CTE that:
1. Finds the highest-priority pending job whose `run_at <= now()`
2. Locks it with `FOR UPDATE SKIP LOCKED`
3. Updates it to `running` in the same statement
4. Returns the full job row

Returns `(nil, nil)` when no job is available — this is a signal to the worker loop to sleep and try again.

**`Heartbeat()`** (lines 144–154) — Updates `last_heartbeat = now()` for a running job. Guarded by `lock_version` so a zombie worker's heartbeat is silently ignored.

**`Complete()`** (lines 159–172) — Sets `status = 'done'`. Guarded by `lock_version` and `status = 'running'` — if the job was reclaimed (version bumped) or is no longer running, this is a no-op.

**`Fail()`** (lines 182–221) — The most complex function. Two paths:
- **Retry path** (`attempts < max_attempts`): Computes a backoff delay using `base × 2^(attempts-1)` capped at 600s, then applies full jitter (uniform random from `[0, maxDelay]`). Resets the job to `pending` with `run_at` in the future and clears `locked_by`/`locked_at`.
- **DLQ path** (`attempts >= max_attempts`): Sets `status = 'dead'` and preserves `last_error`.

Both paths are guarded by `lock_version`.

**`Reap()`** (lines 228–242) — Finds all `running` jobs whose `last_heartbeat` is older than the stale threshold and resets them to `pending`. Returns the count of recovered jobs. Safe to run concurrently from multiple processes because the `WHERE` clause is naturally idempotent.

**`pow2()`** (lines 245–251) — Simple helper that computes 2^n using a loop. Used by the backoff calculation. Unexported (lowercase) because it's an implementation detail.

---

### `cmd/api/main.go` — HTTP Enqueue Server

**Purpose:** A minimal HTTP server that accepts job submissions.

**What it does:**
1. Creates a database connection pool via `db.New()`
2. Registers a single HTTP handler at `POST /enqueue`
3. Listens on port `:8080`

**The request/response cycle:**

```
Client                           API Server                    PostgreSQL
  │                                  │                              │
  │  POST /enqueue                   │                              │
  │  {"payload":{...},"priority":5}  │                              │
  │ ────────────────────────────────►│                              │
  │                                  │  INSERT INTO jobs (...)      │
  │                                  │  RETURNING id                │
  │                                  │ ────────────────────────────►│
  │                                  │◄──────────────────────────── │
  │                                  │  id = 42                     │
  │◄──────────────────────────────── │                              │
  │  {"id": 42}                      │                              │
```

**Key design decisions:**
- **`json.RawMessage` for payload:** The API doesn't interpret the payload — it passes it through as raw bytes to the database. This keeps the API generic (any JSON is valid).
- **Method check:** Only `POST` is allowed. `GET /enqueue` returns 405.
- **Validation:** Empty payload → 400 Bad Request. Missing queue name → defaults to `"default"`.
- **`r.Context()` passed to Enqueue:** If the client disconnects mid-request, the context is cancelled and the database query is aborted. Prevents orphaned queries.

---

### `cmd/worker/main.go` — Worker Process

**Purpose:** The consumer side of the queue. Polls for jobs, claims them, executes them, and handles success/failure.

**What it does on startup:**
1. Parses command-line flags (`-id`, `-queue`, `-concurrency`, `-poll`, `-heartbeat`, `-reaper`, `-stale`)
2. Sets up signal handling (`Ctrl+C` / `SIGTERM` → context cancellation)
3. Creates a database connection pool
4. Spawns one reaper goroutine
5. Spawns N worker goroutines (configurable, default 4)
6. Waits for all goroutines to exit, then prints "shut down cleanly"

**The key functions:**

**`main()`** (lines 30–68) — Orchestrator. Sets up the context, pool, and goroutines. The `done` channel + final `for range` loop is a standard Go pattern for waiting on N goroutines to finish.

**`runReaper()`** (lines 72–88) — A ticker-based loop that calls `job.Reap()` every 15 seconds. Logs how many stale jobs it recovered. Exits cleanly when the context is cancelled.

**`workerLoop()`** (lines 91–125) — Each goroutine runs this loop:
1. Check `ctx.Err()` — if the context is cancelled (shutdown), return
2. Call `job.Claim()` — if no job available, sleep 500ms and try again
3. If a job is claimed, call `execute()`
4. Go back to step 1

**`execute()`** (lines 129–174) — Runs one job:
1. Creates a child context (`hbCtx`) for the heartbeat goroutine
2. Spawns the heartbeat goroutine (sends `Heartbeat()` every 10s)
3. Calls `simulateWork()` (the fake job handler)
4. Based on the result: calls `Complete()` or `Fail()`
5. `defer hbCancel()` stops the heartbeat goroutine exactly when `execute()` returns

**`simulateWork()`** (lines 178–186) — Simulates real work: sleeps for 50–2000ms (random), fails 20% of the time. In a real system, this would be the actual job handler (send email, process image, etc.).

**`sleep()`** (lines 189–194) — Context-aware sleep. Returns early if the context is cancelled (shutdown signal), preventing a goroutine from being stuck in a sleep during shutdown.

**`defaultWorkerID()`** (lines 197–200) — Generates `hostname-PID` as a unique-enough identifier for local demo use.

**Worker flags:**

| Flag | Default | Purpose |
|------|---------|---------|
| `-id` | hostname-PID | Unique worker identifier |
| `-queue` | `default` | Which queue to consume |
| `-concurrency` | `4` | Number of goroutines per process |
| `-poll` | `500ms` | How long to sleep when the queue is empty |
| `-heartbeat` | `10s` | How often to refresh `last_heartbeat` |
| `-reaper` | `15s` | How often to scan for stale jobs |
| `-stale` | `45s` | Heartbeat age that marks a job as stale |

---

### `cmd/bench/main.go` — Benchmark Tool

**Purpose:** Measures raw queue throughput and per-job latency at varying concurrency levels.

**How it works:**
1. For each concurrency level (default: 1, 2, 4, 8, 16):
   - Cleans up any leftover bench jobs
   - Enqueues 1,000 jobs into a dedicated `bench` queue
   - Spawns N goroutines that claim + immediately complete jobs (no simulated work)
   - Measures wall-clock time for the drain phase
   - Records per-job latency (Claim start → Complete return)
   - Verifies zero duplicates and zero leftover jobs in the database
2. Computes percentiles (p50, p95, p99) and throughput (jobs/sec)
3. Prints the report and writes it to `BENCHMARK.md`

**Why it uses zero simulated work:** We're measuring the queue mechanics (the cost of the `SKIP LOCKED` CTE + completion UPDATE), not fake `time.Sleep` calls. This gives a true throughput ceiling for the Postgres-based queue.

---

### `internal/job/job_unit_test.go` — Unit Tests

**Purpose:** Tests the backoff calculation logic in isolation. No database required.

**Tests:**

| Test | What it verifies |
|------|-----------------|
| `TestPow2` | `pow2(n)` returns correct powers of 2 |
| `TestBackoffBounds` | Max delay matches formula for each attempt (5s, 10s, 20s, 40s, ... capped at 600s) |
| `TestBackoffIsAlwaysNonNegative` | Max delay ≥ 0 for attempts 1–100 |
| `TestBackoffCapAt600` | At attempt 8+ the delay is correctly capped to 600 seconds |

**Why these are in the same package (`package job`):** `pow2` is unexported (lowercase). Tests in the same package can access unexported functions. This avoids exporting an implementation detail just for testing.

---

### `internal/job/job_integration_test.go` — Integration Tests

**Purpose:** Tests every queue operation end-to-end against a live Postgres database.

**Test isolation strategy:** Each test generates a unique queue name (`test_TestName_12345`) and deletes all rows in that queue on cleanup. Tests never interfere with each other or with manual use of the `default` queue.

**Tests (13 total):**

| Test | What it proves |
|------|---------------|
| `TestEnqueueAndClaim` | Basic lifecycle works: enqueue → claim → status=running |
| `TestComplete` | Complete sets status=done in the database |
| `TestComplete_StaleLockVersion` | Fencing token blocks zombie writes (the key correctness test) |
| `TestFail_RetryWithBackoff` | Failed job resets to pending with future run_at |
| `TestFail_DLQ` | Exhausted attempts → status=dead |
| `TestReaper` | Stale heartbeat → job recovered to pending → re-claimable |
| `TestReaper_DoesNotReapFreshJobs` | Fresh heartbeat → not reaped (no false positives) |
| `TestHeartbeat` | Heartbeat advances last_heartbeat timestamp |
| `TestHeartbeat_StaleLockVersion` | Wrong lock_version → heartbeat is silent no-op |
| `TestConcurrentClaims_ZeroDuplicates` | 8 goroutines, 100 jobs, zero duplicate claims |
| `TestClaimPriorityOrder` | Jobs claimed in descending priority order |
| `TestEnqueue_EmptyPayload` | Nil payload → validation error |
| `TestDelayedJob` | Future run_at → Claim returns nil |

---

### `migrations/0001_init.sql` — Database Schema

**Purpose:** Creates the `jobs` table and its partial index. Run once when setting up a new database.

**How to apply:**
```bash
docker compose exec postgres psql -U jobqueue -f - < migrations/0001_init.sql
```

**Why raw SQL (not an ORM migration tool):** Every column, constraint, and index in the schema exists for a specific reason. Using raw SQL keeps those decisions explicit and visible, rather than hidden behind an abstraction layer.

See [ARCHITECTURE.md](ARCHITECTURE.md) for a column-by-column explanation.

---

### `docker-compose.yml` — Infrastructure

**Purpose:** Runs PostgreSQL 16 in a Docker container with persistent storage.

**Key settings:**
- Port `5433:5432` — avoids conflict with local Postgres on default port
- Named volume `jobqueue-data` — data survives `docker compose down`
- Healthcheck via `pg_isready` — confirms Postgres is accepting connections

---

### `go.mod` / `go.sum` — Dependency Management

**`go.mod`** defines the module name (`jqs`), Go version, and dependencies:
- `github.com/jackc/pgx/v5` — High-performance Postgres driver using the native wire protocol (not database/sql). Includes `pgxpool` for connection pooling.
- Transitive dependencies: `pgpassfile`, `pgservicefile`, `puddle/v2` (connection pool internals), `golang.org/x/sync`, `golang.org/x/text`

**`go.sum`** is auto-generated. It contains cryptographic checksums for every dependency to ensure reproducible builds. Never edit this file by hand.

---

### `.gitignore`

```
bin/            # Compiled binaries (rebuilt from source)
execution.txt   # Local execution logs
steps.md        # Build steps log (personal notes)
.DS_Store       # macOS filesystem metadata
```

---

## How the Files Connect

```
                    cmd/api/main.go
                         │
                         │ imports
                         ▼
              ┌── internal/db/db.go ──────────────────┐
              │      (connection pool)                 │
              │                                        │
              │ imports                                │
              ▼                                        ▼
    internal/job/job.go ◄──────── cmd/worker/main.go
         (queue logic)           cmd/bench/main.go
              │
              │ queries
              ▼
    migrations/0001_init.sql
         (schema)
              │
              │ runs in
              ▼
    docker-compose.yml
         (Postgres 16)
```

**The dependency chain is simple:**
1. `docker-compose.yml` runs Postgres
2. `migrations/0001_init.sql` creates the schema inside Postgres
3. `internal/db/db.go` connects to Postgres
4. `internal/job/job.go` executes queries against the schema
5. `cmd/api/`, `cmd/worker/`, `cmd/bench/` import `db` and `job` to build their binaries

No circular dependencies. No external services beyond Postgres. Every binary can be built and understood independently.
