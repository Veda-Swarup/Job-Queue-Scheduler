# JQS — A Distributed Job Queue Built on PostgreSQL

## What It Is

JQS is a distributed job queue and async task scheduler built from scratch in Go, using **PostgreSQL as the only coordination layer** — no Redis, no RabbitMQ, no Kafka, no external broker of any kind.

It solves a common backend problem: you have work that's too slow or too risky to do inside an HTTP request (sending emails, processing files, calling flaky APIs), so you record it as a "job" in durable storage, respond to the caller immediately, and let a pool of independent workers pick it up and execute it in the background.

Here's what JQS handles:

- **Concurrent claiming** — Multiple workers race to pick up jobs. A Postgres row-lock mechanism (`FOR UPDATE SKIP LOCKED`) ensures each job is claimed by exactly one worker, with zero duplicates, zero blocking.
- **Crash recovery** — Workers send periodic heartbeats. If a worker dies mid-job, a background reaper detects the stale heartbeat and makes the job available again.
- **Zombie protection** — A fencing token (`lock_version`) prevents a paused-then-resumed worker from overwriting state that a replacement worker has already taken over.
- **Smart retries** — Failed jobs are retried with exponential backoff and full jitter to avoid overwhelming a recovering dependency.
- **Dead letter queue** — Jobs that exhaust all retry attempts are moved to a `dead` status (preserved, not deleted) so failures are visible and actionable.
- **Horizontal scaling** — Spinning up more worker processes immediately increases throughput. No configuration changes, no coordination setup.

---

## Why Build This Instead of Using SQS/Sidekiq/Celery?

Because using a queue and understanding a queue are fundamentally different.

Every design decision in this project starts with a failure scenario:

> *"What goes wrong if I don't handle this?"*

And ends with a concrete mechanism in the code:

> *"Here's the exact query, column, or check that prevents it."*

The value isn't that JQS produces a working queue — you could use SQS for that. The value is that every line of the implementation has a reason, and that reason traces back to a specific failure mode the line prevents.

---

## The Core Insight: Why Postgres Works as a Queue

The central architectural choice is using Postgres as the message broker. This works because of one feature: **`SELECT ... FOR UPDATE SKIP LOCKED`**.

### The problem without it

If two workers both run `SELECT * FROM jobs WHERE status = 'pending' LIMIT 1`, they can read the **same row** before either updates it. Both claim the same job. The email gets sent twice.

This isn't a hypothetical edge case — at any meaningful concurrency level, it happens constantly.

### What `FOR UPDATE` does

It locks the selected row. A second transaction trying to select it will **block** until the first finishes. This prevents duplicates, but reintroduces a different problem: all workers queue behind one lock instead of working in parallel. You've traded a correctness bug for a performance bottleneck.

### What `SKIP LOCKED` adds

It tells Postgres: "If this row is already locked, don't wait — skip it and try the next candidate."

Now:
- Worker A locks row 1
- Worker B sees row 1 is locked, skips to row 2, locks it
- Worker C skips rows 1 and 2, locks row 3

Ten workers claim ten different jobs simultaneously, with **zero blocking, zero duplication**, and no coordination code beyond the SQL statement itself.

**In one sentence:** `FOR UPDATE` gives correctness (no duplicate claims); `SKIP LOCKED` gives back the concurrency that plain locking would destroy.

### The tradeoff

A single Postgres table handles **thousands of jobs per second** on commodity hardware (benchmarked at **5,775 jobs/sec** — see [Performance](#performance) below). But it's not Kafka. At tens of thousands of jobs per second or when horizontal database sharding is needed, a dedicated broker wins. JQS treats this as an acknowledged, reasoned-about limitation, not a hidden weakness — because understanding where a design breaks is part of understanding the design.

---

## The Mechanisms, One by One

Each mechanism exists because of a specific failure mode. The table at the end of this section maps every mechanism to the failure it prevents.

### 1. Atomic Claim — Closing the TOCTOU Gap

**The problem:** If claiming is two steps — `SELECT` the next pending job, then `UPDATE` it to `running` — there's a window between the check and the action where another worker can interleave. This is a classic TOCTOU (time-of-check to time-of-use) race.

**The fix:** Collapse both steps into a single atomic SQL statement using a CTE (Common Table Expression):

```sql
WITH next_job AS (
    SELECT id FROM jobs
    WHERE status = 'pending' AND run_at <= now() AND queue_name = $1
    ORDER BY priority DESC, run_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs
SET status = 'running', locked_by = $2, locked_at = now(),
    last_heartbeat = now(), lock_version = lock_version + 1, attempts = attempts + 1
FROM next_job
WHERE jobs.id = next_job.id
RETURNING jobs.*;
```

The CTE selects and the UPDATE modifies in one transaction — no gap, no interleaving possible.

**Why the partial index matters:** The claim query only ever searches for `status = 'pending'` rows. Without the `WHERE status = 'pending'` condition on the index, it would grow to include millions of `done` rows over time — pure overhead. The partial index stays small and fast indefinitely, regardless of how much job history accumulates.

> **General principle:** Whenever a system "checks something, then acts on it" as two separate steps, ask whether another actor could act in the gap. If yes, the check and the action need to be one atomic operation.

### 2. Heartbeat + Reaper — Recovering from Crashed Workers

**The problem:** Claiming a job and marking it `running` solves duplication — but what if the worker crashes before completing the job? The job is stuck in `running` forever, silently lost. Nothing in the system will ever revisit it.

**The fix:** Two cooperating mechanisms:

1. **Heartbeat** — While executing a job, a worker goroutine periodically writes `last_heartbeat = now()` to the job's row. This is a liveness signal: "I'm still alive and working on this."

2. **Reaper** — A background loop (running on every worker process) periodically scans for jobs in `running` state whose heartbeat has gone stale beyond a configurable threshold (default: 45 seconds). Stale jobs are reset to `pending` so another worker can claim them.

**Why the reaper is safe to run on every worker (no leader election needed):** The reaper's operation is a conditional `UPDATE ... WHERE last_heartbeat < threshold`. Once one process's update lands, the row no longer matches the `WHERE` clause for anyone else. Running the same query from five workers simultaneously is harmless — it's naturally idempotent.

**The threshold tradeoff:**
- Too short (e.g., 5s) → false positives: a job that's just slow (GC pause, slow API call) gets prematurely reclaimed and duplicated
- Too long (e.g., 10 min) → a genuinely crashed worker's job sits stuck for a long time
- Rule of thumb: stale threshold = 3–5× the heartbeat interval (heartbeat every 10s → reap after 30–45s)

**The fundamental consequence — at-least-once execution:** If a worker finishes real work (e.g., sends an email) but crashes *before* writing `status = 'done'`, the reaper will reclaim the job and another worker will run it again. This means job handlers should be **idempotent** (safe to execute twice) if duplicate side effects matter. This is a deliberate design property — the same tradeoff made by every major queue system — not a bug.

This pattern ("prove you're alive by renewing a lease, or the lease expires") is the same mechanism used in Kubernetes leader election, Zookeeper distributed locks, and etcd.

### 3. Fencing Tokens — Protecting Against Zombie Workers

**The problem:** Heartbeat timeouts are probabilistic. The reaper can't perfectly distinguish a dead worker from a live-but-paused one. Consider this scenario:

1. Worker A claims a job (lock_version = 1)
2. Worker A pauses (GC pause, slow disk flush) for slightly longer than the stale threshold
3. The reaper assumes Worker A is dead and resets the job to `pending`
4. Worker B claims the job (lock_version = 2) and starts processing
5. Worker A resumes, unaware it's been reclaimed, and tries to mark the job `done`

Without a guard, Worker A's stale write succeeds — and now the job has been processed twice, or worse, Worker A's `done` overwrites Worker B's in-progress state.

**The fix:** `lock_version` — incremented every time a job is claimed. Every state-mutating operation includes:

```sql
WHERE id = $1 AND lock_version = $2
```

When Worker A tries to complete with `lock_version = 1`, but the reaper + Worker B have bumped it to 2, Worker A's write matches **zero rows** and silently no-ops. The stale worker can't corrupt anything.

This is the same technique used by distributed lock systems (Chubby, etcd) under the name "fencing token." The version acts as a generation counter — only the current generation's writes are accepted.

> **This is arguably the single most important correctness mechanism in the whole design**, because it's what makes the system safe even when the heartbeat/reaper mechanism gets a liveness judgment wrong.

### 4. Exponential Backoff with Full Jitter — Respectful Retries

**The problem:** When a job fails, retrying it immediately is usually wrong. If it failed because a downstream service is struggling, an instant retry hits the same struggling service again — with more concurrent load from all the other workers doing the same thing.

**Exponential backoff:** Doubles the wait time between each retry — 5s, 10s, 20s, 40s... capped at 10 minutes. This gives a failing dependency time to recover.

**But pure backoff has its own problem:** Every job that failed during a brief outage wakes up and retries at the *exact same instant*, creating a synchronised spike called the **thundering herd** — which can overwhelm a service that was just recovering.

**Full jitter fixes this:** Instead of a fixed exponential value, the retry delay is sampled uniformly from `[0, max_delay]`. Jobs that all failed at the same moment now spread their retries randomly across the full window, turning a synchronized spike into a gradual ramp.

**How it's implemented:** The retry delay is written directly into the job's `run_at` column. The claim query already filters `run_at <= now()`, so delayed retries are invisible to workers until the right moment — no additional scheduler or timer needed.

### 5. Dead Letter Queue — Making Failures Visible

**The problem:** Some failures are not transient. A malformed payload, a logic bug in the handler, a dependency that will never recover — these jobs fail identically on every retry. Continuing to retry them wastes worker capacity and buries other work.

**The fix:** Once a job's `attempts` reaches `max_attempts`, it transitions to `status = 'dead'` instead of being retried. The row is **preserved, not deleted**, with `last_error` set to the final failure message. A human can inspect what went wrong and manually re-enqueue the job if appropriate.

This is the difference between a system that silently loses unrecoverable work and one that makes failures **observable and actionable**.

### Summary: Each Mechanism and the Failure It Solves

| Mechanism | Failure it prevents | What happens without it |
|---|---|---|
| `FOR UPDATE SKIP LOCKED` | Duplicate job claims | Same job runs N times simultaneously |
| Atomic CTE claim | TOCTOU race between check and claim | Another worker interleaves in the gap |
| Heartbeat + reaper | Jobs lost when a worker crashes | Job stuck in `running` forever |
| Fencing token (`lock_version`) | Zombie worker corrupting state | Stale worker overwrites new worker's progress |
| Exponential backoff | Hammering a failing dependency | Retries worsen the outage |
| Full jitter | Thundering herd (synchronized retries) | All failed jobs retry at the same instant |
| Dead letter queue | Infinite retries on broken jobs | Resources wasted, failures invisible |

---

## Architecture

```
                       ┌──────────────────────────┐
                       │    HTTP POST /enqueue     │
                       └────────────┬─────────────┘
                                    │
                                    ▼
                       ┌──────────────────────────┐
                       │       Enqueue API         │
                       │    (cmd/api/main.go)      │
                       └────────────┬─────────────┘
                                    │  INSERT INTO jobs
                                    ▼
                       ┌──────────────────────────┐
                       │       jobs table          │
                       │      (PostgreSQL)         │
                       └─────┬──────┬──────┬──────┘
                             │      │      │
                    ┌────────┘      │      └────────┐
                    ▼               ▼               ▼
             Worker Process 1  Worker Process 2  Worker Process N
             (cmd/worker)      (cmd/worker)      (cmd/worker)
             ├─ goroutine #0   ├─ goroutine #0   ├─ goroutine #0
             ├─ goroutine #1   ├─ goroutine #1   ├─ goroutine #1
             ├─ goroutine #2   ├─ goroutine #2   ├─ goroutine #2
             └─ reaper loop    └─ reaper loop    └─ reaper loop
```

Each worker process runs a configurable number of goroutines, each independently claiming and executing jobs. Multiple processes can run on the same machine or across multiple machines — **the only shared state is the Postgres database**.

Workers are separate OS processes, not goroutines in one binary. This is deliberate: real production workers are separate pods, containers, or machines that don't share memory. The database is their only coordination point. Running them as independent processes proves the coordination mechanism works across actors with no shared memory — which is the real claim this project needs to substantiate.

---

## Schema

```sql
CREATE TABLE jobs (
    id             BIGSERIAL PRIMARY KEY,
    queue_name     TEXT        NOT NULL DEFAULT 'default',
    payload        JSONB       NOT NULL,
    status         TEXT        NOT NULL DEFAULT 'pending',
    priority       INT         NOT NULL DEFAULT 0,
    run_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts       INT         NOT NULL DEFAULT 0,
    max_attempts   INT         NOT NULL DEFAULT 5,
    locked_by      TEXT,
    locked_at      TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    lock_version   INT         NOT NULL DEFAULT 0,
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_jobs_claimable ON jobs (queue_name, priority DESC, run_at)
WHERE status = 'pending';
```

**Every column exists for a reason:**

| Column | Why it exists |
|---|---|
| `queue_name` | Isolates different job types so they can be prioritised or consumed independently |
| `payload` (JSONB) | Self-contained job data — a worker never needs to fetch extra context to execute |
| `status` | Drives the state machine: `pending` → `running` → `done` / `dead` |
| `priority` | Determines claim order among pending jobs (highest first) |
| `run_at` | Double duty: "don't claim before this time" for delayed jobs, and "retry not-before" for backoff |
| `attempts` / `max_attempts` | Drives the retry-vs-DLQ decision |
| `locked_by` / `locked_at` | Identifies which worker holds a job, and since when |
| `last_heartbeat` | Liveness signal for the reaper |
| `lock_version` | Fencing token — prevents zombie workers from corrupting state |
| `last_error` | Preserves failure context for debugging dead jobs |
| `created_at` | Audit trail |

**Why the partial index?** The claim query only ever searches for `pending` rows. Without `WHERE status = 'pending'`, the index balloons to include every `done`, `dead`, and `running` row — pure overhead that grows unboundedly. The partial index stays small and fast regardless of table history.

---

## Performance

**Benchmark tool:** [`cmd/bench/main.go`](cmd/bench/main.go)

Each round enqueues 1,000 jobs into an isolated queue, then drains them with N worker goroutines. Jobs are claimed and immediately completed (no simulated work), so the numbers reflect **raw queue throughput**: the cost of the claim CTE + completion UPDATE round-trips to Postgres.

| Workers | Throughput | p50 latency | p95 latency | p99 latency |
|--------:|-----------:|------------:|------------:|------------:|
| 1 | **2,233 jobs/sec** | 380µs | 508µs | 573µs |
| 2 | **3,022 jobs/sec** | 543µs | 647µs | 732µs |
| 4 | **4,005 jobs/sec** | 769µs | 1.0ms | 1.1ms |
| 8 | **5,068 jobs/sec** | 1.1ms | 1.5ms | 2.0ms |
| 16 | **5,775 jobs/sec** | 1.9ms | 2.5ms | 3.2ms |

**What the numbers show:**
- **Peak: 5,775 jobs/sec** at 16 workers (2.6× over single-worker baseline)
- Near-linear scaling from 1→4 workers, then diminishing returns at 8→16 as Postgres row-lock contention becomes the bottleneck
- Sub-millisecond per-job latency at low concurrency (380µs p50 at 1 worker)
- Every round verified **zero duplicate claims** and **zero leftover jobs**

This is consistent with the design's known scale ceiling: a single Postgres table handles thousands of jobs/sec comfortably, but row-lock contention becomes visible at higher concurrency. Beyond this, horizontal database partitioning or a move to a dedicated broker (Kafka, SQS) would be the next step.

Full report with environment details: [`BENCHMARK.md`](BENCHMARK.md)

---

## Testing

**17 tests** across unit and integration suites, all passing. Full details in [`TESTING.md`](TESTING.md).

### Unit tests (4 tests)
Verify the backoff calculation in isolation — `pow2` correctness, exponential growth bounds at each attempt, the 600-second cap, and non-negativity.

### Integration tests (13 tests, against live Postgres)

| Test | What it proves |
|---|---|
| `TestConcurrentClaims_ZeroDuplicates` | 8 workers race to claim 100 jobs — zero duplicates, all completed |
| `TestComplete_StaleLockVersion` | Zombie worker's stale `Complete` silently no-ops (fencing token works) |
| `TestReaper` | Stale heartbeat → job reset to `pending` → re-claimable by new worker |
| `TestReaper_DoesNotReapFreshJobs` | Fresh heartbeat → reaper correctly ignores |
| `TestFail_RetryWithBackoff` | Failed job resets to `pending` with future `run_at` and `last_error` |
| `TestFail_DLQ` | `attempts >= max_attempts` → status transitions to `dead` |
| `TestClaimPriorityOrder` | Jobs claimed in descending priority order |
| `TestDelayedJob` | Future `run_at` → `Claim()` returns nil (job invisible until scheduled) |
| `TestEnqueueAndClaim` | Basic lifecycle: enqueue → claim → status=running, version=1 |
| `TestComplete` | Completing with correct version → status=done in DB |
| `TestHeartbeat` | Heartbeat advances `last_heartbeat` timestamp |
| `TestHeartbeat_StaleLockVersion` | Heartbeat with wrong version is silent no-op |
| `TestEnqueue_EmptyPayload` | Nil payload → error (validates NOT NULL constraint) |

```bash
# Run all tests
DATABASE_URL="postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable" \
  go test -v -count=1 ./internal/job/
```

---

## What the Live Execution Proved

Beyond automated tests, the system was manually exercised with two separate worker processes (`w1`, `w2`) with 3 goroutines each, processing a burst of 10 enqueued jobs:

- **Zero duplicate claims** — 6 goroutines across 2 processes claimed 6 different jobs simultaneously (at 22:46:44, w1#1→11, w1#0→16, w1#2→15, w2#0→20, w2#1→19, w2#2→18 — zero overlap)
- **Reaper recovery** — jobs stuck from an earlier crashed run reappeared as `attempt=3/5`, `4/5`, `5/5` after the reaper reset them
- **Fencing correctness** — stale writes from old workers no-oped silently; no corrupted states observed
- **Backoff visible** — failed job retry timestamps show increasing, randomized gaps (12s, 5s between retries for job 1)
- **DLQ reached** — job 1 failed on all 5 attempts and transitioned to `status='dead'`
- **Clean shutdown** — `Ctrl+C` on both workers produced `shut down cleanly` after finishing in-progress work

Full execution log: [`execution.txt`](execution.txt)

---

## What's Left

- **Scheduler** — `run_at` supports delayed jobs already, but there is no background loop for recurring/cron-scheduled jobs
- **Dashboard** — no `/stats` endpoint or CLI status tool for queue health observability

Tracked in [`TODO.md`](TODO.md).

---

## Project Structure

```
jqs/
├── cmd/
│   ├── api/            # HTTP enqueue server
│   │   └── main.go
│   ├── worker/         # Worker process (poll, claim, execute, heartbeat, reaper)
│   │   └── main.go
│   └── bench/          # Benchmark tool (throughput + latency sweep)
│       └── main.go
├── internal/
│   ├── db/             # Postgres connection pool (pgxpool)
│   │   └── db.go
│   └── job/            # All job-queue logic (enqueue, claim, complete, fail, reap)
│       ├── job.go
│       ├── job_unit_test.go
│       └── job_integration_test.go
├── migrations/
│   └── 0001_init.sql   # Schema: jobs table + partial index
├── docker-compose.yml  # Postgres 16
├── BENCHMARK.md        # Auto-generated benchmark results
├── TESTING.md          # Testing & performance documentation
├── RUN.md              # How to build and run
└── TODO.md             # Remaining work
```

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Language | Go 1.22+ | Goroutines make the concurrency model natural; `pgx` is one of the fastest Postgres drivers |
| Database | PostgreSQL 16 | `FOR UPDATE SKIP LOCKED` enables the entire design |
| Postgres driver | `pgx/v5` (pgxpool) | High-performance, pool-safe, native Postgres wire protocol |
| Local infra | Docker Compose | One command to start Postgres with data persistence |
| HTTP server | `net/http` (stdlib) | Minimal dependency for a thin enqueue API |
| Signal handling | `os/signal` + `signal.NotifyContext` | Clean shutdown via context cancellation |
| Testing | `go test` + live Postgres | 17 tests (4 unit + 13 integration), zero external test frameworks |
| Benchmarking | Custom `cmd/bench` tool | Automated throughput sweep with correctness verification |
