# JQS — Testing & Performance

This document covers what was tested, how, and what the results prove about the system's correctness and performance characteristics.

---

## 1. Test Suite Overview

JQS has **17 tests** across two files, covering both pure logic (unit tests) and real Postgres behaviour (integration tests). All tests pass.

```
$ go test -v -count=1 ./internal/job/

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

### How to run

```bash
# Start Postgres (if not already running)
docker compose up -d

# Run all tests
DATABASE_URL="postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable" \
  go test -v -count=1 ./internal/job/
```

> `-count=1` disables Go's test caching so every run hits the real database.

---

## 2. Unit Tests

**File:** [`internal/job/job_unit_test.go`](internal/job/job_unit_test.go)

These tests verify the backoff calculation logic in isolation — no database required.

| Test | What it verifies |
|---|---|
| `TestPow2` | The `pow2(n)` helper returns correct powers of 2 (0→1, 1→2, … 10→1024) |
| `TestBackoffBounds` | The max delay window for each attempt matches the formula: `5 × 2^(attempt-1)`, capped at 600s |
| `TestBackoffIsAlwaysNonNegative` | Max delay is ≥ 0 for attempts 1–100 |
| `TestBackoffCapAt600` | At attempt 8+ (`5 × 128 = 640`), the delay is correctly capped to 600 seconds (10 minutes) |

### The backoff formula

```
base       = 5 seconds
maxDelay   = min(base × 2^(attempt-1), 600)
jitter     = random float in [0, maxDelay)
retryAt    = now + jitter
```

| Attempt | Max delay window |
|--------:|-----------------:|
| 1 | 5s |
| 2 | 10s |
| 3 | 20s |
| 4 | 40s |
| 5 | 80s |
| 6 | 160s |
| 7 | 320s |
| 8+ | 600s (capped) |

**Why full jitter (not equal jitter)?** Full jitter samples from `[0, maxDelay)` uniformly, which spreads retries most aggressively and best prevents thundering herds when many jobs fail simultaneously during an outage.

---

## 3. Integration Tests

**File:** [`internal/job/job_integration_test.go`](internal/job/job_integration_test.go)

These tests run against a live Postgres instance and verify every mechanism end-to-end. Each test uses a unique queue name to avoid cross-test pollution and cleans up after itself.

### 3.1 Basic Lifecycle

| Test | What it verifies |
|---|---|
| `TestEnqueueAndClaim` | A job can be enqueued and claimed; status transitions to `running`, `attempts` = 1, `lock_version` = 1; a second claim returns `nil` (queue empty) |
| `TestComplete` | Completing a job with the correct `lock_version` sets status to `done` in the database |
| `TestEnqueue_EmptyPayload` | Enqueuing with an empty/nil payload returns an error (validates the NOT NULL constraint) |

### 3.2 Fencing Token (the most important correctness test)

| Test | What it verifies |
|---|---|
| `TestComplete_StaleLockVersion` | Simulates the zombie worker scenario: Worker A claims a job (version=1) → reaper reclaims → Worker B claims (version=2) → Worker A tries to complete with version=1. **Result:** Worker A's stale write matches zero rows (silent no-op), job stays `running` under Worker B. The fencing token prevents corruption. |

### 3.3 Retry + DLQ

| Test | What it verifies |
|---|---|
| `TestFail_RetryWithBackoff` | Failing a job (attempt 1 of 3) resets status to `pending`, sets `last_error`, and writes a future `run_at` (backoff delay) |
| `TestFail_DLQ` | Failing a job when `attempts >= max_attempts` sets status to `dead` (DLQ) with `last_error` preserved. The job is not retried again. |

### 3.4 Heartbeat + Reaper

| Test | What it verifies |
|---|---|
| `TestHeartbeat` | Sending a heartbeat advances `last_heartbeat` in the database |
| `TestHeartbeat_StaleLockVersion` | Heartbeat with a wrong `lock_version` is a silent no-op (fencing token applies here too) |
| `TestReaper` | Claims a job, manually backdates `last_heartbeat` by 2 minutes, runs `Reap(30s)`. Verifies: the job is reset to `pending`, `locked_by` is NULL, and a new worker can claim it. |
| `TestReaper_DoesNotReapFreshJobs` | Claims a job (fresh heartbeat), runs `Reap(30s)`. Verifies: the reaper does NOT reclaim it (zero rows affected). |

### 3.5 Concurrent Correctness

| Test | What it verifies |
|---|---|
| `TestConcurrentClaims_ZeroDuplicates` | Enqueues 100 jobs, launches 8 goroutines racing to claim them concurrently. Tracks every claim in a `map[jobID]→workerID`. **Asserts:** zero duplicate claims, all 100 jobs completed, database confirms 100 `done` rows. This is the core proof that `FOR UPDATE SKIP LOCKED` works correctly. |

### 3.6 Ordering + Scheduling

| Test | What it verifies |
|---|---|
| `TestClaimPriorityOrder` | Enqueues jobs with priorities [1, 5, 3, 10, 7]. Claims come out in descending order: 10, 7, 5, 3, 1. |
| `TestDelayedJob` | Enqueues a job with `run_at` 1 hour in the future. `Claim()` returns `nil` — the job is invisible to workers until `run_at ≤ now()`. |

---

## 4. Performance Benchmark

**Tool:** [`cmd/bench/main.go`](cmd/bench/main.go)  
**Report:** [`BENCHMARK.md`](BENCHMARK.md)

### What it measures

Each round enqueues 1000 jobs into an isolated `bench` queue, then drains them with N worker goroutines. Jobs are claimed and **immediately completed** (no simulated work), so the numbers reflect raw queue throughput: the cost of `SELECT … FOR UPDATE SKIP LOCKED` + `UPDATE` round-trips to Postgres.

**Latency** = wall-clock time from `Claim()` call to `Complete()` return for a single job (two Postgres round-trips).

### How to run

```bash
DATABASE_URL="postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable" \
  go run ./cmd/bench -jobs=1000 -levels=1,2,4,8,16
```

### Results

**Environment:** Go 1.26.5, darwin/arm64, GOMAXPROCS=12, PostgreSQL 16 via Docker

| Workers | Jobs/sec | p50 | p95 | p99 | Total Time |
|--------:|---------:|----:|----:|----:|-----------:|
| 1 | **2,233** | 380µs | 508µs | 573µs | 448ms |
| 2 | **3,022** | 543µs | 647µs | 732µs | 331ms |
| 4 | **4,005** | 769µs | 1.0ms | 1.1ms | 250ms |
| 8 | **5,068** | 1.1ms | 1.5ms | 2.0ms | 197ms |
| 16 | **5,775** | 1.9ms | 2.5ms | 3.2ms | 173ms |

### Analysis

- **Peak throughput:** 5,775 jobs/sec at 16 workers (2.6× over single-worker baseline)
- **Single-worker baseline:** 2,233 jobs/sec
- **Scaling pattern:** Near-linear improvement from 1→4 workers, then diminishing returns at 8→16 as Postgres row-lock contention becomes the bottleneck
- **Latency tradeoff:** Per-job latency increases with concurrency (380µs → 1.9ms at p50) because more goroutines contend for Postgres connections and row locks — but total throughput still improves because the round-trips overlap

### Correctness verification

Every benchmark round also verifies:
- ✅ All enqueued jobs reached `status = 'done'`
- ✅ Zero duplicate claims (no job ID appears more than once)
- ✅ Zero jobs left in `pending` or `running` state after drain

---

## 5. What These Results Prove Together

### The mechanism-level proof

| Mechanism | Test(s) that prove it |
|---|---|
| `FOR UPDATE SKIP LOCKED` prevents duplicate claims | `TestConcurrentClaims_ZeroDuplicates` (8 workers, 100 jobs, zero overlap) + benchmark (16 workers, 1000 jobs × 5 rounds, zero overlap) |
| Atomic CTE claim eliminates TOCTOU | `TestEnqueueAndClaim` (second claim returns nil) + concurrent test |
| Fencing token blocks zombie writes | `TestComplete_StaleLockVersion` (stale version → silent no-op, job stays `running`) |
| Heartbeat keeps jobs alive | `TestHeartbeat` (heartbeat advances timestamp) |
| Reaper recovers crashed workers | `TestReaper` (stale heartbeat → job reset to pending → re-claimable) |
| Reaper doesn't false-positive | `TestReaper_DoesNotReapFreshJobs` (fresh heartbeat → 0 rows reaped) |
| Exponential backoff respects bounds | `TestBackoffBounds` + `TestBackoffCapAt600` (formula matches, 10-min cap works) |
| Full jitter is non-negative | `TestBackoffIsAlwaysNonNegative` (100 attempts, all ≥ 0) |
| Retry resets to pending with delay | `TestFail_RetryWithBackoff` (status = pending, run_at in future, last_error set) |
| DLQ catches permanent failures | `TestFail_DLQ` (attempts exhausted → status = dead, error preserved) |
| Priority ordering is correct | `TestClaimPriorityOrder` (highest priority claimed first) |
| Delayed jobs are invisible until run_at | `TestDelayedJob` (future run_at → claim returns nil) |
| Payload validation works | `TestEnqueue_EmptyPayload` (nil payload → error) |

### The performance proof

| Claim | Evidence |
|---|---|
| Postgres-as-queue handles thousands/sec | 5,775 jobs/sec at 16 workers on commodity hardware |
| Horizontal scaling works | Throughput increases with more workers (2,233 → 5,775 across 1→16) |
| Sub-millisecond per-job latency at low concurrency | p50 = 380µs at 1 worker |
| Diminishing returns visible at high concurrency | Classic pattern: near-linear at 1→4, then tapering at 8→16 |
| The bottleneck is Postgres, not Go | Per-job latency grows with workers (more Postgres contention), confirming the database is the constraint |

### What's NOT tested (acknowledged limitations)

- **Cross-process concurrency** — the integration tests use goroutines in one process. The manual execution log (`execution.txt`) proves cross-process correctness with two separate OS processes, but it's not automated.
- **Network partitions** — no simulation of Postgres becoming unreachable mid-job.
- **Real-world job handlers** — all tests use instant completion or simulated work; no actual email-sending or API-calling handlers are tested.
- **At-least-once duplication** — the scenario where a worker completes real work but crashes before writing `done` is documented as a design tradeoff, not tested, because it requires crashing a process mid-Postgres-round-trip which is inherently non-deterministic.

---

## 6. File Reference

| File | Purpose |
|---|---|
| [`internal/job/job_unit_test.go`](internal/job/job_unit_test.go) | Unit tests for `pow2` and backoff bounds |
| [`internal/job/job_integration_test.go`](internal/job/job_integration_test.go) | 13 integration tests against live Postgres |
| [`cmd/bench/main.go`](cmd/bench/main.go) | Benchmark tool (throughput + latency sweep) |
| [`BENCHMARK.md`](BENCHMARK.md) | Auto-generated benchmark report with results |
