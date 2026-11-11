# JQS — Benchmark Report

**Date:** 2026-10-01 23:04:23 IST  
**Go version:** go1.26.5  
**OS/Arch:** darwin/arm64  
**GOMAXPROCS:** 12  
**Jobs per round:** 1000  
**Database:** PostgreSQL 16 via Docker (localhost:5433)  
**Job work:** none (claim → immediate complete)  

---

## What This Measures

Each round enqueues N jobs into a dedicated `bench` queue, then drains them
with the specified number of worker goroutines. Jobs are claimed and immediately
completed — no simulated work — so the numbers reflect raw queue throughput:
the cost of `SELECT … FOR UPDATE SKIP LOCKED` + `UPDATE` round-trips to Postgres.

**Latency** = wall-clock time from `Claim()` start to `Complete()` return for a
single job (includes both Postgres round-trips: the CTE claim + the status update).

---

## Results

| Workers | Jobs | Total Time | Jobs/sec | p50 | p95 | p99 | Min | Max | Mean |
|--------:|-----:|-----------:|---------:|----:|----:|----:|----:|----:|-----:|
| 1 | 1000 | 448ms | **2233.3** | 380µs | 508µs | 573µs | 317µs | 3.174ms | 396µs |
| 2 | 1000 | 331ms | **3021.8** | 543µs | 647µs | 732µs | 440µs | 4.072ms | 559µs |
| 4 | 1000 | 250ms | **4004.5** | 769µs | 1.014ms | 1.119ms | 558µs | 5.109ms | 794µs |
| 8 | 1000 | 197ms | **5068.4** | 1.137ms | 1.485ms | 1.956ms | 718µs | 7.505ms | 1.166ms |
| 16 | 1000 | 173ms | **5775.4** | 1.894ms | 2.458ms | 3.241ms | 1.198ms | 6.945ms | 1.947ms |

---

## Analysis

- **Peak throughput:** 5775.4 jobs/sec at 16 workers (2.6x over single-worker)
- **Single-worker baseline:** 2233.3 jobs/sec

### What these numbers mean

The throughput is bounded by Postgres round-trip latency — each job requires
two sequential queries (claim CTE + complete UPDATE). Adding workers improves
throughput by overlapping these round-trips, up to the point where `FOR UPDATE
SKIP LOCKED` contention or Postgres connection limits become the bottleneck.

At very high worker counts, you may see throughput plateau or decrease — this
is the natural ceiling of a single Postgres table as a queue, and is the point
where a dedicated broker (Kafka, SQS) would outperform this design.

---

## Correctness Verification

Every round verifies:
- ✅ All enqueued jobs reached `status = 'done'`
- ✅ Zero duplicate claims (no job ID appears more than once in `done` status)
- ✅ Zero jobs left in `pending` or `running` state after drain
