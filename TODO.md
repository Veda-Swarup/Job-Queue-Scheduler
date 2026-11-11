# JQS — TODO

## Benchmarking
- [x] Write a load test script that enqueues 1000+ jobs in a burst
- [x] Measure jobs/sec throughput at different worker counts (1, 2, 4, 8)
- [x] Measure p99 claim-to-completion latency
- [x] Record results in a benchmark report with actual numbers

## Observability
- [x] Build a CLI tool or simple dashboard that shows job status counts (`SELECT status, count(*) FROM jobs GROUP BY status`)
- [x] Add a `/stats` endpoint to the API that returns queue depth, running count, dead count
- [x] Log total jobs processed and average processing time on worker shutdown

## Testing
- [x] Write an integration test in Go that:
  - Spins up multiple worker goroutines
  - Enqueues N jobs
  - Asserts all jobs complete with zero duplicates
  - Asserts failed jobs retry correctly
  - Asserts max_attempts exhaustion sends jobs to DLQ
- [x] Write a unit test for the backoff + jitter calculation
- [x] Write a test that simulates reaper recovery (claim a job, don't heartbeat, verify reaper resets it)

## Documentation
- [ ] Create a polished README.md with:
  - Architecture diagram
  - Quick start instructions
  - Sample terminal output showing workers claiming jobs
  - Summary table of mechanisms and failure modes (from about.md Section 13)
- [ ] Add a DESIGN.md extracted from about.md for deep-dive reading

## Stretch Goals
- [x] Add a `/jobs/:id` endpoint to inspect a specific job's status and history
- [x] Add a `/replay` endpoint to re-enqueue dead jobs
- [ ] Add structured JSON logging (instead of plain log.Printf)
- [x] Add Prometheus metrics (jqs_queue_depth, jqs_enqueued_total, jqs_worker_active_jobs, jqs_active_workers)
- [ ] Add graceful drain — on shutdown, finish in-progress jobs but stop claiming new ones (already partially done via signal.NotifyContext)

