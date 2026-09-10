# Job Queue and Task Scheduler — Project Documentation
 
## 1. What This Project Is
 
A backend system where **producers** submit units of work ("jobs") to be executed later, and a pool of independent **workers** picks up those jobs and runs them — safely, even when:
 
- Multiple workers are running at the same time
- A worker crashes mid-job
- A job fails and needs to be retried, but not immediately and not forever
- Some jobs are more urgent than others
- Some jobs need to run on a schedule (once, later, or repeatedly)
 
It is a smaller, self-built version of what production systems like Sidekiq, Celery, or AWS SQS do internally. The entire point of building it yourself — rather than just using one of those — is to understand *why* each piece of the design exists, by having personally reasoned through the failure mode it solves.
 
**Language:** Go
**Database:** PostgreSQL
**Local demo strategy:** Docker Compose (Postgres) + multiple independent Go worker *processes* simulating a distributed set of workers on one machine.
 
---
 
## 2. Why This Problem Exists At All
 
Consider a request that needs to send 10,000 emails, or process a large file, or call a slow third-party API. Doing this synchronously — inside the HTTP request that triggers it — would block the caller for minutes, risk timeouts, and tie up a request-handling thread for no good reason.
 
The standard fix: record the work as a **job** in durable storage, respond to the caller immediately, and let something else — a separate pool of workers — pick up and execute that work asynchronously, with the ability to retry it if it fails.
 
Every non-trivial backend system eventually needs this pattern. Building it from scratch, rather than just using a managed queue, is what makes this project valuable as a learning exercise and as a portfolio piece: it forces you to solve the underlying coordination and reliability problems yourself, instead of trusting a library to have solved them.
 
---
 
## 3. Core Architecture
 
```
Producer (HTTP API) ──> jobs table (Postgres) <── Worker processes (poll, claim, execute)
                              ▲
                    Scheduler (promotes delayed/recurring jobs)
                              │
                              ▼
                    Dead Letter Queue (permanently failed jobs)
```
 
Four logical components:
 
1. **Enqueue API** — a minimal HTTP endpoint that inserts a new row into the `jobs` table.
2. **Worker processes** — independently running instances of the same Go binary. Each runs a pool of goroutines that continuously try to claim and execute jobs.
3. **Reaper** — a background loop (running inside every worker process) that reclaims jobs abandoned by crashed workers.
4. **Scheduler** — a background loop that promotes jobs whose scheduled time has arrived (or recurring jobs, per a cron expression) into the live queue.
 
---
 
## 4. Why Postgres, Not Redis/Kafka/SQS
 
This is a question worth being able to answer precisely, because it's the first thing a reviewer or interviewer is likely to ask.
 
**Reasons Postgres works well here:**
- Job state changes are transactional (ACID) — you never end up in a state where "is this job claimed?" bookkeeping is left half-updated.
- Postgres's `SELECT ... FOR UPDATE SKIP LOCKED` gives genuine multi-consumer concurrency without needing a separate message broker.
- Operationally simpler: one system to run, not a database plus a queue service.
 
**Where this stops being the right choice:**
At very high throughput (tens of thousands of jobs/sec) or when true horizontal sharding across machines is required, a single Postgres table becomes a bottleneck — every claim touches the same table, and constant updates create vacuum/bloat pressure. This is exactly why systems like Kafka or SQS exist: they're built to shard and scale past what one relational table naturally handles.
 
**This project deliberately does not attempt to prove behavior at that scale.** That's an explicit, acknowledged limitation, not a gap being hidden — see Section 11.
 
---
 
## 5. The Database Schema, and Why Each Column Exists
 
```sql
CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL DEFAULT 'default',
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending, running, done, failed, dead
    priority INT NOT NULL DEFAULT 0,
    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    locked_by TEXT,
    locked_at TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    lock_version INT NOT NULL DEFAULT 0,      -- fencing token, see Section 8
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
 
CREATE INDEX idx_jobs_claimable ON jobs (queue_name, priority DESC, run_at)
WHERE status = 'pending';
```
 
| Column | Why it exists |
|---|---|
| `queue_name` | Lets different job types be isolated/prioritized separately later |
| `payload` (JSONB) | Flexible, self-contained job data — a worker never needs to fetch extra context to execute a job |
| `status` | Drives the whole state machine: pending → running → done/failed → (retry or dead) |
| `priority` | Determines claim order among pending jobs |
| `run_at` | Both "don't claim before this time" (delayed jobs) and "retry not-before" (backoff) |
| `attempts` / `max_attempts` | Drives the retry-vs-DLQ decision |
| `locked_by` / `locked_at` | Identifies which worker holds a job, and since when |
| `last_heartbeat` | Liveness signal — see Section 7 |
| `lock_version` | Fencing token — prevents a "zombie" worker from corrupting state — see Section 8 |
| `last_error` | Preserves failure context for debugging dead jobs |
 
**Why the partial index (`WHERE status = 'pending'`)?**
The claim query only ever searches for pending jobs. Without the partial condition, the index would grow to include millions of `done` rows over time, which is pure overhead. The partial index keeps it small and fast indefinitely, regardless of how much history accumulates in the table.
 
---
 
## 6. The Claim Mechanism: `SELECT ... FOR UPDATE SKIP LOCKED`
 
### The problem it solves
 
If N workers each run a naive `SELECT ... LIMIT 1` to find the next job, they can all read the *same* row before any of them updates it — resulting in the same job being processed multiple times (duplicate side effects, e.g. duplicate emails sent).
 
### Why not just use `FOR UPDATE` (without SKIP LOCKED)?
 
`FOR UPDATE` alone solves the duplication problem — it locks the row so a second transaction attempting to select it will *block* until the first transaction finishes. But this reintroduces a different problem: if multiple workers all try to claim starting from the same top-priority row, they queue up behind each other's lock instead of working in parallel. You've solved correctness by accidentally serializing what should be concurrent work.
 
### What `SKIP LOCKED` adds
 
It changes the instruction from "wait for the lock" to "if this row is already locked, skip it and consider the next candidate." Now:
- Worker A locks row 1
- Worker B (near-simultaneously) sees row 1 is locked, skips to row 2, locks it
- Worker C skips 1 and 2, locks row 3
 
All three workers claim **different** jobs in parallel, with zero blocking. This is the mechanism that makes a relational database behave like a real concurrent queue.
 
**In one sentence:** `FOR UPDATE` gives correctness (no duplicate claims); `SKIP LOCKED` gives back the concurrency that plain locking would otherwise destroy.
 
### Why the claim is one atomic statement, not SELECT then UPDATE
 
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
    last_heartbeat = now(), lock_version = lock_version + 1
FROM next_job
WHERE jobs.id = next_job.id
RETURNING jobs.*;
```
 
If the select and the update were two separate statements, there would be a gap between them — a **TOCTOU (time-of-check to time-of-use) window** — during which another transaction could interleave. Combining them into a single atomic statement (via a CTE) removes that gap entirely.
 
**General principle worth internalizing:** whenever a system "checks something, then acts on it" as two separate steps, ask whether another actor could act in the gap between them. If yes, the check and the action need to be collapsed into one atomic operation.
 
---
 
## 7. Heartbeat + Reaper: Surviving Worker Crashes
 
### The problem
 
Locking a row and marking it `running` solves duplication — but what if the worker holding that lock crashes before finishing (process killed, machine failure, network partition)? Nothing else in the system will ever revisit that row. The job is silently lost, stuck in `running` forever.
 
### The fix
 
While a worker holds a job, it periodically writes `last_heartbeat = now()` — a liveness signal, "I'm still alive and working on this." A separate background process, the **reaper**, periodically scans for jobs marked `running` whose heartbeat has gone stale beyond some threshold, and resets them to `pending` so another worker can claim them.
 
This is the same underlying pattern used in real distributed systems for **lock leases** — Kubernetes leader election, distributed cron, Zookeeper/etcd locks all use a variant of "the lock isn't held forever by default; it must be actively renewed or it expires."
 
### The fundamental tradeoff this creates: at-least-once, not exactly-once
 
If a worker finishes real work but crashes *before* marking the job `done`, the reaper will eventually reclaim it and another worker will run it again. This means job handlers should ideally be **idempotent** (safe to execute twice) if duplicate side effects matter in a real system. This project surfaces that tradeoff deliberately rather than hiding it — it's a foundational distributed-systems concept, not a bug in this specific design.
 
### Threshold tuning is a real tradeoff, not a fixed "correct" number
 
- Too short (e.g., 5s): false positives — a job that's just briefly slow (GC pause, slow API call) gets prematurely reclaimed and duplicated.
- Too long (e.g., 10 minutes): a genuinely crashed worker's job sits stuck for a long time before recovery.
- Reasonable rule of thumb: set the reaper threshold to 3–5x the heartbeat interval (e.g., heartbeat every 10s → reap after 30–50s of silence).
 
### Why the reaper is safe to run redundantly on every worker process
 
The reaper's operation is just a conditional `UPDATE ... WHERE last_heartbeat < threshold`. Running this same query on multiple worker processes simultaneously is harmless — the `WHERE` clause naturally prevents double-reclaiming, since once one instance's update runs, the row no longer matches the condition for anyone else. This means the reaper doesn't need leader election or any special "only one reaper" coordination — unlike the *workers'* claim logic, which very much needs coordination (`SKIP LOCKED`) to avoid duplication.
 
---
 
## 8. Fencing Tokens: Closing the Loop on Heartbeat Edge Cases
 
### The scenario this solves
 
A worker pauses (GC pause, slow disk, brief network blip) for slightly longer than the reaper's threshold. The reaper — reasonably, based on the information it has — assumes the worker is dead and reclaims the job. A second worker picks it up and starts processing. Meanwhile, the *original* worker resumes and, unaware it's been reclaimed, finishes and writes a "done" result.
 
Now the job has been processed twice, and possibly by two different workers concurrently near the end — the exact duplication problem `SKIP LOCKED` was meant to prevent, sneaking back in through the reaper.
 
**Why this can't be fully avoided by tuning alone:** liveness detection via heartbeats can never perfectly distinguish "the worker is dead" from "the worker is alive but silent." This is a fundamental limitation of distributed liveness detection, not a solvable bug.
 
### The fix: fencing tokens
 
Add `lock_version`, incremented every time a job is claimed. Any write a worker makes to "complete" a job is conditioned on `WHERE lock_version = <the version it claimed with>`. If the reaper reclaimed the job (which bumps the version) before the original worker's stale completion write arrives, that write matches zero rows and silently no-ops — instead of corrupting the job's state.
 
This is the same pattern used by real distributed lock systems (e.g., Chubby, etcd) to convert a probabilistic liveness signal into a hard correctness guarantee. **This is arguably the single most important correctness mechanism in the whole design**, because it's what makes the system safe even when the heartbeat/reaper mechanism itself gets a liveness judgment wrong.
 
---
 
## 9. Retries: Exponential Backoff With Jitter
 
### Why not retry immediately?
 
If a job fails because a downstream dependency is struggling, retrying instantly just hits the same struggling system again — often with *more* concurrent load, since multiple workers may be retrying at once.
 
### Why exponential backoff
 
Each retry waits longer than the last (e.g., doubling), giving a failing dependency time to recover instead of being continuously hammered.
 
### Why add jitter (randomness)
 
Without randomness, many jobs that failed at the same moment (e.g., during a brief outage) would all retry at the *exact same future instant* — recreating the very overload that caused the failure in the first place. This is called a **thundering herd**. Adding randomness spreads retries across a window instead of a single spike, letting a recovering system see a gradual ramp in load rather than a sudden burst.
 
---
 
## 10. Dead Letter Queue (DLQ)
 
Not every failure is transient. A malformed payload will fail identically no matter how many times it's retried — continuing to retry it wastes worker time and clutters the active queue.
 
Once a job's `attempts` reaches `max_attempts`, it transitions to a terminal `dead` status instead of being retried again. The job (and its `last_error`) is preserved, not deleted, so a human can inspect what went wrong and manually replay it if appropriate.
 
**Why this matters conceptually:** it's the difference between a system that *silently loses* unrecoverable work and a system that makes failures visible and recoverable. This is what turns "retry logic" into a genuine reliability story.
 
---
 
## 11. Local Demo Strategy: Why Multiple Processes, Not Just Goroutines
 
### Why not just use goroutines within one process?
 
Multiple goroutines inside a single Go process share memory — which means you *could* coordinate them with a `sync.Mutex` or semaphore instead of `SKIP LOCKED`. But this would prove a much weaker, less relevant claim: "I can coordinate threads that already share memory." It wouldn't demonstrate the actual problem this project exists to solve.
 
### Why the project runs multiple independent OS processes instead
 
Real production job-queue systems run workers as separate instances — separate pods, containers, or machines — specifically to scale horizontally and survive individual failures. Those workers **do not share memory**, and cannot use in-process primitives like mutexes to coordinate at all. The database becomes the *only* shared coordination point.
 
Running several separately-compiled instances of the same Go worker binary, all independently connecting to one Postgres container, faithfully simulates this — even though everything is happening on one laptop. It proves the coordination mechanism (`SKIP LOCKED` + fencing tokens) works across independent actors that don't share memory, which is the real claim this project needs to substantiate.
 
### What this local setup can and cannot prove
 
**Can prove, credibly:**
- Concurrency correctness under multiple independent workers (the core value of the project)
- Recovery from a crashed worker (via `kill -9` + observing the reaper)
- Real throughput/latency numbers, *for this hardware and this worker count*
 
**Cannot prove, and shouldn't be presented as tested:**
- True multi-datacenter or network-partition behavior between real separate machines
- Behavior at a scale where a single Postgres table becomes a genuine bottleneck
 
The correct way to handle the second category is to **state it as a known, reasoned-about limitation** ("at very high throughput this design would need sharding or a move to a broker like Kafka, because row-lock contention on one table becomes the bottleneck") rather than to either fake testing it or ignore the question.
 
---
 
## 12. Explicit Assumptions
 
Being clear about assumptions matters because they're exactly where a project can silently break, or where an interviewer can probe to see if you understand your own system's boundaries.
 
**Infrastructure assumptions**
- Postgres is reachable and doesn't fail mid-operation; we are not building for database high-availability/failover.
- Postgres's own clock is trustworthy (we always use its `now()`, never an application server's local clock, to avoid clock-skew issues between machines).
- The local machine has sufficient CPU/RAM to run Postgres plus several worker processes comfortably — this is a light workload, not a concern in practice.
 
**Design assumptions**
- At-least-once execution is the accepted model, not exactly-once; idempotency of actual job handlers is treated as the application's responsibility, not the queue's.
- Job payloads are fully self-contained; a worker never needs to fetch additional context mid-execution.
- Job "execution" in the demo is simulated (e.g., sleep + randomized success/failure) rather than real handlers (send email, call an API) — the project proves the *queue mechanics*, not a specific downstream integration.
- Timing thresholds (heartbeat interval, reaper timeout, backoff base) are reasoned defaults, not values tuned against real production traffic, since no such traffic exists for this project.
 
**Scope assumptions**
- No authentication/authorization on the enqueue API.
- No multi-tenancy.
- Any dashboard/UI added is functional, not production-polished.
- Failure simulation is manual (`kill -9`), not automated chaos testing.
 
---
 
## 13. Summary Table: Each Mechanism and the Failure Mode It Solves
 
| Mechanism | Failure mode it prevents | Alternative considered | Why rejected |
|---|---|---|---|
| `FOR UPDATE SKIP LOCKED` | Duplicate job claims across concurrent workers | Plain `FOR UPDATE` | Serializes workers instead of allowing parallel claims |
| Atomic CTE claim query | TOCTOU race between checking and claiming | Separate SELECT then UPDATE | Leaves a gap another transaction could exploit |
| Heartbeat + reaper | Jobs lost forever when a worker crashes mid-execution | No recovery mechanism | Job would be stuck in `running` permanently |
| Fencing token (`lock_version`) | Zombie worker corrupting state after being reclaimed | Heartbeat/reaper alone | Can't fully distinguish "dead" from "slow"; needs a correctness backstop |
| Exponential backoff | Hammering a failing dependency with immediate retries | Immediate retry | Worsens the outage it's reacting to |
| Jitter | Synchronized retry storms ("thundering herd") | Pure exponential backoff, no randomness | All failed jobs retry at the same instant |
| Dead letter queue | Infinite retries on permanently-broken jobs | Retry forever | Wastes resources, hides unrecoverable failures |
| Multi-process local demo | Overstating what goroutine-only concurrency proves | Multiple goroutines in one process | Doesn't require real cross-process coordination; mutex would trivially "solve" it without proving anything relevant |
 
---
 
## 14. What "Done" Looks Like
 
A working system where you can:
1. Enqueue a burst of jobs and watch several independent worker processes claim and process them concurrently with zero duplicates
2. Kill a worker process mid-job and watch the reaper recover the job, with the fencing token preventing any corruption if the killed process partially resumes
3. Force repeated failures on a job and watch it retry with growing, jittered delays before landing in the dead letter queue
4. Produce a real benchmark number (jobs/sec at N worker processes, p99 claim-to-completion latency) from an actual load test
5. Explain, for every mechanism above, what specific failure it prevents and why the alternative approaches were rejected