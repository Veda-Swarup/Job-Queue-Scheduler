# JQS — Job Queue Scheduler

A PostgreSQL-backed job queue written in Go. Independent worker processes claim jobs through `FOR UPDATE SKIP LOCKED`, with scheduled execution, priorities, heartbeats, retries, and dead-job replay.

This is a learning/demo project. The worker simulates work for roughly 50–2,000 ms and randomly fails about 20% of attempts; replace `simulateWork` in `cmd/worker/main.go` to implement useful handlers. Delivery is at least once, so handlers need to tolerate repeated execution.

## Quick start

Requires Go 1.26.5 or newer (see `go.mod`), Docker with Compose, and curl. Run these commands from the repository root:

```sh
docker compose up -d --wait
export DATABASE_URL='postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable'
docker compose exec -T postgres psql -U jobqueue -d jobqueue -v ON_ERROR_STOP=1 < migrations/0001_init.sql
go run ./cmd/api
```

Apply the initial migration once per fresh database; it is not idempotent. Migrations are manual and are not run by Compose or the binaries. The explicit `DATABASE_URL` is required for this setup: Compose exposes port **5433**, while the application fallback uses **5432**. `.env` files are not automatically loaded by the Go programs.

In another terminal, export the same `DATABASE_URL` and start a worker:

```sh
export DATABASE_URL='postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable'
go run ./cmd/worker -id=w1 -queue=default -concurrency=4
```

Submit a job and inspect queue counts:

```sh
curl -sS http://localhost:8080/enqueue \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"action":"demo"},"queue":"default","priority":5,"max_attempts":3}'
curl -sS 'http://localhost:8080/stats?queue=default'
curl -sS http://localhost:8080/jobs/1
```

The returned job ID depends on existing data; use it in place of `1`. Start additional workers with distinct `-id` values to consume the same queue. Stop processes with Ctrl+C and stop PostgreSQL with `docker compose down`; the named volume retains data.

## HTTP API

The server listens on `:8080`.

| Method | Path | Behavior |
| --- | --- | --- |
| POST | `/enqueue` | Submit JSON containing `payload`; returns `{"id":123}`. |
| GET | `/jobs/{id}` | Inspect a job; returns 404 if absent. |
| GET | `/stats?queue=default` | Counts by status; omit `queue` for all queues. |
| GET | `/metrics` | Prometheus text metrics for status counts and active worker slots. |
| POST | `/replay` | Body `{"id":123}` resets a dead job in place; 404 if absent or not dead. |

Enqueue options: `queue` defaults to `default`, `priority` defaults to `0` (higher runs first), `run_at` is an optional RFC 3339 timestamp, and a positive `max_attempts` overrides the default of `5`. Omitted `run_at` means immediately eligible. Enqueue and replay return HTTP 200 on success.

Job inspection currently uses Go field names such as `ID` and `QueueName`; `Payload` is a byte slice and is JSON-encoded as base64. It is not the same JSON shape as the enqueue request.

## Worker configuration

```sh
go run ./cmd/worker -help
```

| Flag | Default | Purpose |
| --- | --- | --- |
| `-id` | hostname + PID | Process identifier; slots append `#N`. |
| `-queue` | `default` | Queue to consume. |
| `-concurrency` | `4` | Worker goroutines per process. |
| `-poll` | `500ms` | Delay after an empty claim or claim error. |
| `-heartbeat` | `10s` | Liveness update interval during execution. |
| `-reaper` | `15s` | Stale-job recovery interval. |
| `-stale` | `45s` | Heartbeat age considered stale. |

Use positive concurrency and durations; keep the stale threshold comfortably above the heartbeat interval.

## Development and validation

```sh
go build ./...
go test -v -count=1 ./...
```

Integration tests require a reachable, migrated PostgreSQL database. They use `DATABASE_URL`, falling back to localhost port 5433, and **skip** when they cannot connect. Check verbose output for skips before treating a green test run as database validation. Use a disposable development database: tests create and delete queue rows, and reaper tests operate across queues, so stop other workers first.

The benchmark measures database queue operations rather than the simulated worker handler:

```sh
go run ./cmd/bench -jobs=1000 -levels=1,2,4,8,16 -queue=bench
```

Each benchmark round deletes existing jobs in its selected queue. Run against a disposable database and a dedicated queue. Throughput depends on hardware, connections, workload, and database state; no universal throughput guarantee is implied.

## Project layout

- `cmd/api`: HTTP submission, inspection, replay, and metrics.
- `cmd/worker`: polling workers, simulated handler, heartbeats, and reaper.
- `cmd/bench`: concurrency benchmark.
- `internal/job`: SQL operations and queue tests.
- `internal/db`: shared connection-pool setup.
- `migrations`: initial schema and partial claim index.

See [ARCHITECTURE.md](ARCHITECTURE.md) for state transitions, SQL coordination, and correctness limits.

## Deployment scope

The supplied credentials and disabled TLS are for local development. The API has no authentication, authorization, rate limiting, request-size limit, or configured HTTP timeouts; Compose publishes PostgreSQL on the host. Restrict network access before running this outside a trusted local environment. Workers log payloads, which may contain sensitive data.

Further production work includes real idempotent handlers, stronger ownership checks, graceful shutdown, schema constraints, migrations tooling, monitoring, retention, and operational tests. See the architecture document for current recovery limitations.

## License

Licensed under the [MIT License](LICENSE).
