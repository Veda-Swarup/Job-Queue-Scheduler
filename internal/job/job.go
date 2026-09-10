// Package job contains all job-queue logic: enqueueing, atomic claiming,
// heartbeating, reaping stale jobs, completing, and failing with retry/DLQ.
package job

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job mirrors a row from the jobs table.
type Job struct {
	ID          int64
	QueueName   string
	Payload     []byte
	Status      string
	Priority    int
	RunAt       time.Time
	Attempts    int
	MaxAttempts int
	LockedBy    *string // nullable
	LockVersion int
	LastError   *string // nullable
	CreatedAt   time.Time
}

// EnqueueParams holds the fields a caller can set when submitting a job.
// Fields left at their zero value fall back to the database column defaults.
type EnqueueParams struct {
	QueueName   string
	Payload     []byte
	Priority    int
	RunAt       *time.Time // nil → run immediately (db default: now())
	MaxAttempts int        // 0 → db default (5)
}

// Enqueue inserts a new job row and returns the assigned ID. Step 3.
func Enqueue(ctx context.Context, pool *pgxpool.Pool, p EnqueueParams) (int64, error) {
	if len(p.Payload) == 0 {
		return 0, fmt.Errorf("payload must not be empty")
	}
	if p.QueueName == "" {
		p.QueueName = "default"
	}

	// Build the INSERT dynamically only for optional columns that were set,
	// letting the database apply its own defaults otherwise.
	var id int64
	switch {
	case p.RunAt != nil && p.MaxAttempts > 0:
		err := pool.QueryRow(ctx, `
			INSERT INTO jobs (queue_name, payload, priority, run_at, max_attempts)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id`,
			p.QueueName, p.Payload, p.Priority, *p.RunAt, p.MaxAttempts,
		).Scan(&id)
		return id, err

	case p.RunAt != nil:
		err := pool.QueryRow(ctx, `
			INSERT INTO jobs (queue_name, payload, priority, run_at)
			VALUES ($1, $2, $3, $4)
			RETURNING id`,
			p.QueueName, p.Payload, p.Priority, *p.RunAt,
		).Scan(&id)
		return id, err

	case p.MaxAttempts > 0:
		err := pool.QueryRow(ctx, `
			INSERT INTO jobs (queue_name, payload, priority, max_attempts)
			VALUES ($1, $2, $3, $4)
			RETURNING id`,
			p.QueueName, p.Payload, p.Priority, p.MaxAttempts,
		).Scan(&id)
		return id, err

	default:
		err := pool.QueryRow(ctx, `
			INSERT INTO jobs (queue_name, payload, priority)
			VALUES ($1, $2, $3)
			RETURNING id`,
			p.QueueName, p.Payload, p.Priority,
		).Scan(&id)
		return id, err
	}
}

// Claim atomically selects and locks the highest-priority pending job in the
// given queue whose run_at <= now(), marking it running and incrementing
// lock_version (the fencing token). Returns nil, nil when no job is available.
//
// The CTE collapses the select and update into one atomic statement —
// no TOCTOU gap between checking availability and claiming. Step 4.
func Claim(ctx context.Context, pool *pgxpool.Pool, queueName, workerID string) (*Job, error) {
	row := pool.QueryRow(ctx, `
		WITH next_job AS (
			SELECT id FROM jobs
			WHERE  status     = 'pending'
			  AND  run_at    <= now()
			  AND  queue_name = $1
			ORDER BY priority DESC, run_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs
		SET    status         = 'running',
		       locked_by      = $2,
		       locked_at      = now(),
		       last_heartbeat = now(),
		       lock_version   = lock_version + 1,
		       attempts       = attempts + 1
		FROM   next_job
		WHERE  jobs.id = next_job.id
		RETURNING
		    jobs.id, jobs.queue_name, jobs.payload, jobs.status,
		    jobs.priority, jobs.run_at, jobs.attempts, jobs.max_attempts,
		    jobs.locked_by, jobs.lock_version, jobs.last_error, jobs.created_at`,
		queueName, workerID,
	)

	j := &Job{}
	err := row.Scan(
		&j.ID, &j.QueueName, &j.Payload, &j.Status,
		&j.Priority, &j.RunAt, &j.Attempts, &j.MaxAttempts,
		&j.LockedBy, &j.LockVersion, &j.LastError, &j.CreatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil // queue is empty right now
	}
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return j, nil

}

// Heartbeat refreshes last_heartbeat so the reaper knows this worker is alive.
// The lock_version guard means a heartbeat from a zombie worker whose job has
// already been reclaimed is silently ignored. Step 6 + 7.
func Heartbeat(ctx context.Context, pool *pgxpool.Pool, jobID int64, lockVersion int) error {
	_, err := pool.Exec(ctx, `
		UPDATE jobs
		SET    last_heartbeat = now()
		WHERE  id           = $1
		  AND  lock_version = $2
		  AND  status       = 'running'`,
		jobID, lockVersion,
	)
	return err
}

// Complete marks a job done. If the lock_version no longer matches (i.e. the
// reaper reclaimed and re-claimed this job), the write matches zero rows and
// safely no-ops — the fencing token prevents zombie corruption. Step 7.
func Complete(ctx context.Context, pool *pgxpool.Pool, jobID int64, lockVersion int) error {
	_, err := pool.Exec(ctx, `
		UPDATE jobs
		SET    status = 'done'
		WHERE  id           = $1
		  AND  lock_version = $2
		  AND  status       = 'running'`,
		jobID, lockVersion,
	)
	if err != nil {
		return fmt.Errorf("complete job %d: %w", jobID, err)
	}
	return nil
}

// Fail handles a failed job execution.
//   - If attempts < max_attempts: reset to 'pending' with a run_at in the
//     future computed by exponential backoff + full jitter (Step 8).
//   - If attempts >= max_attempts: move to 'dead' (DLQ). The row is preserved
//     with last_error so a human can inspect and replay if needed (Step 8).
//
// The lock_version guard applies here too — a stale worker's Fail call for an
// already-reclaimed job no-ops instead of corrupting state (Step 7).
func Fail(ctx context.Context, pool *pgxpool.Pool, j *Job, jobErr error) error {
	errMsg := jobErr.Error()

	if j.Attempts >= j.MaxAttempts {
		_, err := pool.Exec(ctx, `
			UPDATE jobs
			SET    status     = 'dead',
			       last_error = $1
			WHERE  id           = $2
			  AND  lock_version = $3`,
			errMsg, j.ID, j.LockVersion,
		)
		return err
	}

	// Exponential backoff with full jitter.
	// delay is drawn uniformly from [0, base * 2^(attempts-1)] seconds, capped at 10 min.
	// Full jitter (vs. "equal jitter") spreads retries most aggressively, which
	// best prevents thundering herds when many jobs fail simultaneously.
	base := 5.0
	maxDelay := base * pow2(float64(j.Attempts-1))
	if maxDelay > 600 {
		maxDelay = 600
	}
	jitter := rand.Float64() * maxDelay // #nosec G404
	retryAt := time.Now().Add(time.Duration(jitter * float64(time.Second)))

	_, err := pool.Exec(ctx, `
		UPDATE jobs
		SET    status     = 'pending',
		       run_at     = $1,
		       last_error = $2,
		       locked_by  = NULL,
		       locked_at  = NULL
		WHERE  id           = $3
		  AND  lock_version = $4`,
		retryAt, errMsg, j.ID, j.LockVersion,
	)
	return err
}

// Reap finds jobs stuck in 'running' whose heartbeat has gone stale (the
// worker holding them likely crashed) and resets them to 'pending'.
// Running the same UPDATE on multiple worker processes simultaneously is safe —
// once one process's update lands, the row no longer matches the WHERE clause
// for any subsequent process. No leader election needed. Step 6.
func Reap(ctx context.Context, pool *pgxpool.Pool, staleAfter time.Duration) (int64, error) {
	tag, err := pool.Exec(ctx, `
		UPDATE jobs
		SET    status    = 'pending',
		       locked_by = NULL,
		       locked_at = NULL
		WHERE  status         = 'running'
		  AND  last_heartbeat < now() - $1::interval`,
		staleAfter.String(),
	)
	if err != nil {
		return 0, fmt.Errorf("reap: %w", err)
	}
	return tag.RowsAffected(), nil
}

// pow2 returns 2^n (integer exponent, float result).
func pow2(n float64) float64 {
	r := 1.0
	for i := 0.0; i < n; i++ {
		r *= 2
	}
	return r
}
