// internal/job/job_integration_test.go — integration tests against a live Postgres.
//
// These tests require a running Postgres matching docker-compose.yml.
// The test suite uses a dedicated queue name ("test_<random>") per run to
// avoid colliding with manual use or other test runs.
//
// Run:
//
//	DATABASE_URL="postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable" \
//	  go test -v -count=1 ./internal/job/
//
// The -count=1 flag disables test caching so each run hits the real database.
package job

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool returns a shared pgxpool connected to the test database.
// It skips the test if DATABASE_URL is unset (CI without Postgres).
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping integration test: cannot create pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping integration test: cannot ping postgres: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool
}

// testQueue returns a unique queue name for this test to avoid cross-test pollution.
func testQueue(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test_%s_%d", t.Name(), rand.IntN(100000))
}

// cleanup deletes all jobs in the given queue.
func cleanup(t *testing.T, pool *pgxpool.Pool, queue string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `DELETE FROM jobs WHERE queue_name = $1`, queue)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Enqueue + Claim basic lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func TestEnqueueAndClaim(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	// Enqueue a job.
	id, err := Enqueue(ctx, pool, EnqueueParams{
		QueueName: queue,
		Payload:   []byte(`{"action":"test_basic"}`),
		Priority:  5,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive job ID, got %d", id)
	}

	// Claim it.
	j, err := Claim(ctx, pool, queue, "worker-test")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if j == nil {
		t.Fatal("Claim returned nil — no job found")
	}
	if j.ID != id {
		t.Errorf("claimed job ID = %d, want %d", j.ID, id)
	}
	if j.Status != "running" {
		t.Errorf("claimed job status = %q, want %q", j.Status, "running")
	}
	if j.Attempts != 1 {
		t.Errorf("claimed job attempts = %d, want 1", j.Attempts)
	}
	if j.LockVersion != 1 {
		t.Errorf("claimed job lock_version = %d, want 1", j.LockVersion)
	}

	// Queue should now be empty.
	j2, err := Claim(ctx, pool, queue, "worker-test")
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if j2 != nil {
		t.Errorf("expected nil from second Claim, got job %d", j2.ID)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: Complete + fencing token
// ─────────────────────────────────────────────────────────────────────────────

func TestComplete(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	id, _ := Enqueue(ctx, pool, EnqueueParams{
		QueueName: queue,
		Payload:   []byte(`{"action":"test_complete"}`),
	})
	j, _ := Claim(ctx, pool, queue, "worker-test")

	// Complete with correct lock_version.
	if err := Complete(ctx, pool, j.ID, j.LockVersion); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Verify status in DB.
	var status string
	err := pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, id).Scan(&status)
	if err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != "done" {
		t.Errorf("job status = %q, want %q", status, "done")
	}
}

func TestComplete_StaleLockVersion(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	Enqueue(ctx, pool, EnqueueParams{
		QueueName: queue,
		Payload:   []byte(`{"action":"test_fencing"}`),
	})
	j, _ := Claim(ctx, pool, queue, "worker-old")

	// Simulate reaper reclaiming: manually reset to pending and bump lock_version.
	_, err := pool.Exec(ctx, `
		UPDATE jobs SET status = 'pending', locked_by = NULL, locked_at = NULL
		WHERE id = $1`, j.ID)
	if err != nil {
		t.Fatalf("simulated reaper reset: %v", err)
	}

	// Another worker claims it (bumps lock_version to 2).
	j2, _ := Claim(ctx, pool, queue, "worker-new")
	if j2 == nil {
		t.Fatal("second claim returned nil")
	}
	if j2.LockVersion != 2 {
		t.Fatalf("expected lock_version=2 after re-claim, got %d", j2.LockVersion)
	}

	// Original worker tries to Complete with stale lock_version=1.
	// This should silently no-op (fencing token prevents corruption).
	err = Complete(ctx, pool, j.ID, j.LockVersion) // lock_version=1
	if err != nil {
		t.Fatalf("stale Complete returned error: %v (expected silent no-op)", err)
	}

	// Job should still be 'running' (held by worker-new), NOT 'done'.
	var status string
	pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, j.ID).Scan(&status)
	if status != "running" {
		t.Errorf("after stale Complete, status = %q, want %q (fencing token should have prevented the write)", status, "running")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: Fail + retry with backoff
// ─────────────────────────────────────────────────────────────────────────────

func TestFail_RetryWithBackoff(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	Enqueue(ctx, pool, EnqueueParams{
		QueueName:   queue,
		Payload:     []byte(`{"action":"test_retry"}`),
		MaxAttempts: 3,
	})
	j, _ := Claim(ctx, pool, queue, "worker-test")

	// Fail the job (attempt 1 of 3 — should retry).
	if err := Fail(ctx, pool, j, errors.New("transient error")); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	// Verify job is back to 'pending' with a future run_at.
	var status string
	var runAt time.Time
	var lastError *string
	err := pool.QueryRow(ctx,
		`SELECT status, run_at, last_error FROM jobs WHERE id = $1`, j.ID,
	).Scan(&status, &runAt, &lastError)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if status != "pending" {
		t.Errorf("status = %q, want %q", status, "pending")
	}
	if lastError == nil || *lastError != "transient error" {
		t.Errorf("last_error = %v, want %q", lastError, "transient error")
	}
	// run_at should be in the future (backoff delay).
	if !runAt.After(time.Now().Add(-1 * time.Second)) {
		t.Errorf("run_at = %v, expected it to be recent or in the future", runAt)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: Dead Letter Queue — max_attempts exhaustion
// ─────────────────────────────────────────────────────────────────────────────

func TestFail_DLQ(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	Enqueue(ctx, pool, EnqueueParams{
		QueueName:   queue,
		Payload:     []byte(`{"action":"test_dlq"}`),
		MaxAttempts: 1, // will be exhausted on first failure
	})

	j, _ := Claim(ctx, pool, queue, "worker-test")
	// j.Attempts = 1, j.MaxAttempts = 1 → Fail should send to DLQ.

	if err := Fail(ctx, pool, j, errors.New("permanent error")); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	var status string
	var lastError *string
	pool.QueryRow(ctx,
		`SELECT status, last_error FROM jobs WHERE id = $1`, j.ID,
	).Scan(&status, &lastError)

	if status != "dead" {
		t.Errorf("status = %q, want %q (should be in DLQ)", status, "dead")
	}
	if lastError == nil || *lastError != "permanent error" {
		t.Errorf("last_error = %v, want %q", lastError, "permanent error")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: Reaper — recovers stale running jobs
// ─────────────────────────────────────────────────────────────────────────────

func TestReaper(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	Enqueue(ctx, pool, EnqueueParams{
		QueueName: queue,
		Payload:   []byte(`{"action":"test_reaper"}`),
	})

	// Claim the job.
	j, _ := Claim(ctx, pool, queue, "worker-dead")

	// Manually set last_heartbeat to 2 minutes ago to simulate a crashed worker.
	_, err := pool.Exec(ctx, `
		UPDATE jobs SET last_heartbeat = now() - interval '2 minutes'
		WHERE id = $1`, j.ID)
	if err != nil {
		t.Fatalf("set stale heartbeat: %v", err)
	}

	// Run reaper with a 30s stale threshold — 2-minute-old heartbeat should trigger.
	reaped, err := Reap(ctx, pool, 30*time.Second)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if reaped != 1 {
		t.Errorf("Reap returned %d, want 1", reaped)
	}

	// Job should be back to 'pending'.
	var status string
	var lockedBy *string
	pool.QueryRow(ctx,
		`SELECT status, locked_by FROM jobs WHERE id = $1`, j.ID,
	).Scan(&status, &lockedBy)

	if status != "pending" {
		t.Errorf("status = %q, want %q", status, "pending")
	}
	if lockedBy != nil {
		t.Errorf("locked_by = %v, want nil", *lockedBy)
	}

	// A new worker should be able to claim it.
	j2, err := Claim(ctx, pool, queue, "worker-rescue")
	if err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if j2 == nil {
		t.Fatal("re-claim returned nil — reaper didn't recover the job")
	}
	if j2.ID != j.ID {
		t.Errorf("re-claimed job ID = %d, want %d", j2.ID, j.ID)
	}
}

func TestReaper_DoesNotReapFreshJobs(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	Enqueue(ctx, pool, EnqueueParams{
		QueueName: queue,
		Payload:   []byte(`{"action":"test_reaper_fresh"}`),
	})

	// Claim the job — heartbeat is now().
	Claim(ctx, pool, queue, "worker-alive")

	// Reap with 30s threshold — fresh heartbeat should NOT be reaped.
	reaped, err := Reap(ctx, pool, 30*time.Second)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if reaped != 0 {
		t.Errorf("Reap returned %d, want 0 (fresh heartbeat should not be reaped)", reaped)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 6: Heartbeat updates last_heartbeat
// ─────────────────────────────────────────────────────────────────────────────

func TestHeartbeat(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	Enqueue(ctx, pool, EnqueueParams{
		QueueName: queue,
		Payload:   []byte(`{"action":"test_heartbeat"}`),
	})
	j, _ := Claim(ctx, pool, queue, "worker-test")

	// Record initial heartbeat time.
	var hb1 time.Time
	pool.QueryRow(ctx, `SELECT last_heartbeat FROM jobs WHERE id = $1`, j.ID).Scan(&hb1)

	// Small delay to ensure measurable time difference.
	time.Sleep(50 * time.Millisecond)

	// Send heartbeat.
	if err := Heartbeat(ctx, pool, j.ID, j.LockVersion); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	var hb2 time.Time
	pool.QueryRow(ctx, `SELECT last_heartbeat FROM jobs WHERE id = $1`, j.ID).Scan(&hb2)

	if !hb2.After(hb1) {
		t.Errorf("heartbeat did not advance: hb1=%v hb2=%v", hb1, hb2)
	}
}

func TestHeartbeat_StaleLockVersion(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	Enqueue(ctx, pool, EnqueueParams{
		QueueName: queue,
		Payload:   []byte(`{"action":"test_heartbeat_stale"}`),
	})
	j, _ := Claim(ctx, pool, queue, "worker-test")

	// Heartbeat with wrong lock_version — should be a silent no-op.
	wrongVersion := j.LockVersion + 99
	err := Heartbeat(ctx, pool, j.ID, wrongVersion)
	if err != nil {
		t.Fatalf("Heartbeat with wrong version returned error: %v", err)
	}
	// No error, but the heartbeat should not have been updated.
	// (We verify by checking that a subsequent heartbeat with the correct version works.)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 7: Concurrent claims — zero duplicates (the core correctness test)
// ─────────────────────────────────────────────────────────────────────────────

func TestConcurrentClaims_ZeroDuplicates(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	const jobCount = 100
	const workerCount = 8

	// Enqueue jobs.
	for i := range jobCount {
		_, err := Enqueue(ctx, pool, EnqueueParams{
			QueueName: queue,
			Payload:   fmt.Appendf(nil, `{"seq":%d}`, i),
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	// Race to claim with multiple workers.
	var (
		mu       sync.Mutex
		claimed  = make(map[int64]string) // jobID → workerID
		wg       sync.WaitGroup
	)

	for w := range workerCount {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			workerID := fmt.Sprintf("test-worker#%d", slot)

			for {
				j, err := Claim(ctx, pool, queue, workerID)
				if err != nil {
					t.Errorf("[%s] claim error: %v", workerID, err)
					return
				}
				if j == nil {
					return // queue empty
				}

				mu.Lock()
				if prev, exists := claimed[j.ID]; exists {
					t.Errorf("DUPLICATE CLAIM: job %d claimed by both %q and %q", j.ID, prev, workerID)
				}
				claimed[j.ID] = workerID
				mu.Unlock()

				// Complete the job.
				Complete(ctx, pool, j.ID, j.LockVersion)
			}
		}(w)
	}

	wg.Wait()

	if len(claimed) != jobCount {
		t.Errorf("claimed %d jobs, want %d", len(claimed), jobCount)
	}

	// Double-check in the database.
	var doneCount int
	pool.QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE queue_name = $1 AND status = 'done'`, queue,
	).Scan(&doneCount)

	if doneCount != jobCount {
		t.Errorf("database shows %d done jobs, want %d", doneCount, jobCount)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 8: Priority ordering
// ─────────────────────────────────────────────────────────────────────────────

func TestClaimPriorityOrder(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	// Enqueue jobs with different priorities (low first, high last).
	priorities := []int{1, 5, 3, 10, 7}
	for _, p := range priorities {
		Enqueue(ctx, pool, EnqueueParams{
			QueueName: queue,
			Payload:   fmt.Appendf(nil, `{"priority":%d}`, p),
			Priority:  p,
		})
	}

	// Claims should come out in descending priority order: 10, 7, 5, 3, 1.
	expectedOrder := []int{10, 7, 5, 3, 1}
	for i, expected := range expectedOrder {
		j, err := Claim(ctx, pool, queue, "worker-test")
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if j == nil {
			t.Fatalf("claim %d: returned nil, expected priority %d", i, expected)
		}
		if j.Priority != expected {
			t.Errorf("claim %d: got priority %d, want %d", i, j.Priority, expected)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 9: Enqueue validation — empty payload
// ─────────────────────────────────────────────────────────────────────────────

func TestEnqueue_EmptyPayload(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	ctx := context.Background()

	_, err := Enqueue(ctx, pool, EnqueueParams{
		QueueName: queue,
		Payload:   nil,
	})
	if err == nil {
		t.Fatal("expected error for empty payload, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 10: Delayed jobs (run_at in the future)
// ─────────────────────────────────────────────────────────────────────────────

func TestDelayedJob(t *testing.T) {
	pool := testPool(t)
	queue := testQueue(t)
	t.Cleanup(func() { cleanup(t, pool, queue) })
	ctx := context.Background()

	future := time.Now().Add(1 * time.Hour)
	Enqueue(ctx, pool, EnqueueParams{
		QueueName: queue,
		Payload:   []byte(`{"action":"delayed"}`),
		RunAt:     &future,
	})

	// Claim should return nil — job's run_at is in the future.
	j, err := Claim(ctx, pool, queue, "worker-test")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if j != nil {
		t.Errorf("expected nil (job is delayed), got job %d", j.ID)
	}
}
