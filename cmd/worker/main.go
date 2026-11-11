// cmd/worker/main.go — worker process (Steps 4–8).
//
// Run N independent copies of this binary to prove no duplicate claims:
//
//	go run ./cmd/worker -id=w1 &
//	go run ./cmd/worker -id=w2 &
//	go run ./cmd/worker -id=w3 &
//
// Each process spawns a configurable goroutine pool that polls for jobs,
// plus a reaper goroutine that reclaims stale running jobs (Step 6).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"jqs/internal/db"
	"jqs/internal/job"
)

// workerStats tracks aggregate metrics across all goroutines in this process.
// Updated atomically so no mutex is needed.
type workerStats struct {
	completed     atomic.Int64
	failed        atomic.Int64
	totalDuration atomic.Int64 // nanoseconds
}

var stats workerStats

func main() {
	workerID := flag.String("id", defaultWorkerID(), "unique worker identifier")
	queue := flag.String("queue", "default", "queue name to consume")
	concurrency := flag.Int("concurrency", 4, "number of goroutines per process")
	pollInterval := flag.Duration("poll", 500*time.Millisecond, "idle poll interval")
	heartbeatInterval := flag.Duration("heartbeat", 10*time.Second, "heartbeat interval")
	reaperInterval := flag.Duration("reaper", 15*time.Second, "reaper scan interval")
	staleAfter := flag.Duration("stale", 45*time.Second, "heartbeat age that marks a job stale")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := db.New(ctx)
	if err != nil {
		log.Fatalf("[%s] db: %v", *workerID, err)
	}
	defer pool.Close()

	log.Printf("[%s] starting — queue=%s concurrency=%d", *workerID, *queue, *concurrency)

	// Reaper goroutine — runs on every worker process; safe because the
	// UPDATE WHERE clause is naturally idempotent (Step 6).
	go runReaper(ctx, pool, *workerID, *reaperInterval, *staleAfter)

	// Fan out N worker goroutines (Steps 4–8).
	done := make(chan struct{}, *concurrency)
	for i := range *concurrency {
		go func(slot int) {
			workerLoop(ctx, pool, *workerID, slot, *queue, *pollInterval, *heartbeatInterval)
			done <- struct{}{}
		}(i)
	}

	for range *concurrency {
		<-done
	}

	// Log shutdown stats.
	completed := stats.completed.Load()
	failed := stats.failed.Load()
	total := completed + failed
	totalDur := time.Duration(stats.totalDuration.Load())
	if total > 0 {
		avg := totalDur / time.Duration(total)
		log.Printf("[%s] stats — processed=%d completed=%d failed=%d avg_duration=%s total_duration=%s",
			*workerID, total, completed, failed, avg, totalDur)
	}
	log.Printf("[%s] shut down cleanly", *workerID)
}

// runReaper periodically reclaims jobs whose heartbeat has gone stale —
// meaning the worker that held them crashed or was killed (Step 6).
func runReaper(ctx context.Context, pool *pgxpool.Pool, workerID string, interval, staleAfter time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := job.Reap(ctx, pool, staleAfter)
			if err != nil {
				log.Printf("[%s][reaper] error: %v", workerID, err)
			} else if n > 0 {
				log.Printf("[%s][reaper] recovered %d stale job(s)", workerID, n)
			}
		}
	}
}

// workerLoop polls for jobs and executes them one at a time (Step 4+).
func workerLoop(
	ctx context.Context,
	pool *pgxpool.Pool,
	workerID string,
	slot int,
	queue string,
	pollInterval time.Duration,
	heartbeatInterval time.Duration,
) {
	id := fmt.Sprintf("%s#%d", workerID, slot)

	for {
		if ctx.Err() != nil {
			return
		}

		// Step 4: atomic claim — FOR UPDATE SKIP LOCKED.
		j, err := job.Claim(ctx, pool, queue, id)
		if err != nil {
			log.Printf("[%s] claim error: %v", id, err)
			sleep(ctx, pollInterval)
			continue
		}
		if j == nil {
			// No pending job — wait before polling again.
			sleep(ctx, pollInterval)
			continue
		}

		log.Printf("[%s] claimed job %d attempt=%d/%d payload=%s",
			id, j.ID, j.Attempts, j.MaxAttempts, j.Payload)

		execute(ctx, pool, j, id, heartbeatInterval)
	}
}

// execute runs a job with a concurrent heartbeat, then marks it done or failed.
// Steps 5, 6, 7, 8.
func execute(
	ctx context.Context,
	pool *pgxpool.Pool,
	j *job.Job,
	workerID string,
	heartbeatInterval time.Duration,
) {
	start := time.Now()

	// Step 6: heartbeat goroutine keeps last_heartbeat fresh while we work.
	// Cancelled as soon as execute returns so we stop sending stale heartbeats.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()

	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := job.Heartbeat(hbCtx, pool, j.ID, j.LockVersion); err != nil {
					log.Printf("[%s] heartbeat job %d: %v", workerID, j.ID, err)
				}
			}
		}
	}()

	// Step 5: simulate real work — random duration, random failure rate.
	// In a real system this is where you'd call send-email, process-image, etc.
	jobErr := simulateWork(j)

	elapsed := time.Since(start)
	stats.totalDuration.Add(int64(elapsed))

	if jobErr == nil {
		// Step 7: fencing token — Complete only succeeds if lock_version matches.
		if err := job.Complete(ctx, pool, j.ID, j.LockVersion); err != nil {
			log.Printf("[%s] complete job %d: %v", workerID, j.ID, err)
		} else {
			log.Printf("[%s] completed job %d (%s)", workerID, j.ID, elapsed)
			stats.completed.Add(1)
		}
	} else {
		// Step 8: retry with backoff + jitter, or DLQ if exhausted.
		log.Printf("[%s] job %d failed (attempt %d/%d): %v", workerID, j.ID, j.Attempts, j.MaxAttempts, jobErr)
		if err := job.Fail(ctx, pool, j, jobErr); err != nil {
			log.Printf("[%s] fail job %d: %v", workerID, j.ID, err)
		}
		stats.failed.Add(1)
	}
}

// simulateWork mimics job execution: random work duration (50–2000 ms),
// random 20% failure rate. Replace with real handlers in a non-demo system.
func simulateWork(j *job.Job) error {
	workDuration := time.Duration(50+rand.IntN(1950)) * time.Millisecond
	time.Sleep(workDuration)

	if rand.Float64() < 0.20 {
		return fmt.Errorf("simulated failure for job %d", j.ID)
	}
	return nil
}

// sleep blocks for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// defaultWorkerID returns hostname+pid, unique enough for local demo.
func defaultWorkerID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

