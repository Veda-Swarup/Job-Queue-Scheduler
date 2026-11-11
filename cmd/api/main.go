// cmd/api/main.go — minimal HTTP enqueue API (Step 3).
// POST /enqueue   body: { "payload": <any JSON>, "queue": "...", "priority": N }
// Returns: { "id": <job-id> }
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"jqs/internal/db"
	"jqs/internal/job"
)

// enqueueCounter tracks total jobs enqueued since the API started.
var enqueueCounter atomic.Int64

type enqueueRequest struct {
	Payload     json.RawMessage `json:"payload"`
	Queue       string          `json:"queue"`
	Priority    int             `json:"priority"`
	RunAt       *time.Time      `json:"run_at,omitempty"`
	MaxAttempts int             `json:"max_attempts,omitempty"`
}

type enqueueResponse struct {
	ID int64 `json:"id"`
}

type replayRequest struct {
	ID int64 `json:"id"`
}

func main() {
	ctx := context.Background()

	pool, err := db.New(ctx)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	http.HandleFunc("/enqueue", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req enqueueRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
			return
		}
		if len(req.Payload) == 0 {
			http.Error(w, "payload is required", http.StatusBadRequest)
			return
		}
		if req.Queue == "" {
			req.Queue = "default"
		}

		id, err := job.Enqueue(r.Context(), pool, job.EnqueueParams{
			QueueName:   req.Queue,
			Payload:     req.Payload,
			Priority:    req.Priority,
			RunAt:       req.RunAt,
			MaxAttempts: req.MaxAttempts,
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("enqueue: %v", err), http.StatusInternalServerError)
			return
		}

		enqueueCounter.Add(1)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(enqueueResponse{ID: id})
	})

	// GET /stats?queue=default — returns aggregate job counts by status.
	// Omit ?queue to get counts across all queues.
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		queueName := r.URL.Query().Get("queue")
		stats, err := job.Stats(r.Context(), pool, queueName)
		if err != nil {
			http.Error(w, fmt.Sprintf("stats: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	})

	// GET /metrics — Prometheus-format metrics for queue depth and worker utilization.
	// Scraped by Prometheus/Grafana at a configurable interval.
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Queue depth by status.
		stats, err := job.Stats(r.Context(), pool, "")
		if err != nil {
			http.Error(w, fmt.Sprintf("metrics: %v", err), http.StatusInternalServerError)
			return
		}

		// Per-worker utilization (running jobs per worker goroutine).
		util, err := job.WorkerUtilization(r.Context(), pool)
		if err != nil {
			http.Error(w, fmt.Sprintf("metrics: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		var b strings.Builder

		// Queue depth gauge.
		b.WriteString("# HELP jqs_queue_depth Number of jobs by status.\n")
		b.WriteString("# TYPE jqs_queue_depth gauge\n")
		fmt.Fprintf(&b, "jqs_queue_depth{status=\"pending\"} %d\n", stats.Pending)
		fmt.Fprintf(&b, "jqs_queue_depth{status=\"running\"} %d\n", stats.Running)
		fmt.Fprintf(&b, "jqs_queue_depth{status=\"done\"} %d\n", stats.Done)
		fmt.Fprintf(&b, "jqs_queue_depth{status=\"dead\"} %d\n", stats.Dead)

		// Total jobs ever enqueued via this API instance (counter).
		b.WriteString("# HELP jqs_enqueued_total Total jobs enqueued since API start.\n")
		b.WriteString("# TYPE jqs_enqueued_total counter\n")
		fmt.Fprintf(&b, "jqs_enqueued_total %d\n", enqueueCounter.Load())

		// Worker utilization: running jobs per worker goroutine.
		b.WriteString("# HELP jqs_worker_active_jobs Number of jobs currently being processed by each worker.\n")
		b.WriteString("# TYPE jqs_worker_active_jobs gauge\n")
		for worker, count := range util {
			fmt.Fprintf(&b, "jqs_worker_active_jobs{worker=\"%s\"} %d\n", worker, count)
		}

		// Active workers count.
		b.WriteString("# HELP jqs_active_workers Number of workers currently processing jobs.\n")
		b.WriteString("# TYPE jqs_active_workers gauge\n")
		fmt.Fprintf(&b, "jqs_active_workers %d\n", len(util))

		w.Write([]byte(b.String()))
	})

	// GET /jobs/42 — inspect a single job's full state.
	http.HandleFunc("/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		idStr := strings.TrimPrefix(r.URL.Path, "/jobs/")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid job id", http.StatusBadRequest)
			return
		}

		j, err := job.GetByID(r.Context(), pool, id)
		if err != nil {
			http.Error(w, fmt.Sprintf("get job: %v", err), http.StatusInternalServerError)
			return
		}
		if j == nil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(j)
	})

	// POST /replay   body: { "id": 42 } — re-enqueue a dead job.
	http.HandleFunc("/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req replayRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
			return
		}
		if req.ID <= 0 {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		replayedID, err := job.ReplayDead(r.Context(), pool, req.ID)
		if err != nil {
			http.Error(w, fmt.Sprintf("replay: %v", err), http.StatusInternalServerError)
			return
		}
		if replayedID == 0 {
			http.Error(w, "job not found or not in dead status", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"replayed_id": replayedID,
			"status":      "pending",
		})
	})

	addr := ":8080"
	log.Printf("API listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("server: %v", err)
	}
}

