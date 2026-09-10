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
	"time"

	"jqs/internal/db"
	"jqs/internal/job"
)

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

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(enqueueResponse{ID: id})
	})

	addr := ":8080"
	log.Printf("API listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("server: %v", err)
	}
}
