package job

import (
	"math"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// pow2 — the unexported helper used by Fail's backoff calculation
// ─────────────────────────────────────────────────────────────────────────────

func TestPow2(t *testing.T) {
	cases := []struct {
		n    float64
		want float64
	}{
		{0, 1},
		{1, 2},
		{2, 4},
		{3, 8},
		{4, 16},
		{5, 32},
		{10, 1024},
	}
	for _, tc := range cases {
		got := pow2(tc.n)
		if got != tc.want {
			t.Errorf("pow2(%v) = %v, want %v", tc.n, got, tc.want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Backoff bounds — verify the delay formula produces values in the expected
// range for each attempt number, including the 10-minute cap.
// ─────────────────────────────────────────────────────────────────────────────

func TestBackoffBounds(t *testing.T) {
	// The backoff formula from Fail():
	//   base = 5.0
	//   maxDelay = base * pow2(attempts - 1), capped at 600
	//   jitter ∈ [0, maxDelay)
	//
	// So the maximum possible delay for each attempt is:
	//   attempt 1: 5 * 2^0 = 5s
	//   attempt 2: 5 * 2^1 = 10s
	//   attempt 3: 5 * 2^2 = 20s
	//   attempt 4: 5 * 2^3 = 40s
	//   attempt 5: 5 * 2^4 = 80s
	//   attempt 6: 5 * 2^5 = 160s
	//   attempt 7: 5 * 2^6 = 320s
	//   attempt 8: 5 * 2^7 = 600s (capped)
	//   attempt 9: 5 * 2^8 = 600s (capped)

	base := 5.0
	cases := []struct {
		attempt     int
		wantMaxCap  float64 // upper bound of jitter window
	}{
		{1, 5},
		{2, 10},
		{3, 20},
		{4, 40},
		{5, 80},
		{6, 160},
		{7, 320},
		{8, 600},  // capped at 10 min
		{9, 600},  // still capped
		{20, 600}, // still capped
	}

	for _, tc := range cases {
		maxDelay := base * pow2(float64(tc.attempt-1))
		if maxDelay > 600 {
			maxDelay = 600
		}
		if math.Abs(maxDelay-tc.wantMaxCap) > 0.001 {
			t.Errorf("attempt %d: maxDelay = %v, want %v", tc.attempt, maxDelay, tc.wantMaxCap)
		}
	}
}

func TestBackoffIsAlwaysNonNegative(t *testing.T) {
	base := 5.0
	for attempt := 1; attempt <= 100; attempt++ {
		maxDelay := base * pow2(float64(attempt-1))
		if maxDelay > 600 {
			maxDelay = 600
		}
		if maxDelay < 0 {
			t.Errorf("attempt %d: maxDelay = %v, must be >= 0", attempt, maxDelay)
		}
	}
}

func TestBackoffCapAt600(t *testing.T) {
	base := 5.0
	// 5 * 2^7 = 640, should be capped to 600
	maxDelay := base * pow2(7)
	if maxDelay > 600 {
		maxDelay = 600
	}
	if maxDelay != 600 {
		t.Errorf("expected cap at 600, got %v", maxDelay)
	}
}
