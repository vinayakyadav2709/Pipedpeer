package nodestore

import (
	"math"
	"testing"
)

// Health factor: halves on unreachable or failed work (floor 0.1), recovers
// +0.1 per healthy poll up to 1.0; only the failing node changes.
func TestHealthFactor(t *testing.T) {
	type step string
	const (
		healthy     step = "healthy"
		unreachable step = "unreachable"
		failed      step = "failed"
	)
	cases := []struct {
		name  string
		steps []step
		want  float64
	}{
		{"never failed stays 1.0", []step{healthy, healthy, healthy}, 1.0},
		{"unreachable halves", []step{healthy, unreachable}, 0.5},
		{"failed work halves", []step{healthy, failed}, 0.5},
		{"two failures quarter", []step{healthy, failed, failed}, 0.25},
		{"floor 0.1", []step{healthy, failed, failed, failed, failed, failed, failed}, 0.1},
		{"recovers 0.1 per poll", []step{healthy, failed, healthy, healthy}, 0.7},
		{"recovery caps at 1.0", []step{healthy, failed, healthy, healthy, healthy, healthy, healthy, healthy}, 1.0},
		{"floor then recovers", []step{healthy, failed, failed, failed, failed, failed, healthy}, 0.2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			// A bystander must never move.
			must(t, s.MarkHealthy(Node{NodeID: "other"}))
			for _, st := range tc.steps {
				switch st {
				case healthy:
					must(t, s.MarkHealthy(Node{NodeID: "flaky"}))
				case unreachable:
					must(t, s.MarkUnreachable("flaky"))
				case failed:
					must(t, s.MarkFailed("flaky"))
				}
			}
			n, ok := nodeByID(t, s, "flaky")
			if !ok {
				t.Fatal("flaky node missing")
			}
			if math.Abs(n.HealthScore-tc.want) > 1e-9 {
				t.Fatalf("health = %v, want %v", n.HealthScore, tc.want)
			}
			if o, _ := nodeByID(t, s, "other"); o.HealthScore != 1.0 {
				t.Fatalf("healthy bystander health = %v, want 1.0", o.HealthScore)
			}
		})
	}
}
