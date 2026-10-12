package daemonapi

import (
	"encoding/json"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// benchResponse is a node's measured compute throughput.
type benchResponse struct {
	// Score is iterations per second summed across every worker: a number
	// with no unit that only means anything next to another node's.
	Score float64 `json:"score"`
	Cores int     `json:"cores"`
	// Millis is how long the measurement actually ran, so a caller can tell
	// a real sample from one that was cut short.
	Millis int64 `json:"millis"`
}

// handleBench measures what this node can currently do, rather than what its
// hardware suggests it should.
//
// Placement used to read advertised core counts, which are wrong in both the
// ways that matter. They ignore load - a 32-core machine already running
// someone else's job is not a 32-core machine - and they ignore any cap
// imposed on the daemon: every worker in the local test cluster advertises the
// host's 16 cores while actually being held to 8, 2 and 1 by its cgroup. A
// measurement sees the truth in both cases, because it is subject to the same
// scheduler and the same quota as the work will be.
//
// Deliberately arithmetic in registers: no allocation, no syscalls, no memory
// traffic. The point is to rank nodes against each other, and a benchmark
// that touched RAM or disk would rank their caches instead.
func (s *Server) handleBench(w http.ResponseWriter, r *http.Request) {
	millis := int64(200)
	if v := r.URL.Query().Get("ms"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 && n <= 5000 {
			millis = n
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(measureThroughput(time.Duration(millis) * time.Millisecond))
}

func measureThroughput(d time.Duration) benchResponse {
	cores := runtime.NumCPU()
	var total atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(d)

	start := time.Now()
	for i := 0; i < cores; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n int64
			var acc uint64 = 1
			for {
				// Checking the clock every iteration would measure the clock.
				for j := 0; j < 4096; j++ {
					acc = acc*6364136223846793005 + 1442695040888963407
				}
				n += 4096
				if time.Now().After(deadline) {
					break
				}
			}
			// Keep the compiler from deciding none of this was necessary.
			if acc == 0 {
				n++
			}
			total.Add(n)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if elapsed <= 0 {
		return benchResponse{Cores: cores}
	}
	return benchResponse{
		Score:  float64(total.Load()) / elapsed.Seconds(),
		Cores:  cores,
		Millis: elapsed.Milliseconds(),
	}
}
