package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDaemon serves /v1/nodes from a function of elapsed time.
func fakeDaemon(t *testing.T, nodes func(el time.Duration) []connNode) string {
	t.Helper()
	start := time.Now()
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/v1/nodes" {
			_ = json.NewEncoder(w).Encode(nodes(time.Since(start)))
			return
		}
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func nd(id, state, reason string) connNode {
	return connNode{NodeID: id + "00000000", State: state, Reason: reason, Source: "gossip", Path: "lan"}
}

const refusedReason = "dial tcp 10.0.0.2:4242: connect: connection refused"

func TestWaitConnectedEarlyEnd(t *testing.T) {
	connSettle, connPoll = 300*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { connSettle, connPoll = 10*time.Second, time.Second })
	deadline := 6 * time.Second
	rows := []struct {
		name     string
		nodes    func(el time.Duration) []connNode
		want     connOutcome
		minTime  time.Duration
		maxTime  time.Duration // 0: no upper bound
		fullWait bool
	}{
		{"healthy + refused, steady: partly after settle", func(time.Duration) []connNode {
			return []connNode{nd("aaaa", "healthy", ""), nd("bbbb", "unreachable", refusedReason)}
		}, connPartly, 300 * time.Millisecond, 3 * time.Second, false},
		{"healthy + timeout: waits to deadline", func(time.Duration) []connNode {
			return []connNode{nd("aaaa", "healthy", ""), nd("bbbb", "unreachable", "dial tcp: i/o timeout")}
		}, connPartly, deadline, 0, true},
		{"refused turns healthy: connected", func(el time.Duration) []connNode {
			if el > 100*time.Millisecond {
				return []connNode{nd("aaaa", "healthy", ""), nd("bbbb", "healthy", "")}
			}
			return []connNode{nd("aaaa", "healthy", ""), nd("bbbb", "unreachable", refusedReason)}
		}, connConnected, 0, 3 * time.Second, false},
		{"0 healthy + refused: never early", func(time.Duration) []connNode {
			return []connNode{nd("bbbb", "unreachable", refusedReason)}
		}, connFailed, deadline, 0, true},
		{"refused set changes: timer restarts", func(el time.Duration) []connNode {
			n := []connNode{nd("aaaa", "healthy", ""), nd("bbbb", "unreachable", refusedReason)}
			if el > 200*time.Millisecond {
				n = append(n, nd("cccc", "unreachable", refusedReason))
			}
			return n
		}, connPartly, 500 * time.Millisecond, 3 * time.Second, false},
		{"all healthy: at once", func(time.Duration) []connNode {
			return []connNode{nd("aaaa", "healthy", "")}
		}, connConnected, 0, 200 * time.Millisecond, false},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			base := fakeDaemon(t, r.nodes)
			t0 := time.Now()
			res := waitConnected(base, deadline)
			el := time.Since(t0)
			if res.outcome != r.want {
				t.Fatalf("outcome %d want %d", res.outcome, r.want)
			}
			if el < r.minTime-100*time.Millisecond {
				t.Fatalf("returned after %v, want at least %v", el, r.minTime)
			}
			if r.maxTime > 0 && el > r.maxTime {
				t.Fatalf("returned after %v, want under %v", el, r.maxTime)
			}
		})
	}
}

func TestFixForRefused(t *testing.T) {
	got := fixFor(refusedReason)
	for _, w := range []string{"pipedpeer status", "pipedpeer start"} {
		if !strings.Contains(got, w) {
			t.Fatalf("refused fix %q lacks %q", got, w)
		}
	}
	if strings.Contains(got, "4242/tcp") {
		t.Fatalf("refused fix still blames the port: %q", got)
	}
	if g := fixFor("i/o timeout"); !strings.Contains(g, "4242/tcp") {
		t.Fatalf("timeout fix lost the port text: %q", g)
	}
}
