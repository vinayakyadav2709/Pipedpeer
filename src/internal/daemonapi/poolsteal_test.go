package daemonapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestQueuedPartStartsOnAFreePeer: a part queued for a node whose part is
// overdue starts on a free candidate; one whose planned node is predicted to
// free up well before the stand-in could finish keeps waiting for it.
func TestQueuedPartStartsOnAFreePeer(t *testing.T) {
	var b flightBook
	b.mu.Lock()
	b.node("slow").busy[1] = flight{start: time.Now().Add(-3 * time.Second), pred: 1}
	b.node("busy").busy[2] = flight{start: time.Now(), pred: 30}
	b.mu.Unlock()

	k, release, err := b.acquireFirst(context.Background(), []slot{{"slow", 1, 1}, {"fast", 1, 1}})
	if err != nil || k != 1 {
		t.Fatalf("overdue planned node: took candidate %d (%v), want the free one", k, err)
	}
	release()

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	if k, _, err := b.acquireFirst(ctx, []slot{{"busy", 1, 1}, {"crawl", 1, 100}}); err == nil {
		t.Fatalf("took candidate %d; a 100 s stand-in should not beat waiting ~31 s", k)
	}
}

// TestLatePartCopiesToAFreePeer: a part late on a slow peer gets a copy on a
// free peer, which wins long before the slow one answers, with no local copy.
func TestLatePartCopiesToAFreePeer(t *testing.T) {
	reply := func(delay time.Duration) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"results": []map[string]string{
				{"pickle": base64.StdEncoding.EncodeToString([]byte("x"))}}})
		}))
	}
	slow, fast := reply(20*time.Second), reply(0)
	defer slow.Close()
	defer fast.Close()
	slowAddr, fastAddr := strings.TrimPrefix(slow.URL, "http://"), strings.TrimPrefix(fast.URL, "http://")
	pm := newPoolManager()
	ch := &chunk{funcSrc: "def run(x):\n    return x\n", items: []json.RawMessage{json.RawMessage("1")}}
	start := time.Now()
	// Expected 0.1 s on the slow peer: late after 1 s. No local estimate, so
	// only a peer can take the copy.
	_, _, where, raced, err := pm.runRemoteRacing(0, slowAddr, "/nonexistent/bin/run", "/nonexistent", ch, time.Minute, 0.1, 0, fastAddr)
	if err != nil || where != "peer:"+fastAddr || raced {
		t.Fatalf("where %q raced %v err %v; want the free peer's copy to win", where, raced, err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("took %s; the copy should have won soon after the part was late", d)
	}
	if n := pm.book.held(pm.key(fastAddr)); n != 0 {
		t.Errorf("the backup peer still holds %d parts", n)
	}
}

// TestSmallChunkSkipsTheSlowPeer: a chunk too small to split goes whole to
// the peer predicted to finish it first, not round-robin onto a slow one;
// unmeasured idle peers still take turns.
func TestSmallChunkSkipsTheSlowPeer(t *testing.T) {
	pm := newPoolManager()
	peers := []string{"fast", "slow"}
	if a, b := pm.soonestPeer(peers, 0, "", 4), pm.soonestPeer(peers, 1, "", 4); a == b {
		t.Errorf("unmeasured peers: rotation 0 and 1 both picked %d", a)
	}
	pm.rates.observe("fast", 10, time.Second)
	pm.rates.observe("slow", 1, time.Second)
	for r := range 2 {
		if got := pm.soonestPeer(peers, r, "", 4); peers[got] != "fast" {
			t.Errorf("rotation %d picked %s", r, peers[got])
		}
	}
}
