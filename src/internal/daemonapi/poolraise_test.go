package daemonapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// raisingFunc records every call in a file and raises on item 2.
func raisingFunc(callLog string) string {
	return "def run(x):\n" +
		"    with open(" + strconv.Quote(callLog) + ", 'a') as f:\n" +
		"        f.write('%d\\n' % x)\n" +
		"    if x == 2:\n" +
		"        raise ValueError('boom %d' % x)\n" +
		"    return x\n"
}

// A function that raises is an answer, not a dead worker: it runs once, the
// error names the exception, and the warm worker stays.
func TestRunLocalUserExceptionRunsOnce(t *testing.T) {
	cs := newCountingStore(t)
	pm := newPoolManager()
	defer pm.stopAll()

	callLog := filepath.Join(t.TempDir(), "calls")
	c := &chunk{funcSrc: raisingFunc(callLog), funcName: "run",
		items: []json.RawMessage{json.RawMessage("1"), json.RawMessage("2"), json.RawMessage("3")}}
	_, err := pm.runLocal(cs.runPath, cs.path, c)
	if err == nil || !strings.Contains(err.Error(), "ValueError: boom 2") {
		t.Fatalf("error = %v, want the function's ValueError", err)
	}
	calls, _ := os.ReadFile(callLog)
	seen := map[string]int{}
	for _, item := range strings.Fields(string(calls)) {
		seen[item]++
	}
	if seen["2"] != 1 || seen["1"] > 1 || seen["3"] > 1 {
		t.Fatalf("function calls = %q, want each item at most once and 2 exactly once", calls)
	}
	w := pm.workerFor(cs.path)
	if w == nil || w.dead() {
		t.Fatal("the warm worker was dropped because the function raised")
	}
	if _, err := pm.runLocal(cs.runPath, cs.path, oneItem(identityFunc)); err != nil {
		t.Fatalf("chunk after the exception: %v", err)
	}
	if n := len(cs.warmPids(t)); n != 1 {
		t.Fatalf("%d warm workers started, want 1: the exception cost a worker", n)
	}
}

// A function that raised on a peer has run. Trying the next peer, or running
// the part here, would run it again.
func TestRunPartUserExceptionOnPeerIsNotRetried(t *testing.T) {
	cs := newCountingStore(t)
	callLog := filepath.Join(t.TempDir(), "calls")
	peerStore := t.TempDir()
	raising := New("raising-peer-xxxxxxxx")
	defer raising.StopWarmWorkers()
	raisingPeer := httptest.NewServer(raising.Handler())
	defer raisingPeer.Close()
	fakeRun(t, peerStore)
	nextPeerHits := 0
	nextPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		nextPeerHits++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer nextPeer.Close()

	pm := newPoolManager()
	defer pm.stopAll()
	ch := &chunk{funcSrc: raisingFunc(callLog), funcName: "run", dead: new(deadPeers),
		items: []json.RawMessage{json.RawMessage("2")}}
	peers := []string{raisingPeer.Listener.Addr().String(), nextPeer.Listener.Addr().String()}
	_, err := pm.runPart(ch, 0, part{c: ch, peers: peers}, cs.runPath, peerStore, false)
	if err == nil || !strings.Contains(err.Error(), "ValueError: boom 2") {
		t.Fatalf("part error = %v, want the peer's ValueError", err)
	}
	if nextPeerHits != 0 {
		t.Fatalf("the next peer was asked %d time(s) after the function raised", nextPeerHits)
	}
	if calls, _ := os.ReadFile(callLog); strings.Count(string(calls), "2") != 1 {
		t.Fatalf("function calls = %q, want one", calls)
	}
	if n := len(cs.warmPids(t)); n != 0 {
		t.Fatalf("the part ran here after the function raised on a peer (%d local workers)", n)
	}
	if ch.dead.has(peers[0]) {
		t.Fatal("a peer that reported an exception was marked dead")
	}
}
