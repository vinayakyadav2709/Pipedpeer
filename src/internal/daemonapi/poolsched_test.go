package daemonapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSinkPeerLast keeps the submitting machine eligible but last in spill
// order — an idle orchestrator must never outrank real workers.
func TestSinkPeerLast(t *testing.T) {
	peers := []string{"10.0.0.5:4242", "10.0.0.1:4242", "10.0.0.9:4242"}
	got := sinkPeerLast(peers, "10.0.0.1:4242")
	want := []string{"10.0.0.5:4242", "10.0.0.9:4242", "10.0.0.1:4242"}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("expected %v, got %v", want, got)
	}

	if got := sinkPeerLast(peers, ""); len(got) != 3 {
		t.Fatalf("empty submitter must be a no-op, got %v", got)
	}

	alone := sinkPeerLast([]string{"10.0.0.1:4242"}, "10.0.0.1:4242")
	if len(alone) != 1 || alone[0] != "10.0.0.1:4242" {
		t.Fatalf("submitter-only pool must stay intact, got %v", alone)
	}

	same := sinkPeerLast(peers, "not-in-list:1")
	if len(same) != 3 || same[0] != peers[0] {
		t.Fatalf("absent submitter must not reorder, got %v", same)
	}
}

// TestFanWidthRespectsFreeMemory pins the bound that keeps a wide chunk from
// pushing a node over. Nothing below this enforces anything — the sandbox
// carries no cgroup — so if this arithmetic is wrong the OOM killer is the
// next line of defence.
func TestFanWidthRespectsFreeMemory(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	probe := `
import os, sys
ns = {"os": os, "sys": sys}
exec(sys.argv[1], ns)
fan = ns["_fan_width"]

# One item never forks.
assert fan(1, 0) == 1, fan(1, 0)

# An estimate far larger than RAM collapses the width to one.
huge = ns["_free_bytes"]() * 100 or 1 << 60
assert fan(64, huge) == 1, ("huge estimate not bounded", fan(64, huge))

# No estimate must still be bounded by memory, not just by cores: with the
# 256MB-per-item assumption a node cannot fan wider than its free RAM allows.
free = ns["_free_bytes"]()
if free > 0:
    cap = max(1, int(free * 0.4) // (256 << 20))
    assert fan(4096, 0) <= max(1, min(os.cpu_count() or 1, cap)), fan(4096, 0)

# An explicit override wins, but never exceeds the item count.
os.environ["PIPEDPEER_WORKER_PROCS"] = "3"
assert fan(64, 0) == 3, fan(64, 0)
assert fan(2, 0) == 2, fan(2, 0)
print("fan-ok")
`
	out, err := exec.Command(python, "-c", probe, chunkFanOut).CombinedOutput()
	if err != nil {
		t.Fatalf("fan width probe failed:\n%s\n%v", out, err)
	}
	if !strings.Contains(string(out), "fan-ok") {
		t.Fatalf("unexpected probe output: %q", out)
	}
}

// A frames request large enough to be split into micro-chunks must still be
// answered in frames. It was answered in JSON, which the shim could not parse.
func TestPoolMapMicroChunksAnswerInFrames(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	store := fakeRun(t, t.TempDir())

	s := New("pool-test-microframes")
	defer s.StopWarmWorkers()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	emit := `
import base64, pickle, sys
sys.stdout.write("\n".join(base64.b64encode(pickle.dumps(v)).decode() for v in (1, 2, 3, 4)))
`
	out, err := exec.Command(python, "-c", emit).Output()
	if err != nil {
		t.Fatalf("emit pickles: %v", err)
	}
	var items []json.RawMessage
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		b, _ := base64.StdEncoding.DecodeString(l)
		items = append(items, b)
	}

	// 60% of free memory: over the 40% micro-chunk line, under admission.
	required := memAvailBytes(t) * 6 / 10
	hdr := []byte(fmt.Sprintf(`{"func_src": "def run(x):\n    return x * 2\n", "func_name": "run", "items_frames": 4, "required_mem": %d}`, required))
	req, err := http.NewRequest("POST", srv.URL+"/v1/pool/map", bytes.NewReader(buildFrames(hdr, nil, items)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", framesContentType)
	req.Header.Set("X-Pipedpeer-Store", store)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d: %s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != framesContentType {
		t.Fatalf("micro-chunked frames request answered as %q, want frames", ct)
	}
	nl := bytes.IndexByte(body, '\n')
	if nl < 0 {
		t.Fatal("frames response has no header line")
	}
	var outHdr struct {
		ResultsFrames int `json:"results_frames"`
		Receipt       struct {
			Parts []provPart `json:"parts"`
		} `json:"receipt"`
	}
	if err := json.Unmarshal(body[:nl], &outHdr); err != nil {
		t.Fatalf("bad header %q: %v", body[:nl], err)
	}
	if outHdr.ResultsFrames != 4 {
		t.Fatalf("results_frames = %d, want 4", outHdr.ResultsFrames)
	}
	total := 0
	for _, p := range outHdr.Receipt.Parts {
		total += p.Items
	}
	if total != 4 {
		t.Fatalf("receipt accounts for %d items, want 4: %+v", total, outHdr.Receipt.Parts)
	}
}

// A peer that took a forwarded request reports its share as "local"; passed
// through unchanged, the submitter read that as work done on the origin.
func TestRelabelForwardedReceipt(t *testing.T) {
	body := []byte(`{"results_frames": 2, "receipt": {"node": "peer-node", "parts": [{"where": "local", "items": 1, "ms": 3}, {"where": "peer:10.0.0.3:4242", "items": 1, "ms": 4}]}}` + "\n" + "\x00\x00\x00\x01a")
	out := relabelForwardedReceipt(body, "10.0.0.2:4242")

	nl := bytes.IndexByte(out, '\n')
	var hdr struct {
		ResultsFrames int `json:"results_frames"`
		Receipt       struct {
			Parts []provPart `json:"parts"`
		} `json:"receipt"`
	}
	if err := json.Unmarshal(out[:nl], &hdr); err != nil {
		t.Fatal(err)
	}
	if hdr.ResultsFrames != 2 {
		t.Errorf("results_frames = %d, want 2", hdr.ResultsFrames)
	}
	got := []string{hdr.Receipt.Parts[0].Where, hdr.Receipt.Parts[1].Where}
	want := []string{"peer:10.0.0.2:4242", "peer:10.0.0.3:4242"}
	if got[0] != want[0] || got[1] != want[1] {
		t.Errorf("parts = %v, want %v", got, want)
	}
	if !bytes.Equal(out[nl:], body[bytes.IndexByte(body, '\n'):]) {
		t.Error("result frames were altered")
	}
}

// TestSlowPeerEndsThePeerWalk. A part whose peer runs out of time goes local;
// it is not handed to the next peer to start over. A one-item svd part tried
// four peers at 300 s each and hung for over 15 minutes.
func TestSlowPeerEndsThePeerWalk(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	os.MkdirAll(filepath.Join(storePath, "bin"), 0755)
	os.WriteFile(filepath.Join(storePath, "bin", "run"), []byte("#!/bin/sh\nexec "+python+" \"$@\"\n"), 0755)
	runPath := filepath.Join(storePath, "bin", "run")

	old := remotePartBudget
	remotePartBudget = 300 * time.Millisecond
	t.Cleanup(func() { remotePartBudget = old })

	var mu sync.Mutex
	asked := 0
	release := make(chan struct{})
	cancelled := make(chan struct{}, 2)
	slow := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		asked++
		mu.Unlock()
		// Read the body as the real handler does: only then does the server
		// watch the connection and cancel r.Context() when the client leaves.
		io.Copy(io.Discard, r.Body)
		select { // never answers in time
		case <-r.Context().Done():
			cancelled <- struct{}{}
		case <-release:
		}
	}
	p1 := httptest.NewServer(http.HandlerFunc(slow))
	p2 := httptest.NewServer(http.HandlerFunc(slow))
	defer p1.Close()
	defer p2.Close()
	defer close(release)

	s := New("slowpeer-node-xxxxxxxx")
	defer s.StopWarmWorkers()
	peers := []string{p1.Listener.Addr().String(), p2.Listener.Addr().String()}
	s.pool.SetPeerFn(func(_, _ string, _ http.Header) []string { return peers })

	ch := &chunk{funcSrc: doubleSrc, funcName: "double", items: []json.RawMessage{json.RawMessage(`21`)}, force: true}
	results, err := s.pool.runChunk(runPath, storePath, ch, "")
	if err != nil {
		t.Fatalf("chunk failed: %v", err)
	}
	if got := unpickleInts(t, results); len(got) != 1 || got[0] != 42 {
		t.Fatalf("results = %v, want [42] from the local fallback", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if asked != 1 {
		t.Errorf("%d peers were tried for one slow item; a timeout must end the walk", asked)
	}
	select { // the local copy won, so the peer's copy must be cancelled
	case <-cancelled:
	case <-time.After(10 * time.Second):
		t.Error("the slow peer's request was not cancelled after the local copy won")
	}
}

// TestSlowPeerWinsTheRace. A peer past its budget keeps running while a local
// copy starts; when the peer finishes first its result is taken instead of
// being thrown away for a local re-run from scratch.
func TestSlowPeerWinsTheRace(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	os.MkdirAll(filepath.Join(storePath, "bin"), 0755)
	os.WriteFile(filepath.Join(storePath, "bin", "run"), []byte("#!/bin/sh\nexec "+python+" \"$@\"\n"), 0755)
	runPath := filepath.Join(storePath, "bin", "run")

	old := remotePartBudget
	remotePartBudget = 300 * time.Millisecond
	t.Cleanup(func() { remotePartBudget = old })

	// pickle.dumps(99, protocol=2): distinct from the local answer (42).
	peerResult := base64.StdEncoding.EncodeToString([]byte("\x80\x02Kc."))
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select { // past the budget, but long before the local copy can finish
		case <-time.After(1500 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"results": []map[string]string{{"pickle": peerResult}}})
	}))
	defer peer.Close()

	s := New("racepeer-node-xxxxxxxx")
	defer s.StopWarmWorkers()
	s.pool.SetPeerFn(func(_, _ string, _ http.Header) []string { return []string{peer.Listener.Addr().String()} })

	const slowSrc = "import time\ndef slow(x):\n    time.sleep(30)\n    return x * 2\n"
	ch := &chunk{funcSrc: slowSrc, funcName: "slow", items: []json.RawMessage{json.RawMessage(`21`)}, force: true}
	started := time.Now()
	results, err := s.pool.runChunk(runPath, storePath, ch, "")
	if err != nil {
		t.Fatalf("chunk failed: %v", err)
	}
	if got := unpickleInts(t, results); len(got) != 1 || got[0] != 99 {
		t.Fatalf("results = %v, want [99] from the peer that finished first", got)
	}
	if d := time.Since(started); d > 20*time.Second {
		t.Errorf("took %s: the chunk waited for the losing local copy", d)
	}
}

// The depth gate holds a node to its depth: a part beyond it waits in the
// daemon's queue for that node until one leaves, and the waiting part counts
// as that node's backlog only once it runs.
func TestFlightBookHoldsANodeToItsDepth(t *testing.T) {
	var b flightBook
	ctx := context.Background()
	r1, err := b.acquire(ctx, "n1", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	r2, _ := b.acquire(ctx, "n1", 2, 10)
	if got := b.held("n1"); got != 2 {
		t.Fatalf("held = %d, want 2", got)
	}
	if q := b.backlog("n1", time.Now()); q < 19 || q > 20 {
		t.Errorf("backlog = %.2f, want ~20 s", q)
	}
	got := make(chan func(), 1)
	go func() {
		r3, _ := b.acquire(ctx, "n1", 2, 10)
		got <- r3
	}()
	select {
	case <-got:
		t.Fatal("a third part ran on a node of depth 2")
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := b.acquire(ctx, "n2", 2, 1); err != nil {
		t.Fatal("another node's depth blocked:", err)
	}
	r1()
	r1() // a second release is harmless
	select {
	case r3 := <-got:
		r3()
	case <-time.After(5 * time.Second):
		t.Fatal("the queued part never started after one left")
	}
	r2()
	cctx, cancel := context.WithCancel(ctx)
	b.acquire(ctx, "n3", 1, 1)
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	if _, err := b.acquire(cctx, "n3", 1, 1); err == nil {
		t.Error("a cancelled request still got a slot")
	}
}

// /v1/pool/stats?store= paces the shim: each unmeasured peer holds 2 parts
// (one running, one queued), so 2 peers mean 4 chunks in flight, not 4 per
// cluster whatever its size.
func TestPoolStatsPacesTheShim(t *testing.T) {
	s := New("pace-node-xxxxxxxx")
	s.pool.SetPeerFn(func(store, submitter string, _ http.Header) []string {
		if store != "/nix/store/x" {
			return nil
		}
		return []string{"10.0.0.2:4242", "10.0.0.3:4242"}
	})
	s.pool.coresFn = func(string) int { return 8 }
	rec := httptest.NewRecorder()
	s.handlePoolStats(rec, httptest.NewRequest(http.MethodGet, "/v1/pool/stats?store=%2Fnix%2Fstore%2Fx", nil))
	var got map[string]int64
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["peers"] != 2 || got["inflight"] != 4 || got["cores"] != 16 || got["part_ms"] != 450 {
		t.Errorf("pacing = %v, want peers 2, inflight 4, cores 16, part_ms 450", got)
	}
	if _, ok := got["chunks_received"]; !ok {
		t.Error("the counters went missing from /v1/pool/stats")
	}
}

// At most one tail backup runs at a time: two late parts on a slow peer, and
// only one of them starts a copy here before the hard budget.
func TestAtMostOneTailBackup(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		time.Sleep(2500 * time.Millisecond)
		writeJSON(w, http.StatusOK, map[string]any{"results": []map[string]string{
			{"pickle": base64.StdEncoding.EncodeToString([]byte("x"))}}})
	}))
	defer peer.Close()
	addr := strings.TrimPrefix(peer.URL, "http://")
	pm := newPoolManager()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch := &chunk{funcSrc: "def run(x):\n    return x\n", items: []json.RawMessage{json.RawMessage("1")}}
			// Expected 0.1 s on the peer, 0.01 s here: late after 1 s, and a
			// copy here would save time. The local copy fails (no closure),
			// so the peer's answer wins either way.
			_, _, where, _, err := pm.runRemoteRacing(i, addr, "/nonexistent/bin/run", "/nonexistent", ch, time.Minute, 0.1, 0.01)
			if err == nil && where != "peer:"+addr {
				err = fmt.Errorf("won by %q", where)
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("part %d: %v", i, err)
		}
	}
	// runLocal counts its items before it tries to run them.
	if got := pm.stats.snapshot()["items_executed_here"]; got != 1 {
		t.Errorf("%d backup copies started, want exactly 1", got)
	}
	if pm.backups.Load() != 0 {
		t.Error("the backup slot was not given back")
	}
}

// Steps of a part that ran on a peer carry that peer's node id, taken from
// its reply, never the submitter's: "where it ran" in every view reads it.
func TestRemotePartStepsCarryThePeersNodeID(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", framesContentType)
		w.Write(putFrame(poolFramesHeader(1, "peer-node-id", []provPart{{Where: "local", Items: 1, Ms: 3}}), []byte("x")))
	}))
	defer peer.Close()
	addr := strings.TrimPrefix(peer.URL, "http://")
	pm := newPoolManager()
	pm.selfID = "origin-node-id"
	pm.SetPeerFn(func(string, string, http.Header) []string { return []string{addr} })
	ch := &chunk{funcSrc: "def run(x):\n    return x\n", frames: true, force: true,
		items: []json.RawMessage{json.RawMessage("1")}, prov: &provenance{}}
	if _, err := pm.runChunk("/nonexistent/bin/run", "/nonexistent", ch, ""); err != nil {
		t.Fatal(err)
	}
	parts := ch.prov.snapshot()
	if len(parts) != 1 || parts[0].Where != "peer:"+addr || len(parts[0].Steps) == 0 {
		t.Fatalf("parts = %+v, want one peer part with steps", parts)
	}
	for _, st := range parts[0].Steps {
		if st.Machine != "peer-node-id" {
			t.Errorf("step %s carries machine %q, want the peer's node id", st.Name, st.Machine)
		}
	}
}
