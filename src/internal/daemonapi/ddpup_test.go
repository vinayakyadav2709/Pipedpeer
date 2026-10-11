package daemonapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ddpSyncHeader posts one reduce sync with extra header fields and returns
// the reply's header line.
func ddpSyncHeader(t *testing.T, ts *httptest.Server, extra map[string]any) map[string]any {
	t.Helper()
	hdr := map[string]any{"dtype": "float32", "count": 1}
	for k, v := range extra {
		hdr[k] = v
	}
	raw, _ := json.Marshal(hdr)
	body := putFrame(append(raw, '\n'), encodeF32([]float32{1}))
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ddp/sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", DDPReduceContentType)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reply, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sync %v: HTTP %d %s", extra, resp.StatusCode, reply)
	}
	var out map[string]any
	if err := json.Unmarshal(reply[:bytes.IndexByte(reply, '\n')], &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func postJSON(t *testing.T, ts *httptest.Server, path string, v any) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(v)
	resp, err := ts.Client().Post(ts.URL+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// ringOfTwo runs one sync of a two-rank ring, optionally reporting a sharded
// DataLoader.
func ringOfTwo(t *testing.T, ts *httptest.Server, group string, seq int64, sharded bool) []map[string]any {
	t.Helper()
	out := make([]map[string]any, 2)
	var wg sync.WaitGroup
	for rank := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[rank] = ddpSyncHeader(t, ts, map[string]any{
				"group": group, "seq": seq, "rank": rank, "world": 2, "sharded": sharded})
		}()
	}
	wg.Wait()
	return out
}

// TestARankJoinsTheRing: a third rank says it is ready; both ranks hear at
// the same sync that the ring is now three, with a third of the data each
// and rank 0 handing over the model; the joiner gets that model, and the
// ring's next round waits for all three.
func TestARankJoinsTheRing(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	ringOfTwo(t, ts, "g", 1, true)

	if code, out := postJSON(t, ts, "/v1/ddp/join", map[string]any{"group": "g", "rank": 2, "world": 3, "check": true}); code != 200 {
		t.Fatalf("check refused: %d %v", code, out)
	}
	s.ddp.mu.Lock()
	if s.ddp.runs["g"].join != nil || s.ddp.runs["g"].world != 2 {
		t.Error("a check changed the ring")
	}
	s.ddp.mu.Unlock()

	if code, out := postJSON(t, ts, "/v1/ddp/join", map[string]any{"group": "g", "rank": 2, "world": 3}); code != 200 {
		t.Fatalf("join refused: %d %v", code, out)
	}
	if code, _ := postJSON(t, ts, "/v1/ddp/join", map[string]any{"group": "g", "rank": 3, "world": 4, "check": true}); code != http.StatusConflict {
		t.Errorf("a second joiner was let in while one is still joining (%d)", code)
	}

	// The joiner waits for the ring's boundary.
	got := make(chan []byte, 1)
	go func() {
		for {
			resp, err := ts.Client().Get(ts.URL + "/v1/ddp/snapshot?group=g&rank=2")
			if err != nil {
				t.Error(err)
				got <- nil
				return
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				got <- b
				return
			}
			if resp.StatusCode != http.StatusNoContent {
				t.Errorf("snapshot poll: HTTP %d %s", resp.StatusCode, b)
				got <- nil
				return
			}
		}
	}()

	for _, rep := range ringOfTwo(t, ts, "g", 2, true) {
		if rep["world"] != float64(3) || rep["join"] != float64(2) || rep["from"] != float64(0) {
			t.Fatalf("reply does not announce the join: %v", rep)
		}
		w, _ := rep["weights"].([]any)
		if len(w) != 3 || w[2].(float64) < 0.333 || w[2].(float64) > 0.334 {
			t.Errorf("joiner's share %v, want a third", w)
		}
	}
	if rep := ringOfTwo(t, ts, "g", 3, true); rep[0]["join"] != nil || rep[1]["join"] != nil {
		t.Error("the join was announced twice")
	}

	// Rank 0 hands over at its boundary.
	hdr, _ := json.Marshal(map[string]any{"group": "g", "rank": 0, "join": 2, "boundary": 4, "seq": 3, "world": 3})
	snap := putFrame(append(hdr, '\n'), []byte("model+optimizer"))
	resp, err := ts.Client().Post(ts.URL+"/v1/ddp/snapshot", DDPFramesContentType, bytes.NewReader(snap))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("snapshot upload: HTTP %d", resp.StatusCode)
	}
	select {
	case b := <-got:
		if !bytes.Equal(b, snap) {
			t.Fatalf("joiner got %q, want the snapshot as sent", b)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the joiner never got the snapshot")
	}

	// The next round waits for three.
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for rank := range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rep := ddpSyncHeader(t, ts, map[string]any{"group": "g", "seq": 4, "rank": rank, "world": 3})
				if rep["ranks"] != float64(3) {
					t.Errorf("rank %d: round averaged %v ranks, want 3", rank, rep["ranks"])
				}
				if rank == 2 && rep["join"] != nil {
					t.Error("the joiner was told about its own arrival")
				}
			}()
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the grown ring's first round never completed")
	}
}

// TestAJoinIsRefusedForAScriptThatSlicesItsOwnTensors, finally, so the
// submitter says it once and stops trying.
func TestAJoinIsRefusedForAScriptThatSlicesItsOwnTensors(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	ringOfTwo(t, ts, "g", 1, false)
	code, out := postJSON(t, ts, "/v1/ddp/join", map[string]any{"group": "g", "rank": 2, "world": 3, "check": true})
	if code != http.StatusConflict || out["final"] != true ||
		!strings.Contains(out["error"].(string), "slices its own tensors") {
		t.Fatalf("got %d %v, want a final refusal naming the reason", code, out)
	}
}

// TestAJoinerThatDiesBeforeItIsReadyLeavesNoHole: its index is skipped by
// the next joiner, and the ring never waits for it.
func TestAJoinerThatDiesBeforeItIsReadyLeavesNoHole(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ringOfTwo(t, ts, "g", 1, true)

	if code, _ := postJSON(t, ts, "/v1/ddp/join", map[string]any{"group": "g", "rank": 3, "world": 4, "check": true}); code != http.StatusConflict {
		t.Fatal("rank 3 let in while rank 2 is still starting")
	}
	s.markRankDown("g", 2, 4, "failed to start")
	s.ddp.mu.Lock()
	if s.ddp.dropsBelow("g", 2) != 0 || s.ddp.runs["g"].note != "" {
		t.Error("a joiner that never joined changed the ring")
	}
	s.ddp.mu.Unlock()
	if code, out := postJSON(t, ts, "/v1/ddp/join", map[string]any{"group": "g", "rank": 3, "world": 4}); code != 200 {
		t.Fatalf("rank 3 refused: %v", out)
	}
	s.ddp.mu.Lock()
	defer s.ddp.mu.Unlock()
	r := s.ddp.runs["g"]
	if r.world != 4 || s.ddp.dropsBelow("g", 4) != 1 || r.weights[2] != 0 {
		t.Errorf("world %d, drops %d, shares %v; want 4, 1 and none for rank 2",
			r.world, s.ddp.dropsBelow("g", 4), r.weights)
	}
}

// TestARefitReachesEveryRank. It was decided per reply, so only the rank
// whose reply hit the cadence was told, and that rank alone resliced its
// data - leaving the ring at different step counts.
func TestARefitReachesEveryRank(t *testing.T) {
	old := rebalanceEvery
	rebalanceEvery = 2
	defer func() { rebalanceEvery = old }()

	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	told := map[int]int64{}
	for seq := int64(1); seq <= 6; seq++ {
		var wg sync.WaitGroup
		var mu sync.Mutex
		for rank := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Rank 0 does three times rank 1's samples in the same time.
				rep := ddpSyncHeader(t, ts, map[string]any{"group": "g", "seq": seq, "rank": rank,
					"world": 2, "step_ms": 100, "sync_ms": 1, "samples": 3000 - 2000*rank})
				if rep["weights"] != nil {
					mu.Lock()
					if _, ok := told[rank]; !ok {
						told[rank] = seq
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
	}
	if len(told) != 2 || told[0] != told[1] {
		t.Fatalf("new shares reached ranks at syncs %v; want both, at the same one", told)
	}
}

// failingWriter is a joiner whose connection breaks mid-snapshot.
type failingWriter struct{ h http.Header }

func (f *failingWriter) Header() http.Header       { return f.h }
func (f *failingWriter) WriteHeader(int)           {}
func (f *failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// A snapshot write that fails leaves the join in place, so the joiner's next
// poll gets the snapshot instead of 410.
func TestAFailedSnapshotWriteKeepsTheJoin(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ringOfTwo(t, ts, "g", 1, true)
	if code, out := postJSON(t, ts, "/v1/ddp/join", map[string]any{"group": "g", "rank": 2, "world": 3}); code != 200 {
		t.Fatalf("join refused: %d %v", code, out)
	}
	hdr, _ := json.Marshal(map[string]any{"group": "g", "rank": 0, "join": 2})
	snap := putFrame(append(hdr, '\n'), []byte("model+optimizer"))
	resp, err := ts.Client().Post(ts.URL+"/v1/ddp/snapshot", DDPFramesContentType, bytes.NewReader(snap))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("snapshot upload: HTTP %d", resp.StatusCode)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/ddp/snapshot?group=g&rank=2", nil)
	s.handleDDPSnapshotGet(&failingWriter{h: http.Header{}}, req)

	rec := httptest.NewRecorder()
	s.handleDDPSnapshotGet(rec, httptest.NewRequest(http.MethodGet, "/v1/ddp/snapshot?group=g&rank=2", nil))
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), snap) {
		t.Fatalf("retry after a broken write: HTTP %d %q, want 200 and the snapshot", rec.Code, rec.Body.Bytes())
	}
	s.ddp.mu.Lock()
	defer s.ddp.mu.Unlock()
	if s.ddp.runs["g"].join != nil {
		t.Error("the join is still open after the snapshot was delivered")
	}
}
