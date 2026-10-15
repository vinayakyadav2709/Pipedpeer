package daemonapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// ddpReducePost sends one rank's contribution in reduce mode and returns the
// averaged result.
func ddpReducePost(t *testing.T, ts *httptest.Server, group string, seq int64, rank, world int, vals []float32) (int, []float32) {
	t.Helper()
	hdr, _ := json.Marshal(map[string]any{
		"group": group, "seq": seq, "rank": rank, "world": world,
		"dtype": "float32", "count": len(vals),
	})
	body := append(hdr, '\n')
	body = putFrame(body, encodeF32(vals))

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ddp/sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", DDPReduceContentType)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("rank %d: %v", rank, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil
	}
	nl := bytes.IndexByte(raw, '\n')
	if nl < 0 {
		t.Fatalf("rank %d: no header line in reply", rank)
	}
	blob, _, err := readFrame(raw[nl+1:])
	if err != nil {
		t.Fatalf("rank %d: %v", rank, err)
	}
	return resp.StatusCode, decodeF32(blob)
}

// TestDDPReduceReturnsOneAveragedModel is the change that made distributed
// training worth doing on a normal link.
//
// The blackboard handed every rank the whole set back, so the reply was the
// model times the ring size and every rank then averaged it itself. Measured
// on two machines, that was 3.0 MiB down per rank per step against 1.5 MiB up,
// and 60s of sync for a job that took 2.9s on one machine. The daemon now
// averages and replies with one model.
func TestDDPReduceReturnsOneAveragedModel(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	const world = 3
	contributions := [][]float32{
		{1, 2, 3, 4},
		{3, 4, 5, 6},
		{5, 6, 7, 8},
	}
	results := make([][]float32, world)
	var wg sync.WaitGroup
	for r := 0; r < world; r++ {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			code, got := ddpReducePost(t, ts, "g", 1, rank, world, contributions[rank])
			if code != http.StatusOK {
				t.Errorf("rank %d: status %d", rank, code)
				return
			}
			results[rank] = got
		}(r)
	}
	wg.Wait()

	want := []float32{3, 4, 5, 6}
	for rank, got := range results {
		if len(got) != len(want) {
			t.Fatalf("rank %d got %d values, want %d — the reply should be one "+
				"model, not one per rank", rank, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("rank %d element %d = %v, want %v", rank, i, got[i], want[i])
			}
		}
	}
}

// TestDDPReduceRejectsAShapeMismatchWithoutHangingTheRing. A rank that
// disagrees about the model's size cannot be folded in - and if the daemon
// simply refused that one rank, every other rank would sit on the barrier
// until the three-minute timeout, blaming a peer that answered fine.
func TestDDPReduceRejectsAShapeMismatchWithoutHangingTheRing(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	done := make(chan int, 2)
	go func() {
		code, _ := ddpReducePost(t, ts, "g", 1, 0, 2, []float32{1, 2, 3, 4})
		done <- code
	}()
	// Give the first rank time to establish the group's shape.
	time.Sleep(50 * time.Millisecond)
	go func() {
		code, _ := ddpReducePost(t, ts, "g", 1, 1, 2, []float32{1, 2})
		done <- code
	}()

	for i := 0; i < 2; i++ {
		select {
		case code := <-done:
			if code == http.StatusOK {
				t.Error("a shape mismatch was accepted; the average would be silently wrong")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a rank is still blocked on the barrier: one bad contribution " +
				"hung the whole ring rather than failing it")
		}
	}
}

// TestDDPReduceAgreesOnOneSyncInterval covers a bug that was invisible except
// as a worse model.
//
// Ranks measure their own link and compute to decide how often to average, and
// they get different answers - 32 and 20 on the same two-machine run. Nothing
// deadlocks, because the sync sequence counts syncs rather than steps, so the
// ranks simply combine models that have taken different numbers of local
// steps. That is not local SGD, and the only symptom is a final loss that is
// quietly worse: 0.0662 against 0.0603 for the same job.
func TestDDPReduceAgreesOnOneSyncInterval(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	proposals := []int{20, 32}
	agreed := make([]int, len(proposals))
	var wg sync.WaitGroup
	for r := range proposals {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			agreed[rank] = ddpReduceSyncEvery(t, ts, "g", 1, rank, len(proposals),
				[]float32{1, 2}, proposals[rank])
		}(r)
	}
	wg.Wait()

	if agreed[0] != agreed[1] {
		t.Fatalf("ranks were told %d and %d; they must average at the same points "+
			"or they are combining models that have taken different numbers of steps",
			agreed[0], agreed[1])
	}
	if agreed[0] != 32 {
		t.Errorf("agreed interval %d, want 32 — the rank spending most of its run "+
			"on sync is the one setting the pace", agreed[0])
	}
}

// ddpReduceSyncEvery posts a contribution proposing an interval and returns
// the interval the daemon handed back.
func ddpReduceSyncEvery(t *testing.T, ts *httptest.Server, group string, seq int64, rank, world int, vals []float32, propose int) int {
	t.Helper()
	hdr, _ := json.Marshal(map[string]any{
		"group": group, "seq": seq, "rank": rank, "world": world,
		"dtype": "float32", "count": len(vals), "sync_every": propose,
	})
	body := append(hdr, '\n')
	body = putFrame(body, encodeF32(vals))

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ddp/sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", DDPReduceContentType)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("rank %d: %v", rank, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	nl := bytes.IndexByte(raw, '\n')
	if nl < 0 {
		t.Fatalf("rank %d: no header in reply", rank)
	}
	var out struct {
		SyncEvery int `json:"sync_every"`
	}
	if err := json.Unmarshal(raw[:nl], &out); err != nil {
		t.Fatalf("rank %d: %v", rank, err)
	}
	return out.SyncEvery
}

// TestDDPDetectsRanksDoingIdenticalWork is the diagnostic for the failure this
// project keeps rediscovering: correct results, no speed-up, and nothing said.
//
// Sharding is automatic only for DataLoader-based code. A script that slices
// its tensors by hand - which both bundled training demos do - leaves every
// rank computing the same batch, so the averaged gradient equals the
// single-process gradient exactly. Measured on two machines: the loss agreed
// to six digits with the single-node run and the job took 97.7s against 55.0s.
// The daemon is the only place holding every rank's contribution, so it is the
// only place that can notice.
func TestDDPDetectsRanksDoingIdenticalWork(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	same := func(rank int) bool {
		return ddpReduceSameWork(t, ts, "same", 1, rank, 2, []float32{1, 2, 3, 4})
	}
	flags := make([]bool, 2)
	var wg sync.WaitGroup
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func(rank int) { defer wg.Done(); flags[rank] = same(rank) }(r)
	}
	wg.Wait()
	for rank, flagged := range flags {
		if !flagged {
			t.Errorf("rank %d was not told its gradients were identical to its "+
				"peer's; the run is pure overhead and says nothing", rank)
		}
	}
}

// TestDDPDoesNotCryWolfOnDifferentGradients. A false positive here would tell
// a correctly sharded run that it is wasting its time.
func TestDDPDoesNotCryWolfOnDifferentGradients(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	payloads := [][]float32{{1, 2, 3, 4}, {1, 2, 3, 5}}
	flags := make([]bool, 2)
	var wg sync.WaitGroup
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			flags[rank] = ddpReduceSameWork(t, ts, "diff", 1, rank, 2, payloads[rank])
		}(r)
	}
	wg.Wait()
	for rank, flagged := range flags {
		if flagged {
			t.Errorf("rank %d was told it duplicates work though the gradients "+
				"differ by one element", rank)
		}
	}
}

// ddpReduceSameWork posts a contribution and reports the daemon's same_work
// verdict.
func ddpReduceSameWork(t *testing.T, ts *httptest.Server, group string, seq int64, rank, world int, vals []float32) bool {
	t.Helper()
	return ddpReduceKind(t, ts, group, seq, rank, world, vals, "grads")
}

func ddpReduceKind(t *testing.T, ts *httptest.Server, group string, seq int64, rank, world int, vals []float32, kind string) bool {
	t.Helper()
	hdr, _ := json.Marshal(map[string]any{
		"group": group, "seq": seq, "rank": rank, "world": world,
		"dtype": "float32", "count": len(vals), "kind": kind,
	})
	body := append(hdr, '\n')
	body = putFrame(body, encodeF32(vals))

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ddp/sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", DDPReduceContentType)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("rank %d: %v", rank, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	nl := bytes.IndexByte(raw, '\n')
	if nl < 0 {
		t.Fatalf("rank %d: no header in reply", rank)
	}
	var out struct {
		SameWork bool `json:"same_work"`
	}
	if err := json.Unmarshal(raw[:nl], &out); err != nil {
		t.Fatalf("rank %d: %v", rank, err)
	}
	return out.SameWork
}

// TestDDPWeightSyncIsNotDuplicatedWork covers a false positive that reported a
// correctly sharded run as pure overhead.
//
// A run opens by broadcasting the initial weights, and those are identical
// across ranks by construction whenever the script seeds its generator - which
// every training script does. Fingerprinting that sync told a run that had
// just sharded its data perfectly well that its machines were duplicating
// work. A warning that cries wolf is worse than no warning.
func TestDDPWeightSyncIsNotDuplicatedWork(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	flags := make([]bool, 2)
	var wg sync.WaitGroup
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			// Identical payloads, as the opening weight broadcast always is.
			flags[rank] = ddpReduceKind(t, ts, "w", 1, rank, 2,
				[]float32{1, 2, 3, 4}, "weights")
		}(r)
	}
	wg.Wait()
	for rank, flagged := range flags {
		if flagged {
			t.Errorf("rank %d was told it duplicates work, but this was the opening "+
				"weight broadcast, which is identical across ranks by design", rank)
		}
	}
}

// ddpReduceFull posts a contribution and returns status plus the whole reply
// header, so a test can read the fields the shim reads.
func ddpReduceFull(t *testing.T, ts *httptest.Server, group string, seq int64, rank, world int, vals []float32) (int, map[string]any, []float32) {
	t.Helper()
	hdr, _ := json.Marshal(map[string]any{
		"group": group, "seq": seq, "rank": rank, "world": world,
		"dtype": "float32", "count": len(vals), "kind": "grads",
	})
	body := append(hdr, '\n')
	body = putFrame(body, encodeF32(vals))

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ddp/sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", DDPReduceContentType)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("rank %d: %v", rank, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil, nil
	}
	nl := bytes.IndexByte(raw, '\n')
	if nl < 0 {
		t.Fatalf("rank %d: no header", rank)
	}
	var head map[string]any
	if err := json.Unmarshal(raw[:nl], &head); err != nil {
		t.Fatalf("rank %d: %v", rank, err)
	}
	blob, _, err := readFrame(raw[nl+1:])
	if err != nil {
		t.Fatalf("rank %d: %v", rank, err)
	}
	return resp.StatusCode, head, decodeF32(blob)
}
