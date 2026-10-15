package daemonapi

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ddpCorePost sends one reduce-mode sync with the given header and payload. It
// returns the status, the reply's JSON header (the error object when the
// status is not 200) and the result frame.
func ddpCorePost(t *testing.T, ts *httptest.Server, hdr map[string]any, payload []byte) (int, map[string]any, []byte) {
	t.Helper()
	raw, _ := json.Marshal(hdr)
	body := putFrame(append(raw, '\n'), payload)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ddp/sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", DDPReduceContentType)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var head map[string]any
	if resp.StatusCode != http.StatusOK {
		_ = json.Unmarshal(data, &head)
		return resp.StatusCode, head, nil
	}
	nl := bytes.IndexByte(data, '\n')
	if nl < 0 {
		t.Fatalf("reply has no header: %q", data)
	}
	if err := json.Unmarshal(data[:nl], &head); err != nil {
		t.Fatal(err)
	}
	blob, _, err := readFrame(data[nl+1:])
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, head, blob
}

// TestMismatchedManifestsAbortEveryRank is the demo's 3-rank failure: the
// lead built a small model on its CPU, the GPU ranks a wide one, and every
// rank died on a bare "HTTP 400". Now the opening round compares the ranks'
// parameter lists, every rank gets the same 409 abort naming both ranks and
// their devices, and a rank arriving later is refused too instead of
// training on alone.
func TestMismatchedManifestsAbortEveryRank(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	small := map[string]any{"names": []string{"0.weight", "0.bias"}, "shapes": [][]int{{2, 4}, {2}},
		"dtypes": []string{"float32", "float32"}, "device": "cpu", "host": "psipl"}
	wide := map[string]any{"names": []string{"0.weight", "0.bias"}, "shapes": [][]int{{8, 4}, {8}},
		"dtypes": []string{"float32", "float32"}, "device": "cuda, RTX 3060", "host": "yeet"}
	type res struct {
		code int
		head map[string]any
	}
	out := make(chan res, 2)
	for rank, m := range []map[string]any{small, wide} {
		go func(rank int, m map[string]any) {
			n := 10
			if rank == 1 {
				n = 40
			}
			code, head, _ := ddpCorePost(t, ts, map[string]any{"group": "g", "seq": 1, "rank": rank, "world": 3,
				"kind": "weights", "dtype": "float32", "count": n, "manifest": m}, encodeF32(make([]float32, n)))
			out <- res{code, head}
		}(rank, m)
	}
	for i := 0; i < 2; i++ {
		select {
		case r := <-out:
			if r.code != http.StatusConflict || r.head["abort"] != true {
				t.Fatalf("got %d %v, want 409 with abort", r.code, r.head)
			}
			why, _ := r.head["error"].(string)
			for _, want := range []string{"different models", "rank 0 on psipl (cpu)", "rank 1 on yeet (cuda, RTX 3060)", "0.weight"} {
				if !strings.Contains(why, want) {
					t.Errorf("the reason %q does not say %q", why, want)
				}
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a rank was left waiting after the models were found to differ")
		}
	}
	code, head, _ := ddpCorePost(t, ts, map[string]any{"group": "g", "seq": 1, "rank": 2, "world": 3,
		"kind": "weights", "dtype": "float32", "count": 10, "manifest": small}, encodeF32(make([]float32, 10)))
	if code != http.StatusConflict || head["abort"] != true {
		t.Fatalf("a late rank got %d %v; an aborted ring must refuse it", code, head)
	}
}

// TestMatchingManifestsTrain: the check passes rings whose ranks agree.
func TestMatchingManifestsTrain(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	m := map[string]any{"shapes": [][]int{{2}}, "dtypes": []string{"float32"}}
	var wg sync.WaitGroup
	for rank := 0; rank < 2; rank++ {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			code, head, _ := ddpCorePost(t, ts, map[string]any{"group": "g", "seq": 1, "rank": rank, "world": 2,
				"kind": "weights", "dtype": "float32", "count": 2, "manifest": m}, encodeF32([]float32{1, 2}))
			if code != http.StatusOK {
				t.Errorf("rank %d: %d %v", rank, code, head)
			}
		}(rank)
	}
	wg.Wait()
}

// TestEveryPeerGoneAbortsTheSurvivor: the demo's surviving rank trained on
// alone at ~10 s a sync for 4m40s after the rest of its ring was gone. A
// later round that closes with only the asking rank is a dead ring: it is
// aborted (409, the shim exits 76) instead of handed a one-rank average.
func TestEveryPeerGoneAbortsTheSurvivor(t *testing.T) {
	old := ddpPartialAfter
	ddpPartialAfter = 300 * time.Millisecond
	defer func() { ddpPartialAfter = old }()

	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	code, head, _ := ddpCorePost(t, ts, map[string]any{"group": "g", "seq": 7, "rank": 0, "world": 2,
		"kind": "grads", "dtype": "float32", "count": 2}, encodeF32([]float32{1, 2}))
	if code != http.StatusConflict || head["abort"] != true {
		t.Fatalf("got %d %v, want 409 abort", code, head)
	}
	if why, _ := head["error"].(string); !strings.Contains(why, "stopped answering") || !strings.Contains(why, "rank 1") {
		t.Errorf("the reason %q does not say which rank went quiet", why)
	}
}

// TestFP32WeightAverageRoundTrips: weights average at full width. Values
// fp16 cannot hold come back as their exact float32 mean.
func TestFP32WeightAverageRoundTrips(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	a := []float32{1.0001, 3.14159265, -2.7182817, 65519.5}
	b := []float32{1.0003, 3.14159265, -2.7182817, 65520.5}
	got := make([][]float32, 2)
	var wg sync.WaitGroup
	for rank, vals := range [][]float32{a, b} {
		wg.Add(1)
		go func(rank int, vals []float32) {
			defer wg.Done()
			code, head, blob := ddpCorePost(t, ts, map[string]any{"group": "g", "seq": 5, "rank": rank, "world": 2,
				"kind": "weights", "dtype": "float32", "count": len(vals)}, encodeF32(vals))
			if code != http.StatusOK {
				t.Errorf("rank %d: %d %v", rank, code, head)
				return
			}
			got[rank] = decodeF32(blob)
		}(rank, vals)
	}
	wg.Wait()
	for i := range a {
		want := float32((float64(a[i]) + float64(b[i])) / 2)
		if got[0][i] != want || got[1][i] != want {
			t.Errorf("element %d: ranks got %v and %v, want exactly %v", i, got[0][i], got[1][i], want)
		}
	}
}

// soloRing is a board holding a hash-sharded two-rank ring with the given
// reports, refitted on every sync.
func soloRing(t *testing.T, fixed bool, reps map[int]rankReport) *ddpBoard {
	t.Helper()
	old := rebalanceEvery
	rebalanceEvery = 1
	t.Cleanup(func() { rebalanceEvery = old })
	b := newDDPBoard()
	r := b.run("g")
	r.sharded, r.fixed, r.syncEvery = true, fixed, 1
	for rank, rep := range reps {
		b.observe("g", rank, rep)
	}
	return b
}

// The demo's GPU run: a 3060 doing its half in 20 ms and a Wi-Fi sync of
// 500 ms, against a slower card. One GPU alone wins by an order of magnitude.
var wifiSplit = map[int]rankReport{
	0: {StepMillis: 20, SyncMillis: 500, Samples: 32},
	1: {StepMillis: 45, SyncMillis: 480, Samples: 32},
}

// TestD2ReleasesTheSlowerRankWhenSoloIsFaster.
func TestD2ReleasesTheSlowerRankWhenSoloIsFaster(t *testing.T) {
	b := soloRing(t, false, wifiSplit)
	frag := b.rebalanceFor("g", 2)
	if !strings.Contains(frag, `"release": 0`) {
		t.Fatalf("no release of rank 1 to rank 0: %q", frag)
	}
	var note map[string]any
	if err := json.Unmarshal([]byte("{"+strings.TrimPrefix(frag, ",")+"}"), &note); err != nil {
		t.Fatalf("note %q is not JSON: %v", frag, err)
	}
	w, _ := note["weights"].([]any)
	if len(w) != 2 || w[0] != 1.0 || w[1] != 0.0 {
		t.Errorf("weights %v, want rank 0 alone", note["weights"])
	}
	if solo, _ := note["solo_ms"].(float64); solo < 30 || solo > 50 {
		t.Errorf("solo estimate %v ms, want ~40 (64 samples at rank 0's 1600/s)", note["solo_ms"])
	}
	if b.rebalanceFor("g", 2) != "" {
		t.Error("a released ring was told something again")
	}
}

// TestD2NeverShrinksAFixedRing: --ddp N is the user's choice.
func TestD2NeverShrinksAFixedRing(t *testing.T) {
	b := soloRing(t, true, wifiSplit)
	for i := 0; i < 3; i++ {
		if frag := b.rebalanceFor("g", 2); strings.Contains(frag, "release") {
			t.Fatalf("a --ddp ring was released: %q", frag)
		}
	}
}

// TestD2KeepsAWorthwhileRing: two equal GPUs on a fast link split well.
func TestD2KeepsAWorthwhileRing(t *testing.T) {
	b := soloRing(t, false, map[int]rankReport{
		0: {StepMillis: 100, SyncMillis: 10, Samples: 32},
		1: {StepMillis: 100, SyncMillis: 10, Samples: 32},
	})
	if frag := b.rebalanceFor("g", 2); strings.Contains(frag, "release") {
		t.Fatalf("a ring twice as fast as one rank was released: %q", frag)
	}
}

// TestD2LeavesHandSlicedDataAlone: a script that slices its own tensors has
// no shard the lead can hand to one rank, so the ring stays.
func TestD2LeavesHandSlicedDataAlone(t *testing.T) {
	b := soloRing(t, false, wifiSplit)
	b.runs["g"].sharded = false
	if frag := b.rebalanceFor("g", 2); strings.Contains(frag, "release") {
		t.Fatalf("released a ring whose data cannot move: %q", frag)
	}
}

// TestAutoNeverChangesTheSyncInterval: the automatic decision is one rank or
// the exact split; every reply keeps the interval the ranks asked for, the
// release round included.
func TestAutoNeverChangesTheSyncInterval(t *testing.T) {
	old := rebalanceEvery
	rebalanceEvery = 3
	defer func() { rebalanceEvery = old }()
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	released := false
	for seq := int64(1); seq <= 6; seq++ {
		var wg sync.WaitGroup
		for rank := 0; rank < 2; rank++ {
			wg.Add(1)
			go func(rank int) {
				defer wg.Done()
				rep := wifiSplit[rank]
				code, head, _ := ddpCorePost(t, ts, map[string]any{"group": "g", "seq": seq, "rank": rank, "world": 2,
					"kind": "grads", "dtype": "float32", "count": 1, "sync_every": 1, "sharded": true,
					"step_ms": rep.StepMillis, "sync_ms": rep.SyncMillis, "samples": rep.Samples},
					encodeF32([]float32{1}))
				if code != http.StatusOK {
					t.Errorf("seq %d rank %d: %d %v", seq, rank, code, head)
					return
				}
				if head["sync_every"] != 1.0 {
					t.Errorf("seq %d rank %d: sync_every %v, want 1", seq, rank, head["sync_every"])
				}
				if head["release"] != nil {
					s.ddp.mu.Lock()
					released = true
					s.ddp.mu.Unlock()
				}
			}(rank)
		}
		wg.Wait()
	}
	if !released {
		t.Error("the ring was never released, so the release round went untested")
	}
}

// TestAThirdRankIsCreditedOnlyItsShareOfTheSync is F40: each rank's gradient
// crosses the lead's link, so dropping one of three saves about half the
// measured sync, not all of it. Dropping it here adds 10 ms of compute and
// saves 7.5 ms of sync: keep it.
func TestAThirdRankIsCreditedOnlyItsShareOfTheSync(t *testing.T) {
	r := planRebalance(reports(
		[3]float64{100, 15, 100},
		[3]float64{100, 15, 100},
		[3]float64{100, 15, 20},
	), []float64{0.4, 0.4, 0.2}, 3)
	if r.Drop != -1 {
		t.Errorf("dropped rank %d: %s", r.Drop, r.Why)
	}
}

// TestInt8DownlinkErrorFeedback: a mean component worth 0.3 of a
// quantisation step rounds to zero in every reply. With the lead's residual
// carried forward it is delivered about every third round, so over many
// rounds the ranks receive what was averaged, not nothing.
func TestInt8DownlinkErrorFeedback(t *testing.T) {
	const scale = 1.0 / 127
	round := func(res []float64) float64 {
		a := newDDPAccumulator(ddpI8, 2)
		// Weighted 3:7, rank A's 1 and rank B's 0 average to 0.3 steps.
		if err := a.addSegmented([]byte{127, 1}, []float64{scale}, 3); err != nil {
			t.Fatal(err)
		}
		if err := a.addSegmented([]byte{127, 0}, []float64{scale}, 7); err != nil {
			t.Fatal(err)
		}
		out, scales, err := a.meanResidual(res)
		if err != nil {
			t.Fatal(err)
		}
		return float64(int8(out[1])) * scales[0]
	}
	var plain, fed float64
	res := make([]float64, 2)
	for i := 0; i < 100; i++ {
		plain += round(nil)
		fed += round(res)
	}
	want := 100 * 0.3 * scale
	if plain != 0 {
		t.Fatalf("without feedback the small component came through as %v; the test no longer tests anything", plain)
	}
	if math.Abs(fed-want) > scale {
		t.Errorf("with feedback the ranks received %v in total, want %v within one step", fed, want)
	}
}
