package daemonapi

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// TestDriftIsZeroWhenRanksAgree: identical contributions do not disagree, at
// any weighting.
func TestDriftIsZeroWhenRanksAgree(t *testing.T) {
	a := newDDPAccumulator(ddpF32, 3)
	for _, w := range []float64{1, 5} {
		if err := a.addFlat(f32Payload([]float64{1, -2, 3}), 0, w); err != nil {
			t.Fatal(err)
		}
	}
	d, ok := a.drift()
	if !ok || d > 1e-6 {
		t.Errorf("drift %v (ok %v), want 0", d, ok)
	}
}

// TestDriftMatchesAReference: sample-weighted spread over the weighted mean's
// norm, against the same sum written out.
func TestDriftMatchesAReference(t *testing.T) {
	xs := [][]float64{{1, 0, 2}, {0, 1, -1}, {3, 3, 0}}
	ws := []float64{2, 1, 5}
	a := newDDPAccumulator(ddpF64, 3)
	for i, x := range xs {
		p := make([]byte, 0, 24)
		for _, v := range x {
			p = appendF64(p, v)
		}
		if err := a.addFlat(p, 0, ws[i]); err != nil {
			t.Fatal(err)
		}
	}
	W := 8.0
	m := make([]float64, 3)
	for i, x := range xs {
		for j := range x {
			m[j] += ws[i] * x[j] / W
		}
	}
	spread, m2 := 0.0, 0.0
	for i, x := range xs {
		for j := range x {
			spread += ws[i] * (x[j] - m[j]) * (x[j] - m[j]) / W
		}
	}
	for _, v := range m {
		m2 += v * v
	}
	want := math.Sqrt(spread / m2)
	got, ok := a.drift()
	if !ok || math.Abs(got-want) > 1e-9 {
		t.Errorf("drift %v (ok %v), want %v", got, ok, want)
	}
}

// TestDriftNeedsTwoRanks: one contribution has nothing to disagree with.
func TestDriftNeedsTwoRanks(t *testing.T) {
	a := newDDPAccumulator(ddpF32, 1)
	if err := a.addFlat(f32Payload([]float64{1}), 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.drift(); ok {
		t.Error("a single contribution reported a drift")
	}
}

// TestDeltaRoundsReportDriftAndWeightBySamples: a DiLoCo round over the
// daemon. Rank 0 trained on 3 samples, rank 1 on 1; the mean is weighted 3:1
// and the reply says how far the two disagreed.
func TestDeltaRoundsReportDriftAndWeightBySamples(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	vals := [][]float32{{1, 0}, {0, 1}}
	samples := []int{3, 1}
	heads := make([]map[string]any, 2)
	blobs := make([][]float32, 2)
	var wg sync.WaitGroup
	for rank := range vals {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			code, head, blob := ddpCorePost(t, ts, map[string]any{"group": "g", "seq": 7, "rank": rank, "world": 2,
				"kind": "delta", "dtype": "float32", "count": 2, "samples": samples[rank]}, encodeF32(vals[rank]))
			if code != http.StatusOK {
				t.Errorf("rank %d: %d %v", rank, code, head)
				return
			}
			heads[rank], blobs[rank] = head, decodeF32(blob)
		}(rank)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	// m = (0.75, 0.25); spread = (3*(0.0625+0.0625) + 1*(0.5625+0.5625))/4 = 0.375.
	want := math.Sqrt(0.375 / (0.75*0.75 + 0.25*0.25))
	for rank := range heads {
		if blobs[rank][0] != 0.75 || blobs[rank][1] != 0.25 {
			t.Errorf("rank %d got mean %v, want [0.75 0.25]", rank, blobs[rank])
		}
		d, ok := heads[rank]["drift"].(float64)
		if !ok || math.Abs(d-want) > 1e-4 {
			t.Errorf("rank %d: drift %v, want %v", rank, heads[rank]["drift"], want)
		}
	}
}

// TestMixedKindsAbortTheRound: a rank sending a gradient into a round of
// pseudo-gradients means the schedules diverged; both ranks stop instead of
// training on the average of the two.
func TestMixedKindsAbortTheRound(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	kinds := []string{"delta", "grads"}
	codes := make([]int, 2)
	heads := make([]map[string]any, 2)
	var wg sync.WaitGroup
	for rank := range kinds {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			codes[rank], heads[rank], _ = ddpCorePost(t, ts, map[string]any{"group": "g", "seq": 4, "rank": rank,
				"world": 2, "kind": kinds[rank], "dtype": "float32", "count": 1}, encodeF32([]float32{1}))
		}(rank)
	}
	wg.Wait()
	for rank := range codes {
		msg, _ := heads[rank]["error"].(string)
		if codes[rank] != http.StatusConflict || !strings.Contains(msg, "sync schedules disagree") {
			t.Errorf("rank %d: %d %v, want 409 naming the disagreement", rank, codes[rank], heads[rank])
		}
	}
}

// TestDeltaResidualKeptApartFromGradients: a gradient round's int8 rounding
// error is never added to a pseudo-gradient of the same size. An all-zero
// delta round must come back as zeros after a gradient round left a residual.
func TestDeltaResidualKeptApartFromGradients(t *testing.T) {
	b := newDDPBoard()
	round := func(kind string, payload []byte) []byte {
		e := &ddpEntry{kind: kind, acc: newDDPAccumulator(ddpI8, 2)}
		for i := 0; i < 2; i++ {
			if err := e.acc.addSegmented(payload, []float64{1.0 / 127}, 1); err != nil {
				t.Fatal(err)
			}
		}
		b.mu.Lock()
		b.finishLocked("g", e)
		b.mu.Unlock()
		if e.err != nil {
			t.Fatal(e.err)
		}
		return e.result
	}
	// Weighted 1:2, the second element averages to a third of a step, which
	// rounds to zero and stays in the residual.
	e := &ddpEntry{kind: "grads", acc: newDDPAccumulator(ddpI8, 2)}
	_ = e.acc.addSegmented([]byte{127, 1}, []float64{1.0 / 127}, 1)
	_ = e.acc.addSegmented([]byte{127, 0}, []float64{1.0 / 127}, 2)
	b.mu.Lock()
	b.finishLocked("g", e)
	res := b.run("g").residual["grads/2"]
	b.mu.Unlock()
	if res == nil || res[1] == 0 {
		t.Fatalf("the gradient round left no residual (%v); the test tests nothing", res)
	}
	for i, v := range round("delta", []byte{0, 0}) {
		if v != 0 {
			t.Errorf("delta element %d came back as %d: the gradients' residual leaked in", i, int8(v))
		}
	}
}

func appendF64(b []byte, v float64) []byte {
	u := math.Float64bits(v)
	for i := 0; i < 8; i++ {
		b = append(b, byte(u>>(8*i)))
	}
	return b
}
