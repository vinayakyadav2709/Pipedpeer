package daemonapi

import (
	"math"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

// newTestPoolStats opens a store that no pending save outlives.
func newTestPoolStats(t *testing.T, path string) *poolStatsStore {
	t.Helper()
	poolStatsStores.Delete(path) // a "restart": read the file again
	st := openPoolStats(path)
	t.Cleanup(func() {
		st.mu.Lock()
		if st.timer != nil {
			st.timer.Stop()
			st.timer = nil
		}
		st.mu.Unlock()
		poolStatsStores.Delete(path)
	})
	return st
}

func fixedHW(m map[string]poolHW) func(string) (poolHW, bool) {
	return func(machine string) (poolHW, bool) {
		h, ok := m[machine]
		return h, ok
	}
}

func TestPoolStatsSecondRunStartsFromPersistedRate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "poolstats.json")
	hw := fixedHW(map[string]poolHW{"m1": {Model: "x", Cores: 4, MHz: 3000}})
	t0 := time.Unix(1_700_000_000, 0)

	st := newTestPoolStats(t, path)
	if _, _, ok := st.prior("m1", "fn", t0, hw); ok {
		t.Fatal("prior before any run")
	}
	for i := 0; i < 5; i++ {
		st.observe("m1", "fn", 100, 1.0, 1.1, t0.Add(time.Duration(i)*time.Second), hw)
	}
	if err := st.save(); err != nil {
		t.Fatal(err)
	}

	st2 := newTestPoolStats(t, path)
	r, o, ok := st2.prior("m1", "fn", t0.Add(time.Hour), hw)
	if !ok || math.Abs(r-100) > 1 {
		t.Fatalf("second run prior = %v, %v; want ~100 items/s", r, ok)
	}
	if math.Abs(o-0.1) > 0.01 {
		t.Fatalf("overhead prior = %v; want ~0.1 s", o)
	}
}

func TestPoolStatsHardwareChangeDropsEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "poolstats.json")
	hws := map[string]poolHW{"m1": {Model: "old", Cores: 4, MHz: 3000}}
	hw := fixedHW(hws)
	t0 := time.Unix(1_700_000_000, 0)
	st := newTestPoolStats(t, path)
	st.observe("m1", "fn", 100, 1.0, 1.0, t0, hw)
	if _, _, ok := st.prior("m1", "fn", t0, hw); !ok {
		t.Fatal("no prior after a part")
	}
	hws["m1"] = poolHW{Model: "old", Cores: 4, MHz: 3100} // clock jitter is not a change
	if _, _, ok := st.prior("m1", "fn", t0, hw); !ok {
		t.Fatal("clock jitter dropped the entry")
	}
	hws["m1"] = poolHW{Model: "new", Cores: 16, MHz: 4000}
	if r, _, ok := st.prior("m1", "fn", t0.Add(time.Hour), hw); ok {
		t.Fatalf("prior %v survived a hardware change", r)
	}
}

// A fake rig: the true rate plus 10% noise. Three warm parts must predict
// within 20%, from cold, from a prior 1.5x off (blended away) and from one
// 3x off (Dask's 2x reset).
func TestPoolStatsPredictionWithin20PercentAfter3Parts(t *testing.T) {
	hw := fixedHW(map[string]poolHW{"m1": {Model: "x", Cores: 4, MHz: 3000}})
	rng := rand.New(rand.NewSource(1))
	t0 := time.Unix(1_700_000_000, 0)
	const truth = 250.0
	for _, stale := range []float64{0, 1.5 * truth, truth / 1.5, 3 * truth, truth / 3} {
		st := newTestPoolStats(t, filepath.Join(t.TempDir(), "poolstats.json"))
		if stale > 0 {
			// Yesterday's run measured a different rate.
			st.observe("m1", "fn", int(stale), 1, 1, t0.Add(-24*time.Hour), hw)
		}
		for i := 0; i < 3; i++ {
			items := 500
			sec := float64(items) / truth * (1 + 0.1*(2*rng.Float64()-1))
			st.observe("m1", "fn", items, sec, sec+0.05, t0.Add(time.Duration(i)*time.Second), hw)
		}
		r, _, ok := st.prior("m1", "fn", t0.Add(3*time.Second), hw)
		if !ok || math.Abs(r-truth)/truth > 0.2 {
			t.Fatalf("stale=%v: predicted %v after 3 parts, truth %v", stale, r, truth)
		}
	}
}

func TestPoolStatsColdStartFromCoresTimesMHz(t *testing.T) {
	hw := fixedHW(map[string]poolHW{
		"fast": {Model: "a", Cores: 8, MHz: 4000},
		"slow": {Model: "b", Cores: 2, MHz: 2000},
	})
	t0 := time.Unix(1_700_000_000, 0)
	st := newTestPoolStats(t, filepath.Join(t.TempDir(), "poolstats.json"))
	st.observe("fast", "fn", 800, 1, 1, t0, hw)
	r, _, ok := st.prior("slow", "fn", t0, hw)
	if !ok || math.Abs(r-100) > 1 {
		t.Fatalf("cold prior = %v, %v; want 800 * (2*2000)/(8*4000) = 100", r, ok)
	}
	// slow turns out twice as fast as its clock says: the speed index learns
	// it and the next function's cold prior carries it.
	for i := 0; i < 20; i++ {
		st.observe("slow", "fn", 200, 1, 1, t0.Add(time.Duration(i)*time.Second), hw)
	}
	st.observe("fast", "fn2", 800, 1, 1, t0, hw)
	r2, _, _ := st.prior("slow", "fn2", t0, hw)
	if r2 < 150 {
		t.Fatalf("speed index not applied: fn2 prior %v, want near 200", r2)
	}
}

func TestPoolStatsBoundedLRU(t *testing.T) {
	hw := fixedHW(nil)
	t0 := time.Unix(1_700_000_000, 0)
	st := newTestPoolStats(t, filepath.Join(t.TempDir(), "poolstats.json"))
	for i := 0; i <= poolStatsMaxKeys; i++ {
		st.observe("m", string(rune('A'+i%26))+time.Duration(i).String(), 1, 1, 1, t0.Add(time.Duration(i)*time.Second), hw)
	}
	if len(st.entries) != poolStatsMaxKeys {
		t.Fatalf("%d entries, want %d", len(st.entries), poolStatsMaxKeys)
	}
	if _, ok := st.entries[poolStatKey("m", "A0s")]; ok {
		t.Fatal("oldest entry not evicted")
	}
}
