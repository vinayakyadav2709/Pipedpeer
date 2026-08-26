package ddpplace

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// benchNode is a stand-in daemon that reports a fixed throughput.
func benchNode(t *testing.T, score float64) (host string, port int, close func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if score < 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"score": score, "cores": 8})
	}))
	h, p := splitHostPort(t, srv.URL)
	return h, p, srv.Close
}

func splitHostPort(t *testing.T, url string) (string, int) {
	t.Helper()
	trimmed := strings.TrimPrefix(url, "http://")
	host, portStr, ok := strings.Cut(trimmed, ":")
	if !ok {
		t.Fatalf("cannot parse %q", url)
	}
	var port int
	if _, err := fmtSscan(portStr, &port); err != nil {
		t.Fatalf("cannot parse port in %q: %v", url, err)
	}
	return host, port
}

func fmtSscan(s string, v *int) (int, error) {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errNotANumber
		}
		n = n*10 + int(r-'0')
	}
	*v = n
	return 1, nil
}

var errNotANumber = errString("not a number")

type errString string

func (e errString) Error() string { return string(e) }

func chosenIDs(p Plan) []string {
	var out []string
	for _, c := range p.Chosen {
		out = append(out, c.Candidate.NodeID)
	}
	return out
}

func has(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// TestMeasurementBeatsAdvertisedCores is the reason this package exists. Every
// worker in the local test cluster advertises the host's core count while
// being held to a fraction of it by a cgroup, so a placement that reads the
// advertisement puts an eighth of a machine on equal footing with a whole one.
func TestMeasurementBeatsAdvertisedCores(t *testing.T) {
	fastHost, fastPort, closeFast := benchNode(t, 8_000_000_000)
	defer closeFast()
	slowHost, slowPort, closeSlow := benchNode(t, 500_000_000)
	defer closeSlow()

	// Identical advertisements, 16x apart in reality.
	cands := []Candidate{
		{NodeID: "fast", Host: fastHost, Port: fastPort, Cores: 16, MemBytes: 1 << 40},
		{NodeID: "slow", Host: slowHost, Port: slowPort, Cores: 16, MemBytes: 1 << 40},
	}
	p := Select(context.Background(), cands, Options{Max: 0, ProbeMillis: 50})
	if !p.Measured {
		t.Fatal("plan reports itself unmeasured though both nodes answered")
	}
	var fastW, slowW float64
	for _, c := range p.Chosen {
		switch c.Candidate.NodeID {
		case "fast":
			fastW = c.Weight
		case "slow":
			slowW = c.Weight
		}
	}
	if fastW <= slowW {
		t.Errorf("fast weight %.3f <= slow weight %.3f; the advertised core counts "+
			"are identical, so only the measurement can tell these apart", fastW, slowW)
	}
}

// TestHopelessNodeIsRefused: in synchronous training every rank waits for the
// slowest, so a node far below the rest sets the pace rather than adding to
// it.
func TestHopelessNodeIsRefused(t *testing.T) {
	h1, p1, c1 := benchNode(t, 10_000_000_000)
	defer c1()
	h2, p2, c2 := benchNode(t, 9_000_000_000)
	defer c2()
	h3, p3, c3 := benchNode(t, 1_000) // seven orders of magnitude down
	defer c3()

	p := Select(context.Background(), []Candidate{
		{NodeID: "a", Host: h1, Port: p1, Cores: 16, MemBytes: 1 << 40},
		{NodeID: "b", Host: h2, Port: p2, Cores: 16, MemBytes: 1 << 40},
		{NodeID: "toaster", Host: h3, Port: p3, Cores: 16, MemBytes: 1 << 40},
	}, Options{Max: 0, ProbeMillis: 50})

	if has(chosenIDs(p), "toaster") {
		t.Error("a node seven orders of magnitude slower than the ring was admitted")
	}
	var explained bool
	for _, r := range p.Rejected {
		if r.Candidate.NodeID == "toaster" && r.Reason != "" {
			explained = true
		}
	}
	if !explained {
		t.Error("no reason recorded for dropping it; a silent drop is " +
			"indistinguishable from never having seen the node")
	}
}

// TestUnreachableNodeIsAssumedAverage guards a self-fulfilling exclusion. A
// node that misses one probe may train perfectly well; scoring it pessimistic
// keeps it out, so it is never measured, so it stays out.
func TestUnreachableNodeIsAssumedAverage(t *testing.T) {
	h1, p1, c1 := benchNode(t, 4_000_000_000)
	defer c1()
	h2, p2, c2 := benchNode(t, -1) // answers 500
	defer c2()

	p := Select(context.Background(), []Candidate{
		{NodeID: "ok", Host: h1, Port: p1, Cores: 16, MemBytes: 1 << 40},
		{NodeID: "quiet", Host: h2, Port: p2, Cores: 16, MemBytes: 1 << 40},
	}, Options{Max: 0, ProbeMillis: 50})

	if !has(chosenIDs(p), "quiet") {
		t.Errorf("a node that missed its probe was excluded; it will never get a "+
			"chance to be measured. chosen=%v rejected=%d", chosenIDs(p), len(p.Rejected))
	}
}

// TestNoMeasurementSaysSo: a plan built entirely from advertised core counts
// is a guess, and presenting it as a measurement is how unverified behaviour
// gets believed.
func TestNoMeasurementSaysSo(t *testing.T) {
	h, p, c := benchNode(t, -1)
	defer c()
	plan := Select(context.Background(), []Candidate{
		{NodeID: "a", Host: h, Port: p, Cores: 8, MemBytes: 1 << 40},
		{NodeID: "b", Host: h, Port: p, Cores: 2, MemBytes: 1 << 40},
	}, Options{Max: 0, ProbeMillis: 50})
	if plan.Measured {
		t.Error("plan claims to be measured though no node answered a probe")
	}
	if len(plan.Chosen) == 0 {
		t.Error("nothing was chosen; an unmeasured cluster should still train")
	}
}

// TestWeightsSumToOne. Batch sizes are derived from these, so weights that do
// not add up mean a step that is not the size anyone asked for.
func TestWeightsSumToOne(t *testing.T) {
	h1, p1, c1 := benchNode(t, 6_000_000_000)
	defer c1()
	h2, p2, c2 := benchNode(t, 3_000_000_000)
	defer c2()
	h3, p3, c3 := benchNode(t, 1_500_000_000)
	defer c3()

	for _, max := range []int{0, 1, 2, 3} {
		plan := Select(context.Background(), []Candidate{
			{NodeID: "a", Host: h1, Port: p1, Cores: 16, MemBytes: 1 << 40},
			{NodeID: "b", Host: h2, Port: p2, Cores: 16, MemBytes: 1 << 40},
			{NodeID: "c", Host: h3, Port: p3, Cores: 16, MemBytes: 1 << 40},
		}, Options{Max: max, ProbeMillis: 50})
		var sum float64
		for _, ch := range plan.Chosen {
			if ch.Weight <= 0 {
				t.Errorf("max=%d: %s has weight %v", max, ch.Candidate.NodeID, ch.Weight)
			}
			sum += ch.Weight
		}
		if len(plan.Chosen) == 0 {
			t.Errorf("max=%d: no ranks chosen", max)
			continue
		}
		if sum < 0.999 || sum > 1.001 {
			t.Errorf("max=%d: weights sum to %.4f, want 1", max, sum)
		}
	}
}

// TestRingCapIsExplained. "--ddp 2" on a four-node cluster drops two nodes,
// and the user asked for that - but the reason still has to be visible, or it
// reads the same as a node having been found unfit.
func TestRingCapIsExplained(t *testing.T) {
	h, p, c := benchNode(t, 5_000_000_000)
	defer c()
	plan := Select(context.Background(), []Candidate{
		{NodeID: "a", Host: h, Port: p, Cores: 16, MemBytes: 1 << 40},
		{NodeID: "b", Host: h, Port: p, Cores: 16, MemBytes: 1 << 40},
		{NodeID: "d", Host: h, Port: p, Cores: 16, MemBytes: 1 << 40},
	}, Options{Max: 2, ProbeMillis: 50})
	if len(plan.Chosen) != 2 {
		t.Fatalf("chose %d ranks, want 2", len(plan.Chosen))
	}
	var capped bool
	for _, r := range plan.Rejected {
		if strings.Contains(r.Reason, "capped") {
			capped = true
		}
	}
	if !capped {
		t.Error("the node dropped to honour --ddp is not distinguished from one " +
			"dropped for being unfit")
	}
}

// TestMemoryIsAHardGate. A node that cannot hold the model does not train
// slowly, it fails.
func TestMemoryIsAHardGate(t *testing.T) {
	h1, p1, c1 := benchNode(t, 4_000_000_000)
	defer c1()
	h2, p2, c2 := benchNode(t, 40_000_000_000) // much faster, far too small
	defer c2()

	plan := Select(context.Background(), []Candidate{
		{NodeID: "roomy", Host: h1, Port: p1, Cores: 16, MemBytes: 32 << 30},
		{NodeID: "tiny", Host: h2, Port: p2, Cores: 16, MemBytes: 1 << 30},
	}, Options{ProbeMillis: 50, WorkingSetBytes: 4 << 30})
	if has(chosenIDs(plan), "tiny") {
		t.Error("a node with 1 GiB was given a rank for a 4 GiB working set; " +
			"it would not train slowly, it would fail")
	}
	if !has(chosenIDs(plan), "roomy") {
		t.Error("the node that can hold the model was not used")
	}
}

// TestEqualBatchesRejectTheStragglers is the "only the ones that give gain"
// rule, on the cluster shape that motivated it.
//
// With equal batches every rank computes the same samples and waits for the
// slowest, so the ring's throughput is k*rate_k over the k fastest nodes.
// Measured scores on the local 16/8/2/1-core test cluster are roughly
// 14.9e9, 7.8e9, 2.0e9 and 1.2e9: two ranks give 15.7e9 and beat one rank's
// 14.9e9, while three give 6.1e9 and four give 4.7e9. Taking every node
// available would leave the run three times slower than using two of them.
func TestEqualBatchesRejectTheStragglers(t *testing.T) {
	scores := []float64{14.9e9, 7.8e9, 2.0e9, 1.2e9}
	names := []string{"host16", "cpu8", "cpu2", "cpu1"}
	var cands []Candidate
	for i, s := range scores {
		h, p, c := benchNode(t, s)
		defer c()
		cands = append(cands, Candidate{NodeID: names[i], Host: h, Port: p, Cores: 16, MemBytes: 1 << 40})
	}

	plan := Select(context.Background(), cands, Options{ProbeMillis: 50})
	got := chosenIDs(plan)
	if len(got) != 2 {
		t.Errorf("chose %d ranks (%v); two is fastest here and four is three times slower",
			len(got), got)
	}
	for _, want := range []string{"host16", "cpu8"} {
		if !has(got, want) {
			t.Errorf("%s should be in the ring; chosen=%v", want, got)
		}
	}
	for _, unwanted := range []string{"cpu2", "cpu1"} {
		if has(got, unwanted) {
			t.Errorf("%s was admitted; it would set the pace for every rank", unwanted)
		}
	}
	var explained int
	for _, r := range plan.Rejected {
		if strings.Contains(r.Reason, "pace") {
			explained++
		}
	}
	if explained != 2 {
		t.Errorf("%d of 2 stragglers explained; a silent drop reads the same as "+
			"never having seen the node", explained)
	}
}

// TestProportionalBatchesKeepSlowNodes records the other half of the rule.
// Once each rank's batch is sized by its speed, a slow rank takes a smaller
// batch and does help - so the equal-batch rejection must not outlive the
// constraint that justified it.
func TestProportionalBatchesKeepSlowNodes(t *testing.T) {
	scores := []float64{14.9e9, 7.8e9, 2.0e9, 1.2e9}
	names := []string{"host16", "cpu8", "cpu2", "cpu1"}
	var cands []Candidate
	for i, s := range scores {
		h, p, c := benchNode(t, s)
		defer c()
		cands = append(cands, Candidate{NodeID: names[i], Host: h, Port: p, Cores: 16, MemBytes: 1 << 40})
	}

	plan := Select(context.Background(), cands, Options{ProbeMillis: 50, ProportionalBatches: true})
	if len(plan.Chosen) != 4 {
		t.Errorf("chose %d of 4 ranks; with per-rank batch sizes every node earns its "+
			"share and none of them sets the pace: %v", len(plan.Chosen), chosenIDs(plan))
	}
	// And the shares must reflect the measurements, or "proportional" is a
	// word rather than a behaviour.
	var fast, slow float64
	for _, c := range plan.Chosen {
		switch c.Candidate.NodeID {
		case "host16":
			fast = c.Weight
		case "cpu1":
			slow = c.Weight
		}
	}
	if fast <= slow*4 {
		t.Errorf("fastest weight %.4f is not far above the slowest %.4f, though they "+
			"measure 12x apart", fast, slow)
	}
}
