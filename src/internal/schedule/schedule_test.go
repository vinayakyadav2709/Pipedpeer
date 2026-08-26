package schedule

import (
	"math"
	"testing"
)

func local(id string, rate float64) Device {
	return Device{ID: id, Node: "local", Kind: CPU, Rate: rate, MemBytes: 1 << 40}
}

func remote(id string, rate, bw, setup float64) Device {
	return Device{ID: id, Node: id, Kind: CPU, Rate: rate, BytesPerSec: bw,
		SetupSec: setup, MemBytes: 1 << 40}
}

func shareOf(p Plan, id string) int {
	for _, s := range p.Shares {
		if s.Device.ID == id {
			return s.Items
		}
	}
	return 0
}

// TestSharesFollowSpeedNotHeadcount is the bug this package exists for. An
// even split hands the same work to a 20-core box and a 4-core box, so the
// fast machine finishes and waits, and the job takes as long as the slowest
// participant. Shares have to be proportional to measured throughput.
func TestSharesFollowSpeedNotHeadcount(t *testing.T) {
	p := Compute(Options{Items: 1000}, []Device{
		local("fast", 100),
		remote("slow", 25, math.Inf(1), 0),
	})
	fast, slow := shareOf(p, "fast"), shareOf(p, "slow")
	if fast+slow != 1000 {
		t.Fatalf("shares total %d, want 1000", fast+slow)
	}
	ratio := float64(fast) / float64(slow)
	if ratio < 3.6 || ratio > 4.4 {
		t.Errorf("share ratio %.2f (fast=%d slow=%d), want about 4:1 — an even "+
			"split would be 1:1 and would leave the fast device idle", ratio, fast, slow)
	}
	tf := float64(fast) / 100
	ts := float64(slow) / 25
	if math.Abs(tf-ts) > 0.05*math.Max(tf, ts) {
		t.Errorf("finish times differ: fast %.2fs, slow %.2fs", tf, ts)
	}
}

// TestSlowLinkDeviceIsRefused covers the "only the ones that give gain" rule.
// A device that cannot start until after everyone else has finished adds
// nothing, and a scheduler that uses every device it can see would take it.
func TestSlowLinkDeviceIsRefused(t *testing.T) {
	p := Compute(Options{Items: 100}, []Device{
		local("here", 100),
		remote("far", 1000, math.Inf(1), 30),
	})
	if got := shareOf(p, "far"); got != 0 {
		t.Errorf("gave %d items to a device that needs 30s to start a 1s job", got)
	}
	if p.Makespan > 1.01 {
		t.Errorf("makespan %.2fs, want about 1s", p.Makespan)
	}
	if len(p.Rejected) == 0 {
		t.Error("device was dropped with no reason recorded; a silent drop is " +
			"indistinguishable from never having seen the device")
	}
}

// TestAddingDevicesNeverSlowsTheJobDown is the promise the whole design rests
// on: a user should be able to add machines without ever making things worse.
func TestAddingDevicesNeverSlowsTheJobDown(t *testing.T) {
	opts := Options{Items: 10000, BytesPerItem: 1024}
	base := []Device{local("here", 500)}
	alone := Compute(opts, base)

	candidates := []Device{
		remote("useful", 400, 100<<20, 0.2),
		remote("dialup", 800, 10<<10, 1),
		remote("distant", 5000, 1<<30, 90),
		remote("tiny", 3, 1<<30, 0.1),
	}
	for i := range candidates {
		with := Compute(opts, append(append([]Device{}, base...), candidates[:i+1]...))
		if with.Makespan > alone.Makespan*1.001 {
			t.Errorf("adding %d device(s) made the job slower: %.3fs alone, %.3fs with",
				i+1, alone.Makespan, with.Makespan)
		}
	}
}

// TestFastLinkBeatsFastDeviceWhenTheLinkIsTheLimit checks that transfer cost
// is folded into throughput rather than ignored. Shipping to a GPU across a
// slow link can cost more than the compute saves, which is precisely the case
// a "GPUs first" rule gets wrong.
func TestFastLinkBeatsFastDeviceWhenTheLinkIsTheLimit(t *testing.T) {
	opts := Options{Items: 1000, BytesPerItem: 1 << 20}
	p := Compute(opts, []Device{
		{ID: "gpu", Node: "far", Kind: GPU, Rate: 1000, BytesPerSec: 1 << 20, MemBytes: 1 << 40},
		local("cpu", 100),
	})
	if shareOf(p, "cpu") <= shareOf(p, "gpu") {
		t.Errorf("gave the wire-bound GPU %d items and the local CPU %d; the "+
			"GPU can only receive one item per second",
			shareOf(p, "gpu"), shareOf(p, "cpu"))
	}
}

// TestDeviceTooSmallForTheModelIsNotUsed: memory is a hard gate, not a
// preference. A GPU that cannot hold the working set does not do the work
// slowly, it fails.
func TestDeviceTooSmallForTheModelIsNotUsed(t *testing.T) {
	p := Compute(Options{Items: 500, WorkingSetBytes: 8 << 30}, []Device{
		local("big", 100),
		{ID: "small", Node: "n2", Kind: GPU, Rate: 10000, MemBytes: 6 << 30},
	})
	if got := shareOf(p, "small"); got != 0 {
		t.Errorf("gave %d items to a 6 GiB device for an 8 GiB working set", got)
	}
	var found bool
	for _, r := range p.Rejected {
		if r.Device.ID == "small" && r.Reason == "not enough memory for the working set" {
			found = true
		}
	}
	if !found {
		t.Error("no memory reason recorded for the rejected device")
	}
}

// TestCPUsAreUsedAlongsideGPUs is the hybrid case: the old rule picked GPU
// nodes and left every CPU in the cluster idle.
func TestCPUsAreUsedAlongsideGPUs(t *testing.T) {
	p := Compute(Options{Items: 10000}, []Device{
		{ID: "gpu0", Node: "yeet", Kind: GPU, Rate: 4000, MemBytes: 1 << 40},
		{ID: "cpu-yeet", Node: "yeet", Kind: CPU, Rate: 600, MemBytes: 1 << 40},
		{ID: "cpu-fedora", Node: "fedora", Kind: CPU, Rate: 800, BytesPerSec: 1 << 30, MemBytes: 1 << 40},
	})
	for _, id := range []string{"gpu0", "cpu-yeet", "cpu-fedora"} {
		if shareOf(p, id) <= 0 {
			t.Errorf("%s got no work; every one of these lowers the makespan", id)
		}
	}
	if shareOf(p, "gpu0") <= shareOf(p, "cpu-yeet") {
		t.Error("the GPU should carry more than the CPU pool beside it")
	}
}

// TestTinySharesAreGivenBack: a device owed three items is not worth a round
// trip, and the work it was holding has to go somewhere rather than vanish.
func TestTinySharesAreGivenBack(t *testing.T) {
	opts := Options{Items: 1000, MinItems: 50}
	p := Compute(opts, []Device{
		local("main", 1000),
		remote("sliver", 1, math.Inf(1), 0),
	})
	if got := shareOf(p, "sliver"); got != 0 {
		t.Errorf("sliver kept %d items, below the %d floor", got, opts.MinItems)
	}
	total := 0
	for _, s := range p.Shares {
		total += s.Items
	}
	if total != opts.Items {
		t.Fatalf("shares total %d after the drop, want %d — work was lost", total, opts.Items)
	}
}

// TestEveryItemIsAssigned guards the property whose absence is silent: shares
// that do not add up mean either lost work or work done twice.
func TestEveryItemIsAssigned(t *testing.T) {
	cases := []Options{
		{Items: 1}, {Items: 7}, {Items: 999}, {Items: 100000},
		{Items: 333, BytesPerItem: 4096}, {Items: 64, MinItems: 8},
	}
	devs := []Device{
		local("a", 137),
		remote("b", 61, 1<<25, 0.05),
		remote("c", 940, 1<<28, 0.3),
	}
	for _, opts := range cases {
		p := Compute(opts, devs)
		total := 0
		for _, s := range p.Shares {
			if s.Items < 0 {
				t.Fatalf("Items=%d: negative share %d", opts.Items, s.Items)
			}
			total += s.Items
		}
		if total != opts.Items {
			t.Errorf("Items=%d: shares total %d", opts.Items, total)
		}
	}
}

// TestNoDevicesIsNotACrash: a cluster can be empty, and callers rely on an
// empty plan rather than a panic to fall back to running locally.
func TestNoDevicesIsNotACrash(t *testing.T) {
	if p := Compute(Options{Items: 100}, nil); len(p.Shares) != 0 {
		t.Errorf("got %d shares from no devices", len(p.Shares))
	}
	p := Compute(Options{Items: 100}, []Device{{ID: "dead", Rate: 0}})
	if len(p.Shares) != 0 {
		t.Errorf("got shares from a device with no measured rate")
	}
	if len(p.Rejected) != 1 {
		t.Errorf("unmeasured device was dropped without a reason")
	}
}
