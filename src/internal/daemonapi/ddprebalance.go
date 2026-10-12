package daemonapi

import (
	"fmt"
	"sort"
)

// Mid-run rebalancing: shares chosen from what the ring is actually doing,
// rather than from a probe taken before it started.
//
// Placement measures a CPU loop for a quarter of a second on an idle machine.
// What a rank is worth for THIS job on THIS data, while everything else on
// the box is also running, is a different number - and it moves: a laptop
// throttles, another job lands, someone closes the lid. The ring already
// reports the two figures that settle it, because each rank needs them for
// its own sync tuning.

// rankReport is what one rank tells the daemon about how it is getting on.
type rankReport struct {
	// StepMillis is its mean compute time for one step.
	StepMillis float64
	// SyncMillis is its mean cost for one gradient exchange.
	SyncMillis float64
	// Samples is how many samples one of its steps covers.
	Samples int
}

// rate is samples per second of compute: what this rank is worth, in units
// that mean the same thing on a GPU and on a CPU because they come from the
// same model and the same data.
func (r rankReport) rate() float64 {
	if r.StepMillis <= 0 || r.Samples <= 0 {
		return 0
	}
	return float64(r.Samples) / (r.StepMillis / 1000)
}

// rebalance is a decision about the ring, taken from what it reported.
type rebalance struct {
	// Weights is each rank's new share, summing to 1. Empty when the shares
	// in use are already right.
	Weights []float64
	// Drop names a rank the ring is better off without, or -1.
	Drop int
	// Why is the reason, in the words the user needs to hear.
	Why string
}

// rebalanceMinShift is how far a share must move before it is worth
// re-issuing. Measurement wobbles by a few percent from one round to the
// next, and resharding on that would have the ranks re-slicing their data
// every epoch for nothing.
const rebalanceMinShift = 0.10

// planRebalance decides what to do with the ring, given what each rank
// reported and the shares it is using now.
//
// Returns a zero rebalance (Drop -1, no weights) when the ring is fine, which
// is the common case and must stay cheap.
func planRebalance(reports map[int]rankReport, current []float64, world int) rebalance {
	none := rebalance{Drop: -1}
	if world < 2 || len(reports) < world {
		// Never judge a ring on a subset: the fastest rank reports first, and
		// on its own it looks like a ring of one.
		return none
	}

	rates := make([]float64, world)
	var totalRate, syncSum float64
	var stepSamples float64
	syncN := 0
	for rank, rep := range reports {
		if rank < 0 || rank >= world {
			return none
		}
		r := rep.rate()
		if r <= 0 {
			// A rank that has not measured itself yet. Deciding without it
			// would reshare its work to everyone else on no evidence.
			return none
		}
		rates[rank] = r
		totalRate += r
		stepSamples += float64(rep.Samples)
		if rep.SyncMillis > 0 {
			syncSum += rep.SyncMillis
			syncN++
		}
	}
	if totalRate <= 0 {
		return none
	}

	want := make([]float64, world)
	for i, r := range rates {
		want[i] = r / totalRate
	}

	// Is any rank worth dropping? Every rank in the ring costs one gradient
	// exchange per step. A rank contributes its rate and costs that exchange,
	// so it earns its place only while the compute it adds outweighs the sync
	// it adds.
	if syncN > 0 {
		syncSec := (syncSum / float64(syncN)) / 1000
		if drop, why := worstFreeloader(rates, totalRate, stepSamples, syncSec, world); drop >= 0 {
			return rebalance{Weights: nil, Drop: drop, Why: why}
		}
	}

	if !shiftedEnough(current, want) {
		return none
	}
	return rebalance{
		Weights: want,
		Drop:    -1,
		Why: fmt.Sprintf("shares refitted to measured throughput: %s",
			sharePercents(want)),
	}
}

// worstFreeloader names the rank the ring is better off without, or -1.
//
// Dropping rank j leaves the others to absorb its work, so a step's compute
// rises from W/total to W/(total-rate_j) - and one exchange per step goes
// away. It is worth it when the exchange saved is larger than the compute
// added.
//
// Only the slowest rank is ever a candidate: if the slowest earns its place
// then so does everyone above it, and dropping a fast rank to save its sync
// is never the better trade at these ratios.
func worstFreeloader(rates []float64, totalRate, stepSamples, syncSec float64, world int) (int, string) {
	if world <= 2 {
		// A ring of two is a ring or it is nothing. Dropping to one rank is
		// not a rebalance, it is abandoning the run, and the caller asked for
		// distribution.
		return -1, ""
	}
	slowest, slowRate := -1, 0.0
	for i, r := range rates {
		if slowest == -1 || r < slowRate {
			slowest, slowRate = i, r
		}
	}
	// Rank 0 holds the blackboard every other rank posts to. Dropping it is
	// not something this can do from here.
	if slowest <= 0 {
		return -1, ""
	}
	remaining := totalRate - slowRate
	if remaining <= 0 {
		return -1, ""
	}
	// A step's work is the samples the ring covers in one, so work/totalRate
	// is the step time the ranks are actually reporting. Normalising the work
	// to 1 instead was the first attempt and made the comparison meaningless:
	// compute came out in tens of microseconds against a sync in tens of
	// milliseconds, so every ring looked worth cutting down to two ranks.
	if stepSamples <= 0 {
		return -1, ""
	}
	computeNow := stepSamples / totalRate
	computeAfter := stepSamples / remaining
	added := computeAfter - computeNow
	if added >= syncSec {
		return -1, ""
	}
	return slowest, fmt.Sprintf(
		"rank %d contributes %.0f%% of the ring's throughput but costs a "+
			"gradient exchange every step. Dropping it adds %.0f ms of compute "+
			"per step and saves %.0f ms of sync, so the ring is faster without it",
		slowest, 100*slowRate/totalRate, 1000*added, 1000*syncSec)
}

// shiftedEnough reports whether the new shares differ from the ones in use by
// enough to be worth re-slicing everyone's data for.
func shiftedEnough(current, want []float64) bool {
	if len(current) != len(want) {
		// No shares in use, or a different ring. Either way the new ones are
		// news.
		return true
	}
	for i := range want {
		if abs(want[i]-current[i]) >= rebalanceMinShift {
			return true
		}
	}
	return false
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// sharePercents renders shares for a person.
func sharePercents(w []float64) string {
	idx := make([]int, len(w))
	for i := range idx {
		idx[i] = i
	}
	sort.Ints(idx)
	out := ""
	for _, i := range idx {
		if out != "" {
			out += ", "
		}
		out += fmt.Sprintf("rank %d %.0f%%", i, 100*w[i])
	}
	return out
}
