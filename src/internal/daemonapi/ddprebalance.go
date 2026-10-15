package daemonapi

import (
	"fmt"
	"math"
	"sort"

	"github.com/rs/zerolog/log"
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
// The exchange saved is the rank's marginal share of the sync, not all of
// it: every rank's gradient crosses the lead's link, so the measured sync
// carries N-1 ranks' traffic and one rank fewer saves about sync/(N-1).
// Crediting the whole sync to one rank dropped ranks that paid their way
// (F40).
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
	saved := syncSec / float64(world-1)
	if added >= saved {
		return -1, ""
	}
	return slowest, fmt.Sprintf(
		"rank %d contributes %.0f%% of the ring's throughput but costs a "+
			"gradient exchange every step. Dropping it adds %.0f ms of compute "+
			"per step and saves %.0f ms of sync, so the ring is faster without it",
		slowest, 100*slowRate/totalRate, 1000*added, 1000*saved)
}

// ddpSoloMargin is how much faster one rank alone must look before the ring
// is given up: step times wobble by a few percent, and a switch on noise is
// a switch the run cannot take back.
const ddpSoloMargin = 1.10

// planSolo is D2's comparison: the measured split step against the fastest
// rank doing the whole step alone. It returns that rank, or -1, with both
// times in milliseconds.
//
// split is what a step costs the ring now: compute plus this rank's share of
// one sync (sync/H at an interval of H). Each rank's own figure includes its
// wait for the slowest, so they agree; the smallest is taken, which leans
// towards keeping the ring. solo is the fastest rank's measured rate applied
// to the ring's whole step: linear in samples, which overstates a GPU's time
// for a bigger batch, so that too leans towards keeping the ring.
func planSolo(reports map[int]rankReport, live, syncEvery int) (int, float64, float64) {
	if live < 2 || len(reports) < live {
		return -1, 0, 0
	}
	h := float64(max(1, syncEvery))
	fast, fastRate := -1, 0.0
	var samples float64
	split := math.MaxFloat64
	for rank, rep := range reports {
		r := rep.rate()
		if r <= 0 || rep.SyncMillis <= 0 {
			return -1, 0, 0 // a rank not measured yet: no verdict
		}
		samples += float64(rep.Samples)
		if r > fastRate {
			fast, fastRate = rank, r
		}
		split = min(split, rep.StepMillis+rep.SyncMillis/h)
	}
	solo := 1000 * samples / fastRate
	if solo*ddpSoloMargin >= split {
		return -1, solo, split
	}
	return fast, solo, split
}

// dilocoRanks is --ddp-sync fast's interval by ring size (DILOCO-MANY,
// docs/benchmarks/diloco.md "Ranks"): the largest H that kept every test model
// within 1% of one device over 3 seeds, for rings of up to upTo ranks. Plain
// averaging drifts further from one device as ranks are added (each trains
// on a smaller slice of the batch), so bigger rings sync more often; past
// the last row nothing passed. The shim holds the same table
// (_DILOCO_RANKS in shim ddp.py; TestDiLoCoRanksMatchShim).
var dilocoRanks = []struct{ upTo, every int }{
	{2, 32},
	{3, 16},
	{4, 4},
}

// dilocoEvery is the validated interval for a ring of n ranks, or 0 when no
// interval passed at that size: fast sync is then never picked for it.
func dilocoEvery(n int) int {
	for _, r := range dilocoRanks {
		if n <= r.upTo {
			return r.every
		}
	}
	return 0
}

// planDiLoCo is U32's comparison: the ring's step under DiLoCo (each rank's
// compute plus one sync per dilocoEvery(live) steps; the smallest, as in
// planSolo) against planSolo's split and solo times. DiLoCo trains a slightly
// different model, so it must beat both by ddpSoloMargin, never tie. No
// verdict (false) until planSolo has one: every rank measured. Never for a
// ring size no interval passed at. solo is math.MaxFloat64 when one rank
// alone is not on offer (--ddp N).
func planDiLoCo(reports map[int]rankReport, live int, solo, split float64) (float64, bool) {
	every := dilocoEvery(live)
	if every == 0 || live < 2 || len(reports) < live || solo <= 0 || split <= 0 {
		return 0, false
	}
	fast := math.MaxFloat64
	for _, rep := range reports {
		fast = min(fast, rep.StepMillis+rep.SyncMillis/float64(every))
	}
	return fast, fast*ddpSoloMargin < min(split, solo)
}

// soloFor decides D2 for a ring the user did not size: when the fastest rank
// alone beats the split, every other rank is released at the next epoch
// boundary (they exit 77) and that rank takes the whole dataset, syncing
// nothing. Only for a hash-sharded DataLoader ring - the only kind whose data
// can move to one rank - and never under --ddp N, during a join, or twice.
// Returns the note fragment, or "". Caller holds the board lock.
//
// U32: when the ranks left the sync mode to us, DiLoCo is the third option,
// taken only when it beats both (planDiLoCo); the ring keeps every rank.
// Under --ddp N it need only beat the split: N fixes the ring's size, not its
// sync mode (--ddp-sync does that), and one rank alone is not on offer.
func (b *ddpBoard) soloFor(group string, r *ddpRun, world int) string {
	if r.released || r.join != nil || r.loading != nil {
		return ""
	}
	live := world - b.dropsBelow(group, world)
	keep, solo, split := planSolo(r.reports, live, r.syncEvery)
	if r.autoSync && !r.diloco {
		bar, alone := solo, fmt.Sprintf(" and %.0f ms on one rank alone", solo)
		if r.fixed {
			bar, alone = math.MaxFloat64, ""
		}
		if fast, ok := planDiLoCo(r.reports, live, bar, split); ok {
			r.diloco = true
			every := dilocoEvery(live)
			why := fmt.Sprintf("training with DiLoCo, sync every %d steps: about %.0f ms a step "+
				"against %.0f ms for the exact split%s "+
				"(--ddp-sync exact for per-step sync)", every, fast, split, alone)
			log.Info().Str("group", group).Float64("diloco_ms", fast).Float64("split_ms", split).
				Float64("solo_ms", solo).Bool("fixed", r.fixed).Msg("ddp: " + why)
			return fmt.Sprintf(`, "diloco": %d, "why": %q`, every, why)
		}
	}
	if r.fixed {
		return ""
	}
	if keep < 0 || !r.sharded {
		return ""
	}
	r.released = true
	r.weights = make([]float64, world)
	r.weights[keep] = 1
	log.Info().Str("group", group).Int("keep", keep).Float64("solo_ms", solo).
		Float64("split_ms", split).Msg("ddp: one rank alone is faster than the split; releasing the others")
	return ringFragment(r) + fmt.Sprintf(`, "release": %d, "solo_ms": %.0f, "split_ms": %.0f, "release_ranks": %d`,
		keep, solo, split, live)
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
