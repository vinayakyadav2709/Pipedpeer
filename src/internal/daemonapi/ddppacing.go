package daemonapi

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

// pacedBy reports whether one rank is setting the pace for the whole ring, in
// the words the user needs to hear.
//
// With equal batches - which is what runs today - a step costs what the
// slowest rank costs, so a rank several times slower than the rest is not
// contributing a share of the work, it is deciding how long every step takes.
// Nothing said so before: the loss came out right and the run was simply
// slow, which is the failure mode this whole project exists to make visible.
//
// Silent until every rank has reported, because the fastest rank arriving
// first would otherwise look like a ring of one.
func pacedBy(steps map[int]float64, world int) (string, bool) {
	if world < 2 || len(steps) < world {
		return "", false
	}
	slowRank, slow := -1, 0.0
	fast := math.MaxFloat64
	var total float64
	for rank, ms := range steps {
		if ms > slow {
			slowRank, slow = rank, ms
		}
		if ms < fast {
			fast = ms
		}
		total += ms
	}
	if slow <= 0 || fast <= 0 || slow/fast < pacedRatio {
		return "", false
	}
	// What the ring costs per step now, against what it would cost without
	// the straggler. Equal batches: the step is the slowest rank, and dropping
	// it leaves the next slowest - approximated here by the mean of the rest,
	// which is enough to say whether the difference is worth acting on.
	rest := (total - slow) / float64(len(steps)-1)
	return fmt.Sprintf(
		"rank %d takes %.0f ms a step against %.0f ms for the fastest, and with "+
			"equal batches every rank waits for the slowest - so this one rank is "+
			"setting the pace for the whole ring. Without it a step would cost "+
			"about %.0f ms.", slowRank, slow, fast, rest), true
}

// pacedRatio is how much slower one rank must be before it is worth saying so.
// Ranks on identical hardware vary by a few percent from load alone; a factor
// of two is a different device, or a machine doing something else.
const pacedRatio = 2.0

// rankSamples renders how many samples each rank's step covered.
//
// Reported because its absence hid a real fault: the sample count arrived as
// zero for every rank, which made the weighted average fall back to treating
// them as equals and left the refit with no rates. Nothing failed - the
// number was simply never looked at.
func rankSamples(r *ddpRun) string {
	if r == nil || len(r.reports) == 0 {
		return "none reported"
	}
	ranks := make([]int, 0, len(r.reports))
	for rank := range r.reports {
		ranks = append(ranks, rank)
	}
	sort.Ints(ranks)
	parts := make([]string, 0, len(ranks))
	for _, rank := range ranks {
		parts = append(parts, fmt.Sprintf("rank %d %d", rank, r.reports[rank].Samples))
	}
	return strings.Join(parts, ", ")
}

// rankTimes renders the ring's measured step times in rank order.
func rankTimes(steps map[int]float64) string {
	ranks := make([]int, 0, len(steps))
	for r := range steps {
		ranks = append(ranks, r)
	}
	sort.Ints(ranks)
	parts := make([]string, 0, len(ranks))
	for _, r := range ranks {
		parts = append(parts, fmt.Sprintf("rank %d %.0f ms", r, steps[r]))
	}
	return strings.Join(parts, ", ")
}

// rebalanceEvery is how many syncs pass between refits.
//
// Often enough that a machine which slows down mid-run is noticed within
// seconds, rare enough that the ranks are not re-slicing their data
// constantly - a reshare costs an epoch boundary, which is the only place it
// can safely happen. A var for tests.
var rebalanceEvery = 20

// rebalanceFor decides whether to tell the ring something, and returns the
// JSON fragment to append to the reply (empty when there is nothing to say).
//
// Caller holds the board lock.
func (b *ddpBoard) rebalanceFor(group string, world int) string {
	r := b.runs[group]
	if r == nil {
		return ""
	}
	r.round++
	if r.released || rebalanceEvery <= 0 || r.round%rebalanceEvery != 0 {
		return ""
	}
	if frag := b.soloFor(group, r, world); frag != "" {
		return frag
	}

	plan := planRebalance(r.reports, r.weights, world)
	if plan.Drop >= 0 && r.fixed {
		return "" // --ddp N: the user chose the ring; never shrink it
	}
	if plan.Drop >= 0 && !r.dropped[plan.Drop] {
		r.dropped[plan.Drop] = true
		log.Warn().Str("group", group).Int("rank", plan.Drop).Msg(plan.Why)
		return fmt.Sprintf(`, "drop": %d, "why": %q`, plan.Drop, plan.Why)
	}
	if plan.Weights == nil {
		return ""
	}
	r.weights = plan.Weights
	log.Info().Str("group", group).Str("shares", sharePercents(plan.Weights)).
		Msg("ring reshared from measured throughput")
	parts := make([]string, len(plan.Weights))
	for i, w := range plan.Weights {
		parts[i] = strconv.FormatFloat(w, 'f', 6, 64)
	}
	return fmt.Sprintf(`, "weights": [%s], "why": %q`,
		strings.Join(parts, ","), plan.Why)
}
