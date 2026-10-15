package daemonapi

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type ddpEntry struct {
	blobs   map[int][]byte // rank -> opaque payload (blackboard mode)
	fetched map[int]bool   // ranks that have taken the result home
	done    chan struct{}  // closed when all world contributions are present
	world   int
	created time.Time

	// Reduce mode: contributions are folded into acc as they arrive and the
	// individual payloads are dropped, so the lead daemon holds one buffer
	// instead of one model per rank. result is computed once, when the last
	// rank arrives, and served to every waiter.
	acc          *ddpAccumulator
	result       []byte
	resultScale  float64
	resultScales []float64
	// kind is the first contribution's Kind; every rank in a round must send
	// the same one (a gradient averaged with a DiLoCo pseudo-gradient is
	// garbage that trains on silently).
	kind string
	// drift is how far the contributions disagreed (ddpAccumulator.drift),
	// set with result when hasDrift.
	drift    float64
	hasDrift bool
	// seen tracks which ranks have contributed, since acc keeps only a count
	// and a rank that retried must not be folded in twice.
	seen map[int]bool
	// stepMillis is each rank's mean compute time for a step, as reported by
	// the rank. The spread across ranks is what says whether the ring is
	// balanced - and with equal batches the slowest of them is the step time
	// for everybody.
	stepMillis map[int]float64

	// err is set when a contribution could not be folded in - a rank that
	// disagrees about the model's shape, say. Held rather than returned to
	// that one rank, because the others are blocked waiting for a result that
	// is never coming.
	err error
	// partialAt is when this round gives up waiting for everybody. A rank
	// that has died is indistinguishable from one that is merely slow, and
	// waiting three minutes to find out means one lost machine takes the
	// whole run with it.
	partialAt time.Time

	// syncEvery is the agreed averaging interval: the largest any rank
	// proposed. Largest rather than smallest because the proposal is a rank's
	// estimate of how much of its run sync is eating, and the rank suffering
	// most is the one setting the pace for everybody.
	syncEvery int

	// expected is how many contributions this round waits for: the world size
	// less any rank the ring has been told to continue without.
	expected int
	// partial is how many ranks actually contributed when a round completed
	// short-handed; zero when everybody arrived.
	partial int
	// holdExpired: this round was held for a join handover past
	// ddpHandoverHold, and said so.
	holdExpired bool
	// held: this round waited on ranks busy with a join handover, so the
	// ranks it closes without are known to be alive.
	held bool

	// fingerprint detects ranks doing identical work. Sharding only happens
	// automatically for DataLoader-based code; a script that slices its
	// tensors by hand - which both bundled training demos do - leaves every
	// rank computing the same batch. The gradients are then identical, the
	// average equals the single-process gradient exactly, and the run is
	// correct, slower, and silent about it. This is the only place holding
	// every rank's contribution, so it is the only place that can notice.
	fingerprint uint64
	sameWork    bool
	checked     bool
	// fingerprinted counts the contributions compared. One is not a
	// comparison: a round that closed on a single rank (the others not yet
	// started) matched only itself and reported duplicated work (V3 53).
	fingerprinted int

	// opening is the run's first round, the opening weight broadcast, which
	// is also the start barrier: it waits up to the start wait for every
	// rank, and a rank still missing when it closes is dropped.
	opening bool
	// manifest is the first opening contribution's parameter list; every
	// later rank's must match it (manifestRank names whose it is).
	manifest     *ddpManifest
	manifestRank int

	// shard is the butterfly shard this round averages (0: shard 0 or the
	// whole vector), so its reply's rounding error is kept apart from the
	// other shards' when one daemon serves two.
	shard int
}

type ddpBoard struct {
	mu      sync.Mutex
	entries map[string]*ddpEntry // key: group + "/" + seq
	// measured records which groups have already had their ring reported.
	// Keyed by group rather than by entry: an entry is one sync round, and a
	// flag there says it once per step, which at one sync per step is
	// thousands of identical lines. Measured 41 for a 90-step run before this
	// was keyed properly.
	measured map[string]time.Time // key: group -> when it was reported
	// runs holds what belongs to a training run rather than to one sync
	// round. An entry is one round, so anything kept there is forgotten
	// between steps - which is how the ring's measurement came to be logged
	// 41 times for a 90-step run.
	runs map[string]*ddpRun // key: group
	// local holds the machine-local rounds of ranks sharing this daemon's
	// machine (handleDDPLocal). Key: group + "/" + seq + "@local".
	local map[string]*ddpLocalRound
}

// expectedCount is how many ranks this round waits for. Falls back to the
// world size for entries created before any drop was known.
func (e *ddpEntry) expectedCount() int {
	if e.expected > 0 && e.expected <= e.world {
		return e.expected
	}
	return e.world
}

// dropCount is how many ranks a run has been told to continue without.
// Caller holds the board lock.
func (b *ddpBoard) dropCount(group string) int {
	r := b.runs[group]
	if r == nil {
		return 0
	}
	return len(r.dropped)
}

// dropsBelow counts the dropped ranks a round of the given world size would
// otherwise wait for. A rank that died before it joined has an index past the
// ring's old size and must not shrink the rounds it was never part of.
// Caller holds the board lock.
func (b *ddpBoard) dropsBelow(group string, world int) int {
	r := b.runs[group]
	if r == nil {
		return 0
	}
	n := 0
	for rank := range r.dropped {
		if rank < world {
			n++
		}
	}
	return n
}

// ddpRun is a training run's state across its sync rounds.
type ddpRun struct {
	// reports is what each rank last said about its own throughput, which is
	// what the shares are refitted from.
	reports map[int]rankReport
	// weights is the share set currently in force.
	weights []float64
	// round counts syncs, so the refit happens on a cadence rather than on
	// every step.
	round int
	// dropped names ranks the ring has been told to continue without.
	dropped map[int]bool
	// seen is when this run last synced, so it can be forgotten.
	seen time.Time

	// A change to the ring (a rank died, one is joining, the shares were
	// refitted): every rank must take it at the SAME epoch boundary, or two
	// ranks slice the data differently for an epoch and their step counts
	// part. So the notice rides the reply to one agreed sync (noteSeq), which
	// every live rank fetches. One note at a time, always carrying the whole
	// ring (shares and size), so a newer note can replace an older one.
	lastSeq  int64
	note     string       // reply fragment carrying the ring as it now is
	noteSeq  int64        // first sync whose reply carries it
	noteTold map[int]bool // ranks that already have it

	// world is the ring's size as this daemon knows it; it grows when a
	// rank joins mid-run (ddpup.go).
	world int
	// sharded says a rank reported training from a DataLoader the shim
	// reshards by hash, which is what lets a rank join mid-run.
	sharded bool
	// join is the rank joining now, if any.
	join *ddpJoin
	// loading is a joiner that has its snapshot and is loading it: rounds
	// still wait for it until its first sync (handoverHolds).
	loading *ddpJoin

	// aborted is why the ring cannot go on (ranks built different models,
	// every peer stopped answering). Set once; every later sync of the run
	// is refused with it, so no rank trains on alone after the ring died.
	aborted string
	// fixed: the user chose the ring (--ddp N, PIPEDPEER_DDP_FIXED=1), so
	// it is never shrunk on its own: no freeloader drop, no one-rank switch.
	fixed bool
	// released: the ring was told to finish on one rank (soloFor).
	released bool
	// syncEvery is the interval the ranks last proposed (1 = every step).
	syncEvery int
	// residual is the int8 reply's rounding error per kind and payload size,
	// carried into the next round's mean (downlink error feedback). Keyed by
	// kind too: a DiLoCo pseudo-gradient has the gradients' size, and a
	// gradient's rounding error added to a pseudo-gradient (or back) is off by
	// the learning rate times the interval.
	residual map[string][]float64
}

// run returns a group's run state, creating it. Caller holds the board lock.
func (b *ddpBoard) run(group string) *ddpRun {
	r := b.runs[group]
	if r == nil {
		// seen from birth: a run first created by a rank going down (before
		// any sync) would otherwise look ancient to sweep and lose its drops.
		r = &ddpRun{reports: map[int]rankReport{}, dropped: map[int]bool{}, noteTold: map[int]bool{}, seen: time.Now()}
		b.runs[group] = r
	}
	return r
}

// liveIn counts the ranks in m the ring has not dropped. A dropped rank's
// contribution stays in a round it reached before dying (reduce mode keeps
// only the sum, and its batch is real data nobody else trains this epoch),
// but it must not count toward closing the round: that would let a round
// close on the dead rank alone and hand the live ranks a result without
// their own gradients. Caller holds the board lock.
func liveIn[V any](b *ddpBoard, group string, m map[int]V) int {
	r := b.runs[group]
	n := 0
	for rank := range m {
		if r == nil || !r.dropped[rank] {
			n++
		}
	}
	return n
}

func newDDPBoard() *ddpBoard {
	return &ddpBoard{
		entries:  make(map[string]*ddpEntry),
		measured: make(map[string]time.Time),
		runs:     make(map[string]*ddpRun),
		local:    make(map[string]*ddpLocalRound),
	}
}

// observe records what a rank reported. Caller holds the board lock.
func (b *ddpBoard) observe(group string, rank int, rep rankReport) {
	r := b.run(group)
	if r.dropped[rank] {
		return // a dead rank's last report must not win it a share back
	}
	r.reports[rank] = rep
	r.seen = time.Now()
}

// sweep drops entries older than maxAge: a rank that died mid-run must not
// pin its group's blobs (a full model per rank) in memory forever.
func (b *ddpBoard) sweep(maxAge time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for k, e := range b.entries {
		if time.Since(e.created) > maxAge {
			delete(b.entries, k)
		}
	}
	// One key per training run, kept only to say the ring's measurement once.
	// A daemon that has served a thousand runs should not still be holding a
	// thousand group names.
	for group, when := range b.measured {
		if time.Since(when) > maxAge {
			delete(b.measured, group)
		}
	}
	for group, r := range b.runs {
		if time.Since(r.seen) > maxAge {
			delete(b.runs, group)
		}
	}
	for k, lr := range b.local {
		if time.Since(lr.created) > maxAge {
			delete(b.local, k)
		}
	}
}

// finishLocked computes a round's result from its accumulator and lets the
// sum go. An int8 round adds the previous reply's rounding error for a
// payload of the same size before quantising, and keeps the new one: the
// lead's half of R1's error feedback (each rank keeps its own uplink half).
// Every rank still receives identical bytes, so the models stay identical.
// Caller holds the board lock.
// The round's drift is taken from the sums before they go.
// ponytail: residuals keyed by kind and payload size; two optimizers whose
// gradients have the same element count share one. Key by optimizer if that
// bites.
func (b *ddpBoard) finishLocked(group string, e *ddpEntry) {
	e.drift, e.hasDrift = e.acc.drift()
	var res []float64
	if e.acc.dtype == ddpI8 {
		r := b.run(group)
		if r.residual == nil {
			r.residual = map[string][]float64{}
		}
		n := len(e.acc.sum)
		k := e.kind + "/" + strconv.Itoa(n)
		if e.shard > 0 {
			k += "#" + strconv.Itoa(e.shard) // equal shards on one daemon stay apart
		}
		if res = r.residual[k]; res == nil {
			res = make([]float64, n)
			r.residual[k] = res
		}
	}
	e.result, e.resultScales, e.err = e.acc.meanResidual(res)
	if len(e.resultScales) > 0 {
		e.resultScale = e.resultScales[0]
	}
	e.acc = nil
}

// abortLocked ends a run for good: every later sync is refused with why.
// Caller holds the board lock.
func (b *ddpBoard) abortLocked(group, why string) {
	r := b.run(group)
	if r.aborted != "" {
		return
	}
	r.aborted = why
	log.Warn().Str("group", group).Str("why", why).Msg("ddp ring aborted")
}

// ddpStartWait is how long the opening round waits for every rank. A rank's
// first sync comes once its closure is imported and its script has reached
// the first optimizer step, so on a cold node this covers the closure's
// transfer. Bounded because a rank still receiving a multi-GB closure over a
// relay took the friend's 2-laptop run to 792 s of training at 45 s a step
// (V3 53); the ring starts without it and it can join later.
var ddpStartWait = 5 * time.Minute

// ddpStartWaitMax caps what a rank may ask for.
const ddpStartWaitMax = time.Hour

// startWait is the opening round's wait, given a rank's request in seconds.
func startWait(secs float64) time.Duration {
	if secs <= 0 {
		return ddpStartWait
	}
	return min(time.Duration(secs*float64(time.Second)), ddpStartWaitMax)
}

// dropMissedStart drops every rank that never reached the opening round, so
// no later round waits for it and its data moves to the ranks that came.
// Caller holds the board lock.
func (b *ddpBoard) dropMissedStart(group string, e *ddpEntry) {
	r := b.run(group)
	var missed []string
	first := -1
	for rank := 0; rank < e.world; rank++ {
		if e.seen[rank] || r.dropped[rank] {
			continue
		}
		b.markRankDownLocked(group, rank, e.world, "not ready when training started")
		if first < 0 {
			first = rank
		}
		missed = append(missed, strconv.Itoa(rank))
	}
	if first < 0 {
		return
	}
	left := e.world - b.dropsBelow(group, e.world)
	why := fmt.Sprintf("rank %s not ready after %s (closure still on its way, or the "+
		"script not started); training started on the %d rank(s) that were, and their "+
		"data moves to them at the next epoch. A late rank can join once it is ready "+
		"if the script trains from a DataLoader",
		strings.Join(missed, ", "), time.Since(e.created).Round(time.Second), left)
	b.publish(r, r.lastSeq+1, ringFragment(r)+fmt.Sprintf(`, "down": %d, "why": %q`, first, why))
	log.Warn().Str("group", group).Strs("ranks", missed).Int("left", left).
		Msg("ddp start barrier: ranks not ready, the ring starts without them")
}
