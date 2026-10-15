package daemonapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
)

// DDP sync over the daemon's own channel. Ranks never open ports of their
// own: every rank POSTs its blob for (group, seq) to the lead rank's daemon
// on the one port that already works between every pair of nodes, and the
// call returns once all world blobs are present. Averaging happens in the
// shim — the daemon is a blackboard, not a math engine.
//
// This replaces torch.distributed's socket mesh, which needs direct
// rank-to-rank TCP on its own ports — a requirement that holds on a
// datacenter LAN and on nothing pipedpeer actually targets.

// DDPFramesContentType marks the binary form of a sync: a JSON header line
// followed by one length-prefixed payload frame. Base64 inflates a gradient
// by a third both on the wire and in this daemon's memory, and it buys
// nothing here — the daemon never looks inside the payload.
const DDPFramesContentType = "application/vnd.pipedpeer.ddp"

// DDPReduceContentType marks a sync the daemon averages itself. The reply is
// one model rather than one per rank, which is the whole point: on two
// machines the old reply was already twice the model per step, and it grows
// with the ring. See ddpreduce.go for why that mattered enough to give up the
// daemon's ignorance of the payload.
const DDPReduceContentType = "application/vnd.pipedpeer.ddp.reduce"

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

type ddpSyncRequest struct {
	Group string `json:"group"`
	Seq   int64  `json:"seq"`
	Rank  int    `json:"rank"`
	World int    `json:"world"`
	Data  string `json:"data"` // b64, opaque to the daemon

	// Reduce mode only.
	DType ddpDType `json:"dtype,omitempty"`
	Count int      `json:"count,omitempty"`
	// Scale is the int8 quantisation step this rank used. Each rank picks its
	// own from its own values, so it travels with the payload.
	Scale float64 `json:"scale,omitempty"`
	// Scales and Counts give each tensor its own quantisation step. One scale
	// for a whole model lets the largest layer set the step size for every
	// other, which costs most of the precision - see ddpreduce.go.
	Scales []float64 `json:"scales,omitempty"`
	Counts []int     `json:"counts,omitempty"`
	// Kind is "grads" or "weights". Only gradients are checked for ranks
	// duplicating work: the run opens by broadcasting the initial weights,
	// and those are identical by construction whenever the script seeds its
	// generator - which every sane training script does. Fingerprinting that
	// sync reported duplicated work on a correctly sharded run.
	Kind string `json:"kind,omitempty"`
	// SyncEvery is this rank's proposal for how often to average. Ranks
	// measure their own link and compute independently and so arrive at
	// different numbers; averaging at different points in each rank's step
	// sequence is not local SGD, it is nonsense, and it showed up as a
	// measurably worse final loss. The daemon hands everyone one answer.
	SyncEvery int `json:"sync_every,omitempty"`
	// StepMillis is this rank's mean compute time for one step, which every
	// rank already measures for its own sync tuning and which nothing has
	// ever collected.
	//
	// Gathered here it becomes the one measurement the scheduler has never
	// had: what a device is worth for this job, in units that compare across
	// devices. The throughput probe used for placement is an integer loop on
	// the CPU, so a GPU node's score is its CPU's score and a GPU cannot be
	// ranked against a CPU at all. This is the same model, the same batch,
	// on whatever hardware the rank actually has - comparable by
	// construction, and free, because the work is being done anyway.
	StepMillis float64 `json:"step_ms,omitempty"`
	// Samples is how many samples produced this gradient.
	//
	// With equal batches it is the same on every rank and changes nothing.
	// With proportional batches it is the whole correctness argument: a rank
	// training on twice the data computed its mean gradient over twice the
	// samples, and averaging the per-rank means as equals would give the
	// smaller shard twice the influence per sample it earned. The combined
	// gradient is sum(n_i * g_i) / sum(n_i).
	//
	// Absent (0) from a rank running the equal-batch code, which the
	// accumulator reads as a weight of 1 - reproducing the plain average
	// exactly, so a mixed-version ring is wrong in no new way.
	Samples int `json:"samples,omitempty"`
	// SyncMillis is what one gradient exchange has cost this rank on average.
	// The daemon needs it to decide whether a rank pays for itself: every
	// rank costs an exchange per step, and one contributing less compute than
	// it adds in sync is making the run slower.
	SyncMillis float64 `json:"sync_ms,omitempty"`
	// Sharded says this rank trains from a DataLoader the shim reshards by
	// hash. Only such a ring can take a new rank mid-run.
	Sharded bool `json:"sharded,omitempty"`
	// StartWait is how long, in seconds, the opening round waits for every
	// rank before the ring starts without the missing ones
	// (PIPEDPEER_DDP_START_WAIT). Zero: ddpStartWait.
	StartWait float64 `json:"start_wait,omitempty"`
	// Manifest describes the parameters behind the opening round, so a ring
	// whose ranks built different models stops with both described instead
	// of a byte-count error from the accumulator.
	Manifest *ddpManifest `json:"manifest,omitempty"`
	// Fixed: the user chose this ring (--ddp N); never shrink it.
	Fixed bool `json:"fixed,omitempty"`

	// Shard/Shards: one shard of a butterfly reduce (U26). Rank k's daemon
	// averages shard k of every rank's vector, so no one link carries the
	// whole ring (N-1 gradients in and out at the hub's lead). Shard 0 goes to
	// the lead with the full header and carries the ring's control; shards
	// above 0 are bare sums under their own key. Absent: the whole vector.
	Shard  int `json:"shard,omitempty"`
	Shards int `json:"shards,omitempty"`
	// Local/Upstream: ranks sharing this daemon's machine (multi-GPU) average
	// here first, and this daemon sends the lead at Upstream one contribution
	// standing for all of them: one network sync per machine.
	Local    []int  `json:"local,omitempty"`
	Upstream string `json:"upstream,omitempty"`
	// Members: the ranks a forwarded contribution stands for, each with its
	// own report, so the lead counts, measures and tells every one of them.
	Members []ddpMember `json:"members,omitempty"`
}

// ddpMember is one rank inside a machine's combined contribution.
type ddpMember struct {
	Rank       int     `json:"rank"`
	StepMillis float64 `json:"step_ms,omitempty"`
	SyncMillis float64 `json:"sync_ms,omitempty"`
	Samples    int     `json:"samples,omitempty"`
}

// ddpManifest is one rank's view of the model it syncs.
type ddpManifest struct {
	Names  []string `json:"names,omitempty"`
	Shapes [][]int  `json:"shapes"`
	DTypes []string `json:"dtypes"`
	Device string   `json:"device,omitempty"`
	Host   string   `json:"host,omitempty"`
}

// values is how many numbers the manifest's tensors hold.
func (m *ddpManifest) values() int {
	n := 0
	for _, sh := range m.Shapes {
		v := 1
		for _, d := range sh {
			v *= d
		}
		n += v
	}
	return n
}

// who names a rank for a person: "rank 1 on gpu1 (cuda)".
func (m *ddpManifest) who(rank int) string {
	s := fmt.Sprintf("rank %d", rank)
	if m.Host != "" {
		s += " on " + m.Host
	}
	if m.Device != "" {
		s += " (" + m.Device + ")"
	}
	return s
}

// manifestMismatch returns why two ranks' models cannot be averaged, or "".
func manifestMismatch(a *ddpManifest, ra int, b *ddpManifest, rb int) string {
	diff := ""
	if len(a.Shapes) != len(b.Shapes) {
		diff = fmt.Sprintf("%d tensors against %d", len(a.Shapes), len(b.Shapes))
	} else {
		for i := range a.Shapes {
			sameShape := fmt.Sprint(a.Shapes[i]) == fmt.Sprint(b.Shapes[i])
			sameType := i >= len(a.DTypes) || i >= len(b.DTypes) || a.DTypes[i] == b.DTypes[i]
			if sameShape && sameType {
				continue
			}
			name := fmt.Sprintf("tensor %d", i)
			if i < len(a.Names) && a.Names[i] != "" {
				name = a.Names[i]
			}
			da, db := "", ""
			if i < len(a.DTypes) && i < len(b.DTypes) {
				da, db = " "+a.DTypes[i], " "+b.DTypes[i]
			}
			diff = fmt.Sprintf("%s is %v%s against %v%s", name, a.Shapes[i], da, b.Shapes[i], db)
			break
		}
	}
	if diff == "" {
		return ""
	}
	return fmt.Sprintf("the ranks built different models: %s has %d tensors (%d values), "+
		"%s has %d tensors (%d values); first difference: %s. Every rank must build the "+
		"same model - a script that sizes its model by device builds a different one on "+
		"a CPU than on a GPU",
		a.who(ra), len(a.Shapes), a.values(), b.who(rb), len(b.Shapes), b.values(), diff)
}

// ddpMode is how a request wants its payload handled.
type ddpMode int

const (
	ddpModeJSON   ddpMode = iota // base64 envelope, blackboard
	ddpModeFrames                // binary frames, blackboard
	ddpModeReduce                // binary frames, averaged here
)

// readDDPRequest accepts all three forms.
func readDDPRequest(r *http.Request) (ddpSyncRequest, []byte, ddpMode, error) {
	ct := r.Header.Get("Content-Type")
	if ct != DDPFramesContentType && ct != DDPReduceContentType {
		var req ddpSyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return req, nil, ddpModeJSON, err
		}
		// Deliberately not decoded. In blackboard mode whatever a rank
		// deposits is what its peers take home, and validating the encoding
		// would reject payloads the daemon has no business reading.
		return req, []byte(req.Data), ddpModeJSON, nil
	}
	mode := ddpModeFrames
	if ct == DDPReduceContentType {
		mode = ddpModeReduce
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return ddpSyncRequest{}, nil, mode, err
	}
	nl := bytes.IndexByte(body, '\n')
	if nl < 0 {
		return ddpSyncRequest{}, nil, mode, fmt.Errorf("ddp frames body has no header line")
	}
	var req ddpSyncRequest
	if err := json.Unmarshal(body[:nl], &req); err != nil {
		return req, nil, mode, err
	}
	blob, _, err := readFrame(body[nl+1:])
	return req, blob, mode, err
}

// handleDDPSync is one round trip per rank per sync: deposit our blob, block
// until the set is complete, take every rank's blob home.
func (s *Server) handleDDPSync(w http.ResponseWriter, r *http.Request) {
	req, blob, mode, err := readDDPRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Group == "" || req.World < 1 || req.Rank < 0 || req.Rank >= req.World {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "group, world and rank in [0,world) required"})
		return
	}

	if req.Shards > 0 && (mode != ddpModeReduce || req.Shard < 0 || req.Shard >= req.Shards ||
		req.Shards > req.World) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a shard needs reduce mode and shard in [0,shards), shards <= world"})
		return
	}
	if len(req.Local) > 0 {
		s.handleDDPLocal(w, r, req, blob, mode)
		return
	}
	// The ranks this contribution stands for: the sender, or every rank of a
	// machine whose daemon averaged them first.
	contributors := []int{req.Rank}
	reports := []ddpMember{{Rank: req.Rank, StepMillis: req.StepMillis, SyncMillis: req.SyncMillis, Samples: req.Samples}}
	if len(req.Members) > 0 {
		contributors, reports = nil, req.Members
		for _, m := range req.Members {
			if m.Rank < 0 || m.Rank >= req.World {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "member rank outside [0,world)"})
				return
			}
			contributors = append(contributors, m.Rank)
		}
	}

	key := req.Group + "/" + jsonInt(req.Seq)
	if req.Shard > 0 {
		key += "#" + strconv.Itoa(req.Shard)
	}
	// Whether this request carries the ring's control (shares, drops, notes,
	// step times): shard 0 or the whole vector. A butterfly shard above 0 is
	// a bare sum, wherever it lands.
	lead := req.Shard == 0

	opening := lead && req.Kind == "weights" && req.Seq == 1

	s.ddp.mu.Lock()
	if r := s.ddp.runs[req.Group]; r != nil && r.aborted != "" {
		why := r.aborted
		s.ddp.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]any{"abort": true, "error": why})
		return
	}
	if r := s.ddp.runs[req.Group]; opening && r != nil && r.dropped[req.Rank] {
		// Missed the start barrier: the ring is training without this rank,
		// and its model is the untrained one. The shim exits on this (status
		// ddpMissedStartExit) so the submitter can start it again as a joiner.
		s.ddp.mu.Unlock()
		writeJSON(w, http.StatusGone, map[string]any{"missed_start": true,
			"error": "this rank was not ready when the ring started; the ring went on without it"})
		return
	}
	e, ok := s.ddp.entries[key]
	if !ok {
		wait := ddpPartialAfter
		if opening {
			wait = startWait(req.StartWait)
		}
		e = &ddpEntry{
			blobs:     make(map[int][]byte),
			fetched:   make(map[int]bool),
			seen:      make(map[int]bool),
			done:      make(chan struct{}),
			world:     req.World,
			created:   time.Now(),
			partialAt: time.Now().Add(wait),
			opening:   opening,
			kind:      req.Kind,
			shard:     req.Shard,
		}
		if mode == ddpModeReduce {
			if req.Count <= 0 || req.DType.size() == 0 {
				s.ddp.mu.Unlock()
				writeJSON(w, http.StatusBadRequest,
					map[string]string{"error": "reduce mode needs a positive count and a known dtype"})
				return
			}
			e.acc = newDDPAccumulator(req.DType, req.Count)
			if len(req.Counts) > 1 {
				if err := e.acc.withSegments(req.Counts); err != nil {
					s.ddp.mu.Unlock()
					writeJSON(w, http.StatusBadRequest,
						map[string]string{"error": "segment lengths: " + err.Error()})
					return
				}
			}
		}
		s.ddp.entries[key] = e
	}
	if e.world != req.World {
		s.ddp.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "world size mismatch within group"})
		return
	}
	// A rank the ring has been told to continue without is not a rank that is
	// merely late. Counting it as expected would have every remaining step
	// wait out the full partial timeout - 45 seconds each - for a gradient
	// that is never coming, which would make dropping a freeloader far worse
	// than keeping it.
	e.expected = e.world - s.ddp.dropsBelow(req.Group, e.world)
	if e.expected < 1 {
		e.expected = 1
	}
	if lead {
		run := s.ddp.run(req.Group)
		if req.World > run.world {
			run.world = req.World
		}
		if req.Sharded {
			run.sharded = true
		}
		if req.Fixed {
			run.fixed = true
		}
		if req.SyncEvery > 0 {
			run.syncEvery = req.SyncEvery
		}
		if run.loading != nil && run.loading.rank == req.Rank {
			run.loading = nil // the joiner loaded the model and is syncing
		}
		if req.Seq > run.lastSeq {
			run.lastSeq = req.Seq
			run.seen = time.Now()
			// Decided once per sync and told to every rank at this one: a
			// refit decided per reply reached only the rank whose reply hit
			// the cadence, and that rank alone resliced its data.
			// Not while a rank is joining: a refit note would replace the
			// join note before every rank had it.
			if frag := s.ddp.rebalanceFor(req.Group, run.world); frag != "" && run.join == nil {
				s.ddp.publish(run, req.Seq, frag)
			}
		}
	}
	if req.SyncEvery > e.syncEvery {
		e.syncEvery = req.SyncEvery
	}
	for _, rep := range reports {
		if !lead || rep.StepMillis <= 0 {
			continue
		}
		if e.stepMillis == nil {
			e.stepMillis = map[int]float64{}
		}
		e.stepMillis[rep.Rank] = rep.StepMillis
		s.ddp.observe(req.Group, rep.Rank, rankReport{
			StepMillis: rep.StepMillis,
			SyncMillis: rep.SyncMillis,
			Samples:    rep.Samples,
		})
		if _, said := s.ddp.measured[req.Group]; !said && len(e.stepMillis) >= e.world {
			s.ddp.measured[req.Group] = time.Now()
			// Said once, whatever the verdict. What each device is worth for
			// this job is the measurement the scheduler has never had, and
			// printing it only when something is wrong would leave a balanced
			// ring looking like one nobody measured.
			log.Info().Str("group", req.Group).Str("ranks", rankTimes(e.stepMillis)).
				Str("samples", rankSamples(s.ddp.runs[req.Group])).
				Msg("ring measured: time for one step, on each rank's own hardware")
			if msg, ok := pacedBy(e.stepMillis, e.world); ok {
				log.Warn().Str("group", req.Group).Msg(msg)
			}
		}
	}
	var complete bool
	if e.result != nil || e.err != nil {
		// The round finished without us - either everyone else arrived and
		// this rank was slow, or it completed short-handed. Either way the
		// answer exists and is the same one every other rank applied, so
		// taking it home keeps this rank in step rather than diverging.
		complete = true
	} else if e.acc != nil {
		// Folded in on arrival and the payload dropped: holding every rank's
		// model until the last one shows up is what made the lead daemon's
		// memory scale with the ring.
		if req.Manifest != nil && e.opening && !e.seen[req.Rank] {
			if e.manifest == nil {
				e.manifest, e.manifestRank = req.Manifest, req.Rank
			} else if why := manifestMismatch(e.manifest, e.manifestRank, req.Manifest, req.Rank); why != "" {
				e.err = fmt.Errorf("%s", why)
			}
		}
		fresh := !slices.ContainsFunc(contributors, func(c int) bool { return e.seen[c] })
		if req.Kind != "" && e.kind != "" && req.Kind != e.kind && fresh && e.err == nil {
			e.err = fmt.Errorf("rank %d sent %s to sync %d while the others sent %s: the "+
				"ranks' sync schedules disagree, and averaging the two would train on "+
				"garbage", req.Rank, req.Kind, req.Seq, e.kind)
		}
		if fresh && e.err == nil {
			// Only worth doing early: ranks that duplicate work do it from
			// the first step, and hashing every payload forever would put a
			// pass over the model into every sync.
			if req.Kind == "grads" && req.Seq <= ddpFingerprintSeqs {
				h := fnv1a(blob)
				e.fingerprinted++
				if len(e.seen) == 0 {
					e.fingerprint, e.sameWork, e.checked = h, true, true
				} else if h != e.fingerprint {
					e.sameWork = false
				}
			}
			scales := req.Scales
			if len(scales) == 0 {
				scales = []float64{req.Scale}
			}
			if addErr := e.acc.addSegmented(blob, scales, float64(req.Samples)); addErr != nil {
				// Recorded rather than returned to this rank alone: the
				// others are blocked on a result that is now never coming,
				// and a timeout three minutes later would name nothing.
				e.err = fmt.Errorf("rank %d: %w", req.Rank, addErr)
			} else {
				for _, c := range contributors {
					e.seen[c] = true
				}
			}
		}
		complete = e.err != nil || liveIn(s.ddp, req.Group, e.seen) >= e.expectedCount()
		if complete && e.err == nil && e.result == nil {
			s.ddp.finishLocked(req.Group, e)
		}
		if e.err != nil {
			s.ddp.abortLocked(req.Group, e.err.Error())
		}
	} else {
		e.blobs[req.Rank] = blob
		complete = liveIn(s.ddp, req.Group, e.blobs) >= e.expectedCount()
	}
	if complete {
		select {
		case <-e.done:
		default:
			close(e.done)
		}
	}
	partialAt := e.partialAt
	s.ddp.mu.Unlock()

	if !complete {
		partialC := time.After(time.Until(partialAt))
		hard := ddpHardTimeout
		if d := time.Until(partialAt) + time.Minute; d > hard {
			hard = d // the start barrier outlasts the usual bound
		}
		hardC := time.After(hard)
	wait:
		for {
			select {
			case <-e.done:
				break wait
			case <-r.Context().Done():
				return
			case <-partialC:
				// A rank handing over or loading the model at a join is busy,
				// not dead: hold the round for it (bounded) rather than
				// average without it.
				if s.handoverHolds(req.Group, key) {
					partialC = time.After(ddpHandoverPoll)
					continue
				}
				// Long enough to rule out an ordinary slow step, short enough
				// that a dead rank does not cost the run. Complete with whoever
				// turned up: averaging over fewer ranks is a smaller step in the
				// same direction, and every rank still receives the identical
				// result, so nobody drifts.
				if !s.completePartial(req.Group, key, req.Rank) {
					writeJSON(w, http.StatusGatewayTimeout, map[string]string{
						"error": "ddp sync timed out and too few ranks had arrived to " +
							"average anything"})
					return
				}
				break wait
			case <-hardC:
				if s.handoverHolds(req.Group, key) {
					hardC = nil // the hold has its own bound; partialC ends it
					continue
				}
				writeJSON(w, http.StatusGatewayTimeout,
					map[string]string{"error": "ddp sync timed out waiting for peer ranks"})
				return
			}
		}
	}

	s.ddp.mu.Lock()
	if e.acc != nil || e.result != nil || e.err != nil {
		result, rerr, agreedEvery, rscale := e.result, e.err, e.syncEvery, e.resultScale
		rscales, _ := json.Marshal(e.resultScales)
		driftJSON := ""
		if e.hasDrift {
			driftJSON = ", \"drift\": " + strconv.FormatFloat(e.drift, 'g', 6, 64)
		}
		sameWork := e.checked && e.sameWork && e.fingerprinted > 1
		// How many ranks the result actually averages. Equal to the world
		// size unless the round completed short-handed, and the shim says so
		// when it is not - a run quietly training on half its machines is
		// exactly the kind of thing that should never be discovered later.
		ranks := e.world
		if e.partial > 0 {
			ranks = e.partial
		}
		// The ring's notes ride shard 0 only: the shim reads that reply for
		// them, and a note marked told on another shard's reply is lost.
		rebalanceJSON := ""
		for _, c := range contributors {
			e.fetched[c] = true
			if lead {
				if f := s.ddp.noteFor(req.Group, c, req.Seq); f != "" {
					rebalanceJSON = f
				}
			}
		}
		if len(e.fetched) >= e.expectedCount() || rerr != nil {
			delete(s.ddp.entries, key)
		}
		s.ddp.mu.Unlock()
		if rerr != nil {
			// The round cannot be completed for anybody, so the ring is
			// over: 409 with abort tells every rank to stop (exit 76)
			// rather than retry or train on alone.
			writeJSON(w, http.StatusConflict, map[string]any{"abort": true, "error": rerr.Error()})
			return
		}
		body := []byte(fmt.Sprintf(
			"{\"reduced\": true, \"sync_every\": %d, \"scale\": %v, \"scales\": %s, "+
				"\"same_work\": %t, \"ranks\": %d%s%s}\n",
			agreedEvery, rscale, rscales, sameWork, ranks, driftJSON, rebalanceJSON))
		body = putFrame(body, result)
		w.Header().Set("Content-Type", DDPReduceContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}
	ordered := make([][]byte, e.world)
	for rank, b := range e.blobs {
		ordered[rank] = b
	}
	e.fetched[req.Rank] = true
	if len(e.fetched) >= e.expectedCount() {
		delete(s.ddp.entries, key)
	}
	s.ddp.mu.Unlock()

	if mode == ddpModeFrames {
		body := []byte(fmt.Sprintf("{\"blob_frames\": %d}\n", len(ordered)))
		for _, b := range ordered {
			body = putFrame(body, b)
		}
		w.Header().Set("Content-Type", DDPFramesContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}
	asStrings := make([]string, len(ordered))
	for i, b := range ordered {
		asStrings[i] = string(b)
	}
	writeJSON(w, http.StatusOK, map[string]any{"blobs": asStrings})
}

// ddpLocalRound is one sync round of the ranks sharing this daemon's machine
// (the multi-GPU fast path, U26): their contributions are summed here, sent
// to the lead as one, and the lead's reply is handed back to each of them
// byte for byte. A machine with two GPUs then uploads and downloads one
// gradient a step instead of two.
type ddpLocalRound struct {
	acc     *ddpAccumulator
	hdr     ddpSyncRequest // the first arrival's header: the forward's template
	members []ddpMember
	want    int
	sent    bool          // the forward has started; later arrivals only wait
	fetched int           // members that took the reply home
	done    chan struct{} // closed once status, ctype and body hold the answer
	status  int
	ctype   string
	body    []byte
	created time.Time
}

// handleDDPLocal is one rank's half of a machine-local round. The round goes
// to the lead once every local rank is in, or after ddpPartialAfter with
// whoever is (a dead local rank must not hold up the ring longer than a dead
// remote one would); the lead's answer, refusals included, is every local
// rank's answer.
func (s *Server) handleDDPLocal(w http.ResponseWriter, r *http.Request, req ddpSyncRequest, blob []byte, mode ddpMode) {
	if mode != ddpModeReduce || req.Count <= 0 || req.DType.size() == 0 || req.Shards > 0 ||
		!slices.Contains(req.Local, req.Rank) || !strings.HasPrefix(req.Upstream, "http://") ||
		!strings.HasSuffix(req.Upstream, "/v1/ddp/sync") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a machine-local sync needs " +
			"reduce mode, a count and dtype, this rank in local, and the lead's /v1/ddp/sync as upstream"})
		return
	}
	key := req.Group + "/" + jsonInt(req.Seq) + "@local"
	s.ddp.mu.Lock()
	lr := s.ddp.local[key]
	if lr == nil {
		lr = &ddpLocalRound{acc: newDDPAccumulator(req.DType, req.Count), hdr: req,
			want: len(req.Local), done: make(chan struct{}), created: time.Now()}
		if len(req.Counts) > 1 {
			if err := lr.acc.withSegments(req.Counts); err != nil {
				s.ddp.mu.Unlock()
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "segment lengths: " + err.Error()})
				return
			}
		}
		s.ddp.local[key] = lr
	}
	fresh := !slices.ContainsFunc(lr.members, func(m ddpMember) bool { return m.Rank == req.Rank })
	if fresh && !lr.sent {
		scales := req.Scales
		if len(scales) == 0 {
			scales = []float64{req.Scale}
		}
		if err := lr.acc.addSegmented(blob, scales, float64(req.Samples)); err != nil {
			s.ddp.mu.Unlock()
			// The other local ranks go on without this one (partial), and the
			// lead counts it missing; this rank stops with the reason.
			log.Warn().Str("group", req.Group).Int("rank", req.Rank).Err(err).
				Msg("ddp: machine-local sync refused a contribution")
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("rank %d: %v", req.Rank, err)})
			return
		}
		lr.members = append(lr.members, ddpMember{Rank: req.Rank, StepMillis: req.StepMillis,
			SyncMillis: req.SyncMillis, Samples: req.Samples})
		lr.hdr.SyncEvery = max(lr.hdr.SyncEvery, req.SyncEvery)
		lr.hdr.Sharded = lr.hdr.Sharded || req.Sharded
		lr.hdr.Fixed = lr.hdr.Fixed || req.Fixed
	}
	if !lr.sent && len(lr.members) >= lr.want {
		lr.sent = true
		go s.ddpForwardLocal(key, lr)
	}
	s.ddp.mu.Unlock()

	partial := time.NewTimer(ddpPartialAfter)
	defer partial.Stop()
	hard := time.NewTimer(ddpPartialAfter + ddpHardTimeout + time.Minute)
	defer hard.Stop()
	for {
		select {
		case <-lr.done:
			s.ddp.mu.Lock()
			if lr.fetched++; lr.fetched >= len(lr.members) && s.ddp.local[key] == lr {
				delete(s.ddp.local, key)
			}
			s.ddp.mu.Unlock()
			w.Header().Set("Content-Type", lr.ctype)
			w.WriteHeader(lr.status)
			_, _ = w.Write(lr.body)
			return
		case <-r.Context().Done():
			return
		case <-partial.C:
			s.ddp.mu.Lock()
			if !lr.sent && len(lr.members) > 0 {
				lr.sent = true
				log.Warn().Str("group", req.Group).Int64("seq", req.Seq).
					Int("arrived", len(lr.members)).Int("want", lr.want).
					Msg("ddp: machine-local sync goes to the lead without every local rank")
				go s.ddpForwardLocal(key, lr)
			}
			s.ddp.mu.Unlock()
		case <-hard.C:
			writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "machine-local ddp sync timed out"})
			return
		}
	}
}

// ddpForwardLocal sends a machine's combined contribution to the lead: the
// sample-weighted mean of its ranks, weighted at the lead by their total
// samples, so the ring's average is the same sum(n_i*g_i)/sum(n_i) the hub
// computes. int8 is re-quantised here with its own error feedback (the
// machine's uplink half, kept apart from any downlink residual).
func (s *Server) ddpForwardLocal(key string, lr *ddpLocalRound) {
	s.ddp.mu.Lock()
	var res []float64
	if lr.acc.dtype == ddpI8 {
		run := s.ddp.run(lr.hdr.Group)
		if run.residual == nil {
			run.residual = map[string][]float64{}
		}
		k := "up/" + lr.hdr.Kind + "/" + strconv.Itoa(len(lr.acc.sum))
		if res = run.residual[k]; res == nil {
			res = make([]float64, len(lr.acc.sum))
			run.residual[k] = res
		}
	}
	payload, scales, err := lr.acc.meanResidual(res)
	hdr := lr.hdr
	hdr.Rank, hdr.Members = lr.members[0].Rank, append([]ddpMember(nil), lr.members...)
	hdr.Local = nil // the lead takes it as an ordinary contribution; ddpPostUpstream drops Upstream
	hdr.StepMillis, hdr.SyncMillis, hdr.Data = 0, 0, ""
	hdr.Samples = int(math.Round(lr.acc.weight))
	hdr.Scale, hdr.Scales = 0, nil
	if lr.acc.dtype == ddpI8 && len(scales) > 0 {
		hdr.Scale, hdr.Scales = scales[0], scales
	}
	lr.acc = nil
	s.ddp.mu.Unlock()

	status, ctype, body := http.StatusConflict, "application/json", []byte(nil)
	if err == nil {
		status, ctype, body, err = ddpPostUpstream(hdr, payload)
	}
	if err != nil {
		// No answer from the lead is the lead lost for every local rank (C3
		// exit 76), said in the same words the shim uses for a direct sync.
		log.Warn().Str("group", hdr.Group).Str("upstream", hdr.Upstream).Err(err).
			Msg("ddp: machine-local sync could not reach the lead")
		status, ctype = http.StatusConflict, "application/json"
		body, _ = json.Marshal(map[string]any{"abort": true,
			"error": "the lead machine stopped answering this machine's daemon (" + err.Error() + ")"})
	}
	s.ddp.mu.Lock()
	lr.status, lr.ctype, lr.body = status, ctype, body
	close(lr.done)
	s.ddp.mu.Unlock()
}

// ddpPostUpstream POSTs one combined contribution to the lead daemon and
// returns its answer as is.
func ddpPostUpstream(hdr ddpSyncRequest, payload []byte) (int, string, []byte, error) {
	upstream := hdr.Upstream
	hdr.Upstream = ""
	head, err := json.Marshal(hdr)
	if err != nil {
		return 0, "", nil, err
	}
	req, err := http.NewRequest(http.MethodPost, upstream, bytes.NewReader(putFrame(append(head, '\n'), payload)))
	if err != nil {
		return 0, "", nil, err
	}
	req.Header.Set("Content-Type", DDPReduceContentType)
	if tok := authtoken.Current(); tok != "" {
		req.Header.Set("X-Pipedpeer-Token", tok)
	}
	// The lead holds a round up to its start wait or partial timeout; this
	// bounds only a lead that stopped answering altogether.
	resp, err := (&http.Client{Timeout: ddpHardTimeout + time.Minute}).Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", nil, err
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), body, nil
}

// handleDDPSyncStatus says which ranks have reached a round, so a rank held
// at the start barrier can say whom it is waiting for.
func (s *Server) handleDDPSyncStatus(w http.ResponseWriter, r *http.Request) {
	seq, err := strconv.ParseInt(r.URL.Query().Get("seq"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "seq: " + err.Error()})
		return
	}
	arrived := []int{}
	world := 0
	s.ddp.mu.Lock()
	if e := s.ddp.entries[r.URL.Query().Get("group")+"/"+jsonInt(seq)]; e != nil {
		world = e.world
		for rank := range e.seen {
			arrived = append(arrived, rank)
		}
		for rank := range e.blobs {
			arrived = append(arrived, rank)
		}
	}
	s.ddp.mu.Unlock()
	sort.Ints(arrived)
	writeJSON(w, http.StatusOK, map[string]any{"world": world, "arrived": arrived})
}

func jsonInt(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// ddpFingerprintSeqs is how many early syncs are checked for ranks doing
// identical work.
const ddpFingerprintSeqs = 3

// fnv1a is FNV-1a 64. Enough to tell "these two models are the same" from
// "these two models differ", which is the only question being asked, and fast
// enough to run over a model without showing up beside a network round trip.
func fnv1a(b []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

// ddpPartialAfter is how long a sync waits for every rank before going ahead
// with the ones present. A dead rank and a slow one look identical from here,
// and the old three-minute wait meant one lost machine stalled the run and
// then failed it.
var ddpPartialAfter = 45 * time.Second

// ddpHandoverHold bounds how long a round is held open, from its creation,
// for the ranks busy with a join handover. Under the shim's 240 s sync
// timeout, so the held ranks are still waiting when the round closes.
// ponytail: a model too big to hand over in this time joins late (the round
// goes partial once); raise it together with the shim's timeout if that bites.
var ddpHandoverHold = 200 * time.Second

// ddpHandoverPoll is how often a held round looks again.
var ddpHandoverPoll = time.Second

// handoverHolds reports whether the round at key should keep waiting because
// a join handover is in flight and the ranks it occupies (the one handing
// over, and the joiner once the round counts it) have not posted yet.
func (s *Server) handoverHolds(group, key string) bool {
	s.ddp.mu.Lock()
	defer s.ddp.mu.Unlock()
	e := s.ddp.entries[key]
	run := s.ddp.runs[group]
	if e == nil || run == nil || e.result != nil || e.err != nil {
		return false
	}
	j, handingOver := run.join, true
	if j == nil {
		j, handingOver = run.loading, false
	}
	if j == nil {
		return false
	}
	arrived := func(rank int) bool {
		if e.seen[rank] {
			return true
		}
		_, ok := e.blobs[rank]
		return ok
	}
	busy := (handingOver && !arrived(j.from) && !run.dropped[j.from]) ||
		(j.rank < e.world && !arrived(j.rank) && !run.dropped[j.rank])
	if !busy {
		return false
	}
	e.held = true
	if time.Since(e.created) > ddpHandoverHold {
		if !e.holdExpired {
			e.holdExpired = true
			log.Warn().Str("group", group).Int("join", j.rank).Int("from", j.from).
				Dur("held", time.Since(e.created).Round(time.Second)).
				Msg("ddp join handover still in flight; closing the round without the ranks busy with it")
		}
		return false
	}
	return true
}

// ddpHardTimeout bounds the whole wait, for the case where partial completion
// is not possible either.
const ddpHardTimeout = 3 * time.Minute

// completePartial finishes a round with whoever has contributed, and reports
// whether the round has an answer now (a result, or the ring's abort).
//
// Under the lock and idempotent: several ranks reach their deadline at about
// the same moment, and only the first should compute the result - the others
// must find it already there rather than averaging a second time.
//
// A later round left with only the asking rank is a ring whose every peer
// stopped answering without anyone saying so: the run is aborted rather than
// letting one rank train on its slice alone at one timeout per step.
func (s *Server) completePartial(group, key string, rank int) bool {
	s.ddp.mu.Lock()
	defer s.ddp.mu.Unlock()

	e := s.ddp.entries[key]
	if e == nil {
		return false
	}
	if e.result != nil || e.err != nil {
		return true // somebody else already finished it
	}
	if e.acc == nil || e.acc.count == 0 {
		return false
	}
	if !e.opening && !e.held && e.expectedCount() >= 2 && liveIn(s.ddp, group, e.seen) == 1 && e.seen[rank] {
		var gone []string
		run := s.ddp.runs[group]
		for r := 0; r < e.world; r++ {
			if !e.seen[r] && (run == nil || !run.dropped[r]) {
				gone = append(gone, strconv.Itoa(r))
			}
		}
		e.err = fmt.Errorf("every other machine in the ring stopped answering (no sync from rank %s for %s)",
			strings.Join(gone, ", "), time.Since(e.created).Round(time.Second))
		e.acc = nil
		s.ddp.abortLocked(group, e.err.Error())
	} else {
		count := e.acc.count
		s.ddp.finishLocked(group, e)
		e.partial = count
	}
	select {
	case <-e.done:
	default:
		close(e.done)
	}
	if e.opening && e.err == nil {
		s.ddp.dropMissedStart(group, e)
	}
	return true
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
