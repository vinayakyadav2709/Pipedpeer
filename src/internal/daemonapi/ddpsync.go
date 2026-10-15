package daemonapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
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
		run.autoSync = req.SyncMode == "auto"
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
		if !lead || rep.StepMillis <= 0 || rep.Samples < 0 {
			continue // a pad step's sample count says nothing about speed
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
