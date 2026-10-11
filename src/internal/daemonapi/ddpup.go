package daemonapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
)

// A rank joining a running ring: a machine that came back, or a new one.
//
// The submitter starts the rank's job; the rank's shim says it is ready
// (POST /v1/ddp/join), which publishes a note growing the ring. Every rank
// takes the new size and shares at its next epoch boundary, where the lowest
// live rank also uploads its model and optimizer (POST /v1/ddp/snapshot). The
// joiner, waiting on GET /v1/ddp/snapshot, loads them, skips the epochs the
// ring already did and posts its first sync with everybody else's next one.
//
// Only a ring that shards a DataLoader by hash can do this: the joiner's
// share has to come out of everybody else's, which a script that slices its
// own tensors never lets the shim change.

type ddpJoin struct {
	rank     int
	from     int // the rank that hands over the model
	oldWorld int
	snap     []byte        // header line + frame, once uploaded
	ready    chan struct{} // closed when snap arrives or the join ends
}

type ddpJoinRequest struct {
	Group string `json:"group"`
	Rank  int    `json:"rank"`
	World int    `json:"world"`
	// Check asks whether a rank could join, without announcing one. The
	// submitter asks before it starts a job, so a ring that cannot grow is
	// said once rather than discovered by a joiner that waits forever.
	Check bool `json:"check,omitempty"`
}

// snapshotMaxBytes bounds one uploaded model + optimizer.
// ponytail: held in memory on the lead daemon until the joiner takes it;
// stream to the job dir if models ever outgrow this.
const snapshotMaxBytes = 4 << 30

// ddpSnapshotWait is how long one GET waits before answering 204.
var ddpSnapshotWait = 25 * time.Second

func (s *Server) handleDDPJoin(w http.ResponseWriter, r *http.Request) {
	var req ddpJoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Group == "" || req.Rank < 1 || req.World != req.Rank+1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "group, rank >= 1 and world = rank+1 required"})
		return
	}
	msg, final := s.ddpJoin(req)
	if msg != "" {
		writeJSON(w, http.StatusConflict, map[string]any{"error": msg, "final": final})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"world": req.World})
}

// ddpJoin admits (or, with Check, vets) a joining rank. It returns why not,
// and whether asking again later is pointless.
func (s *Server) ddpJoin(req ddpJoinRequest) (string, bool) {
	b := s.ddp
	b.mu.Lock()
	defer b.mu.Unlock()

	r := b.runs[req.Group]
	if r == nil || r.lastSeq == 0 {
		return "the ring has not synced yet", false
	}
	if !r.sharded {
		return "this script does not train from a DataLoader the shim can reshard " +
			"(it slices its own tensors), so a rank cannot join mid-run", true
	}
	if r.join != nil {
		return fmt.Sprintf("rank %d is still joining", r.join.rank), false
	}
	if req.Rank < r.world {
		return fmt.Sprintf("rank %d is already in the ring of %d", req.Rank, r.world), false
	}
	for k := r.world; k < req.Rank; k++ {
		if !r.dropped[k] {
			return fmt.Sprintf("rank %d is still starting", k), false
		}
	}
	old := r.world
	from := lowestLive(r, old)
	if from < 0 {
		return "no rank is left to hand over the model", true
	}
	if req.Check {
		return "", false
	}

	// Indices skipped by joiners that died before they were ready.
	for k := old; k < req.Rank; k++ {
		r.dropped[k] = true
	}
	r.weights = sharesWith(r.weights, old, r.dropped, req.World)
	r.world = req.World
	r.join = &ddpJoin{rank: req.Rank, from: from, oldWorld: old, ready: make(chan struct{})}
	live := req.World - b.dropsBelow(req.Group, req.World)
	why := fmt.Sprintf("rank %d joins; the ring grows to %d rank(s) at the next epoch, "+
		"each taking a share of the data", req.Rank, live)
	b.publish(r, r.lastSeq+1, ringFragment(r)+fmt.Sprintf(`, "why": %q`, why))
	log.Info().Str("group", req.Group).Int("rank", req.Rank).Int("from", from).
		Str("shares", sharePercents(r.weights)).Msg("ddp rank joining")
	return "", false
}

// sharesWith extends a ring's shares to world ranks, giving the new rank as
// much as an average live rank and scaling the rest down to make room. Ranks
// in [old, world) other than the last are holes: zero.
func sharesWith(weights []float64, old int, dropped map[int]bool, world int) []float64 {
	if len(weights) != old {
		weights = make([]float64, old)
		for i := range weights {
			weights[i] = 1
		}
	}
	base := sharesWithout(weights, dropped)
	live := 0
	for i, x := range base {
		if x > 0 && !dropped[i] {
			live++
		}
	}
	out := make([]float64, world)
	if live == 0 {
		out[world-1] = 1
		return out
	}
	scale := float64(live) / float64(live+1)
	for i, x := range base {
		out[i] = x * scale
	}
	out[world-1] = 1 / float64(live+1)
	return out
}

// lowestLive is the first rank below n the ring still has, or -1.
func lowestLive(r *ddpRun, n int) int {
	for k := 0; k < n; k++ {
		if !r.dropped[k] {
			return k
		}
	}
	return -1
}

// endJoin forgets the join in progress. Caller holds the board lock.
func (b *ddpBoard) endJoin(r *ddpRun) {
	if j := r.join; j != nil {
		select {
		case <-j.ready:
		default:
			close(j.ready)
		}
		r.join = nil
	}
}

type ddpSnapshotHeader struct {
	Group string `json:"group"`
	Rank  int    `json:"rank"`
	Join  int    `json:"join"`
}

// handleDDPSnapshotPut takes the model and optimizer from the rank handing
// them over, at the epoch boundary where the ring grew. The body is kept as
// sent and served to the joiner verbatim.
func (s *Server) handleDDPSnapshotPut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, snapshotMaxBytes))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	nl := bytes.IndexByte(body, '\n')
	if nl < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "snapshot has no header line"})
		return
	}
	var h ddpSnapshotHeader
	if err := json.Unmarshal(body[:nl], &h); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if _, _, err := readFrame(body[nl+1:]); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	b := s.ddp
	b.mu.Lock()
	run := b.runs[h.Group]
	if run == nil || run.join == nil || run.join.rank != h.Join || run.join.snap != nil {
		b.mu.Unlock()
		log.Warn().Str("group", h.Group).Int("join", h.Join).Int("from", h.Rank).
			Msg("ddp snapshot refused: no rank is waiting for it")
		writeJSON(w, http.StatusConflict, map[string]string{"error": "no rank is waiting to join"})
		return
	}
	run.join.snap = body
	close(run.join.ready)
	b.mu.Unlock()
	log.Info().Str("group", h.Group).Int("join", h.Join).Int("from", h.Rank).Int("bytes", len(body)).
		Msg("ddp snapshot ready for the joining rank")
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleDDPSnapshotGet is the joiner's long poll: 200 with the snapshot,
// 204 when the ring has not reached its boundary yet, 410 when this rank is
// no longer joining.
func (s *Server) handleDDPSnapshotGet(w http.ResponseWriter, r *http.Request) {
	group := r.URL.Query().Get("group")
	rank, err := strconv.Atoi(r.URL.Query().Get("rank"))
	if group == "" || err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "group and rank required"})
		return
	}
	b := s.ddp
	// take reads the snapshot but leaves the join open: it ends only once the
	// joiner has the whole snapshot, so a write that breaks can be retried.
	var joined *ddpJoin
	take := func() ([]byte, <-chan struct{}, bool) {
		b.mu.Lock()
		defer b.mu.Unlock()
		run := b.runs[group]
		if run == nil || run.join == nil || run.join.rank != rank {
			return nil, nil, false
		}
		j := run.join
		if j.snap == nil {
			return nil, j.ready, true
		}
		joined = j
		return j.snap, nil, true
	}
	snap, ready, ok := take()
	if ok && snap == nil {
		select {
		case <-ready:
			snap, _, ok = take()
		case <-time.After(ddpSnapshotWait):
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			return
		}
	}
	if !ok || snap == nil {
		writeJSON(w, http.StatusGone, map[string]string{
			"error": "this rank is no longer joining: the ring dropped it, or the run moved on"})
		return
	}
	w.Header().Set("Content-Type", DDPFramesContentType)
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(snap); err != nil {
		log.Warn().Err(err).Str("group", group).Int("rank", rank).Int("bytes", len(snap)).
			Msg("ddp snapshot write to the joining rank failed; its next poll gets it again")
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	run := b.runs[group]
	if run == nil || run.join != joined {
		return // the ring dropped the joiner, or a new join began, meanwhile
	}
	// It loads the ring as of the boundary, so the note that announced its
	// own arrival is not news to it.
	run.noteTold[rank] = true
	run.join = nil
	log.Info().Str("group", group).Int("rank", rank).Int("world", run.world).Msg("ddp rank joined")
}
