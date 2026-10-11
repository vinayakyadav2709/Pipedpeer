package daemonapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

// A rank that died mid-run. The submitter sees the rank's job fail and says
// so here, on the lead daemon, so the ring stops waiting for it and its share
// of the data moves to the ranks left.
//
// Without this the 45 s partial timeout still lets the run continue, but it
// pays those 45 s on EVERY remaining sync - a dead rank and a slow one look
// the same from here - and the dead rank's data is simply never trained on.

type ddpDownRequest struct {
	Group string `json:"group"`
	Rank  int    `json:"rank"`
	World int    `json:"world"`
	Why   string `json:"why,omitempty"`
}

func (s *Server) handleDDPDown(w http.ResponseWriter, r *http.Request) {
	var req ddpDownRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Group == "" || req.World < 2 || req.Rank < 0 || req.Rank >= req.World {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "group, world >= 2 and rank in [0,world) required"})
		return
	}
	left := s.markRankDown(req.Group, req.Rank, req.World, req.Why)
	writeJSON(w, http.StatusOK, map[string]int{"ranks_left": left})
}

// markRankDown drops a dead rank from a run and returns how many ranks are
// left. Idempotent.
func (s *Server) markRankDown(group string, rank, world int, why string) int {
	b := s.ddp
	b.mu.Lock()
	defer b.mu.Unlock()

	r := b.run(group)
	if r.world == 0 {
		r.world = world
	}
	if rank >= r.world {
		// A rank started to join and died before it said it was ready: the
		// ring never counted it, so there is nothing to re-spread. Recorded
		// so a later joiner does not wait on its index.
		r.dropped[rank] = true
		return r.world - b.dropsBelow(group, r.world)
	}
	world = r.world
	left := world - b.dropsBelow(group, world)
	if r.dropped[rank] {
		return left
	}
	r.dropped[rank] = true
	left--
	// Its last report would keep the refit giving it a share; without every
	// rank's report the refit stays silent, which is right for a ring that
	// has just changed shape.
	delete(r.reports, rank)

	weights := r.weights
	if len(weights) != world {
		weights = make([]float64, world)
		for i := range weights {
			weights[i] = 1
		}
	}
	r.weights = sharesWithout(weights, r.dropped)

	if j := r.join; j != nil {
		switch {
		case j.rank == rank:
			// The joiner died: the ring still grows at the boundary (its
			// size was announced), with a zero share for it.
			b.endJoin(r)
		case j.from == rank:
			j.from = lowestLive(r, j.oldWorld)
		}
	}

	if why == "" {
		why = "its job ended"
	}
	why = fmt.Sprintf("rank %d stopped (%s); its share of the data moves to the %d rank(s) left at the next epoch", rank, why, left)
	b.publish(r, r.lastSeq+1, ringFragment(r)+fmt.Sprintf(`, "down": %d, "why": %q`, rank, why))
	log.Warn().Str("group", group).Int("rank", rank).Int("left", left).Str("shares", sharePercents(r.weights)).
		Msg("ddp rank down")

	// Rounds already open may now have everyone they will ever get. Finish
	// them instead of leaving their waiters to the 45 s timeout.
	prefix := group + "/"
	for key, e := range b.entries {
		if !strings.HasPrefix(key, prefix) || e.result != nil || e.err != nil {
			continue
		}
		expected := max(1, e.world-b.dropsBelow(group, e.world))
		e.expected = expected
		switch {
		case e.acc != nil && e.acc.count > 0 && len(e.seen) >= expected:
			e.result, e.resultScales, e.err = e.acc.meanSegmented()
			if len(e.resultScales) > 0 {
				e.resultScale = e.resultScales[0]
			}
			e.acc = nil
		case e.acc == nil && len(e.blobs) > 0 && len(e.blobs) >= expected:
		default:
			continue
		}
		if len(e.seen) < e.world && len(e.seen) > 0 {
			e.partial = len(e.seen)
		}
		select {
		case <-e.done:
		default:
			close(e.done)
		}
	}
	return left
}

// publish makes frag the ring's note, told once to each rank on its first
// reply from sync seq on. Caller holds the board lock.
func (b *ddpBoard) publish(r *ddpRun, seq int64, frag string) {
	r.note = frag
	r.noteSeq = seq
	r.noteTold = map[int]bool{}
}

// noteFor returns the ring's note for this rank's reply, once, from the
// agreed sync on. Caller holds the board lock.
func (b *ddpBoard) noteFor(group string, rank int, seq int64) string {
	r := b.runs[group]
	if r == nil || r.note == "" || seq < r.noteSeq || r.noteTold[rank] {
		return ""
	}
	r.noteTold[rank] = true
	return r.note
}

// ringFragment is the ring as it now is, for a note: every rank's share and
// the ring's size, plus the joining rank while one is on its way in. Caller
// holds the board lock.
func ringFragment(r *ddpRun) string {
	parts := make([]string, len(r.weights))
	for i, x := range r.weights {
		parts[i] = strconv.FormatFloat(x, 'f', 6, 64)
	}
	frag := fmt.Sprintf(`, "weights": [%s], "world": %d`, strings.Join(parts, ","), len(r.weights))
	if j := r.join; j != nil && j.snap == nil {
		frag += fmt.Sprintf(`, "join": %d, "from": %d`, j.rank, j.from)
	}
	return frag
}

// sharesWithout zeroes the dropped ranks' shares and renormalises the rest.
func sharesWithout(weights []float64, dropped map[int]bool) []float64 {
	out := make([]float64, len(weights))
	var sum float64
	for i, x := range weights {
		if !dropped[i] && x > 0 {
			out[i] = x
			sum += x
		}
	}
	if sum <= 0 {
		return out
	}
	for i := range out {
		out[i] /= sum
	}
	return out
}
