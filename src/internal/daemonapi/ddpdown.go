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
	left := world - len(r.dropped)
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

	if why == "" {
		why = "its job ended"
	}
	why = fmt.Sprintf("rank %d stopped (%s); its share of the data moves to the %d rank(s) left at the next epoch", rank, why, left)
	parts := make([]string, len(r.weights))
	for i, x := range r.weights {
		parts[i] = strconv.FormatFloat(x, 'f', 6, 64)
	}
	r.downNote = fmt.Sprintf(`, "weights": [%s], "down": %d, "why": %q`, strings.Join(parts, ","), rank, why)
	r.downSeq = r.lastSeq + 1
	r.downTold = map[int]bool{}
	log.Warn().Str("group", group).Int("rank", rank).Int("left", left).Str("shares", sharePercents(r.weights)).
		Msg("ddp rank down")

	// Rounds already open may now have everyone they will ever get. Finish
	// them instead of leaving their waiters to the 45 s timeout.
	expected := max(1, world-len(r.dropped))
	prefix := group + "/"
	for key, e := range b.entries {
		if !strings.HasPrefix(key, prefix) || e.result != nil || e.err != nil {
			continue
		}
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

// downNoteFor returns the death notice for this rank's reply, once, from the
// agreed sync on. Caller holds the board lock.
func (b *ddpBoard) downNoteFor(group string, rank int, seq int64) string {
	r := b.runs[group]
	if r == nil || r.downNote == "" || seq < r.downSeq || r.downTold[rank] {
		return ""
	}
	r.downTold[rank] = true
	return r.downNote
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
