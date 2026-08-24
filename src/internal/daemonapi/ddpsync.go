package daemonapi

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
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

type ddpEntry struct {
	blobs   map[int]string // rank -> b64 payload
	fetched map[int]bool   // ranks that have taken the full set home
	done    chan struct{}  // closed when all world blobs are present
	world   int
	created time.Time
}

type ddpBoard struct {
	mu      sync.Mutex
	entries map[string]*ddpEntry // key: group + "/" + seq
}

func newDDPBoard() *ddpBoard {
	return &ddpBoard{entries: make(map[string]*ddpEntry)}
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
}

type ddpSyncRequest struct {
	Group string `json:"group"`
	Seq   int64  `json:"seq"`
	Rank  int    `json:"rank"`
	World int    `json:"world"`
	Data  string `json:"data"` // b64, opaque to the daemon
}

// handleDDPSync is one round trip per rank per sync: deposit our blob, block
// until the set is complete, take every rank's blob home.
func (s *Server) handleDDPSync(w http.ResponseWriter, r *http.Request) {
	var req ddpSyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Group == "" || req.World < 1 || req.Rank < 0 || req.Rank >= req.World {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "group, world and rank in [0,world) required"})
		return
	}

	key := req.Group + "/" + jsonInt(req.Seq)

	s.ddp.mu.Lock()
	e, ok := s.ddp.entries[key]
	if !ok {
		e = &ddpEntry{
			blobs:   make(map[int]string),
			fetched: make(map[int]bool),
			done:    make(chan struct{}),
			world:   req.World,
			created: time.Now(),
		}
		s.ddp.entries[key] = e
	}
	if e.world != req.World {
		s.ddp.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "world size mismatch within group"})
		return
	}
	e.blobs[req.Rank] = req.Data
	complete := len(e.blobs) == e.world
	if complete {
		select {
		case <-e.done:
		default:
			close(e.done)
		}
	}
	s.ddp.mu.Unlock()

	if !complete {
		select {
		case <-e.done:
		case <-r.Context().Done():
			return
		case <-time.After(3 * time.Minute):
			writeJSON(w, http.StatusGatewayTimeout,
				map[string]string{"error": "ddp sync timed out waiting for peer ranks"})
			return
		}
	}

	s.ddp.mu.Lock()
	blobs := make([]string, e.world)
	for rank, blob := range e.blobs {
		blobs[rank] = blob
	}
	e.fetched[req.Rank] = true
	if len(e.fetched) == e.world {
		delete(s.ddp.entries, key)
	}
	s.ddp.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{"blobs": blobs})
}

func jsonInt(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
