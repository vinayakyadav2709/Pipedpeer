package daemonapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
)

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
	if lr.acc.weight <= 0 {
		hdr.Samples = -1 // every rank here padded: the machine weighs nothing
	}
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
