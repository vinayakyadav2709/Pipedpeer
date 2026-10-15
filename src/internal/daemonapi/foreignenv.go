package daemonapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/nixgen"
	"github.com/pipedpeer/pipedpeer/internal/phone"
	"github.com/rs/zerolog/log"
)

// foreignEnvRequest is POST /v1/env/foreign: the submitter's store path and
// the recipe it was built from, for a peer of another system to build its own
// (phone.BuildForeign).
type foreignEnvRequest struct {
	Store  string        `json:"store"`
	Recipe nixgen.Recipe `json:"recipe"`
}

func (s *Server) handleForeignEnv(w http.ResponseWriter, r *http.Request) {
	var req foreignEnvRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request: " + err.Error()})
		return
	}
	if err := phone.BuildForeign(req.Store, req.Recipe); err != nil {
		log.Warn().Err(err).Str("store", req.Store).Msg("refusing a foreign job environment")
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "building"})
}

// offerRecipes sends storePath's recipe (recorded at upload) to full-node
// peers of another system, which cannot import the closure. Best-effort and
// in the background: such a peer joins the pool once its build is done, as a
// slow importer does.
func (s *Server) offerRecipes(storePath string, peers []PeerHealth) {
	v, ok := s.recipes.Load(storePath)
	if !ok {
		if len(peers) > 0 {
			log.Info().Int("peers", len(peers)).Str("store", storePath).
				Msg("no recipe for this closure (older submitter); peers of another system run its chunks on their pinned python")
		}
		return
	}
	body, err := json.Marshal(foreignEnvRequest{Store: storePath, Recipe: v.(nixgen.Recipe)})
	if err != nil {
		log.Error().Err(err).Str("store", storePath).Msg("cannot encode the job recipe")
		return
	}
	for _, ph := range peers {
		go func(ph PeerHealth) {
			if err := postForeignEnv(ph, body); err != nil {
				log.Warn().Err(err).Str("peer", ph.addr()).Str("arch", ph.Arch).Str("store", storePath).
					Msg("peer of another system cannot build this job's environment; it takes none of its chunks")
				return
			}
			log.Info().Str("peer", ph.addr()).Str("arch", ph.Arch).Str("store", storePath).
				Msg("sent the job recipe to a peer of another system; it joins the pool once built")
		}(ph)
	}
}

func postForeignEnv(ph PeerHealth, body []byte) error {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/v1/env/foreign", ph.addr()), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if tok := authtoken.Current(); tok != "" {
		req.Header.Set("X-Pipedpeer-Token", tok)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(b))
	}
	return nil
}
