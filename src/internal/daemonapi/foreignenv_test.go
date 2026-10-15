package daemonapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nixgen"
)

// TestBroadcastOffersTheRecipeToAPeerOfAnotherSystem: a full node of another
// system is not sent the closure but the recipe it was built from.
func TestBroadcastOffersTheRecipeToAPeerOfAnotherSystem(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("f", 8))
	defer s.StopWarmWorkers()

	got := make(chan foreignEnvRequest, 1)
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/env/foreign" {
			t.Errorf("a peer of another system was asked %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		var req foreignEnvRequest
		json.NewDecoder(r.Body).Decode(&req)
		got <- req
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "building"})
	}))
	defer peerSrv.Close()
	hostport := strings.TrimPrefix(peerSrv.URL, "http://")
	peer := &PeerHealth{Host: strings.Split(hostport, ":")[0], Port: mustPort(t, hostport), Status: "healthy", Arch: "other-arch-linux"}
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{hostport: peer}
	s.peersMu.Unlock()

	const store = "/nix/store/abc-env"
	s.recipes.Store(store, nixgen.Recipe{Pkgs: []string{"pandas"}, Python: "python314"})
	s.broadcastClosure(store)
	select {
	case req := <-got:
		if req.Store != store || len(req.Recipe.Pkgs) != 1 || req.Recipe.Pkgs[0] != "pandas" || req.Recipe.Python != "python314" {
			t.Fatalf("offer = %+v", req)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the peer of another system was never offered the recipe")
	}
}
