package daemonapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// TestAPeerThatRefusesTheClosureIsReportedToTheSubmitter: a fresh worker
// refused every closure ("lacks a signature by a trusted key") and the only
// trace was a warning in the submitting daemon's log, while the user saw
// "local 80 items". The refusal is now returned for the upload response.
func TestAPeerThatRefusesTheClosureIsReportedToTheSubmitter(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("r", 8))
	defer s.StopWarmWorkers()

	const reason = "nix copy from a peer's closure: exit status 1: error: cannot add path '/nix/store/b-env' because it lacks a signature by a trusted key"
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/store":
			writeJSON(w, http.StatusOK, map[string]bool{"cached": false})
		case "/v1/store/import":
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": reason})
		default:
			http.NotFound(w, r)
		}
	}))
	defer peerSrv.Close()

	hostport := strings.TrimPrefix(peerSrv.URL, "http://")
	peer := &PeerHealth{Host: strings.Split(hostport, ":")[0], Port: mustPort(t, hostport), Status: "healthy"}
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{hostport: peer}
	s.peersMu.Unlock()

	storePath := filepath.Join(t.TempDir(), "nix", "store", "refused-env")
	if _, err := s.narCache.store(storePath, bytes.NewReader([]byte(storePath+"\n"))); err != nil {
		t.Fatal(err)
	}

	got := s.broadcastClosure(storePath)
	if len(got) != 1 {
		t.Fatalf("want one refusal, got %q", got)
	}
	if !strings.Contains(got[0], hostport) || !strings.Contains(got[0], "lacks a signature by a trusted key") {
		t.Fatalf("the refusal must name the peer and its reason: %q", got[0])
	}
	if strings.Contains(got[0], `{"error"`) {
		t.Fatalf("the peer's JSON reached the user unparsed: %q", got[0])
	}
}

// TestABroadcastWithNoRecipientsPacksNothing (V2 95): the seed-peers stage
// exported the whole 3.6 GB CUDA closure and then sent it to nobody, adding
// 3m24s to every GPU run. Every peer here already holds the closure, so the
// recipient list is empty and nothing may be packed, signed or not.
func TestABroadcastWithNoRecipientsPacksNothing(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("p", 8))
	defer s.StopWarmWorkers()

	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/store":
			writeJSON(w, http.StatusOK, map[string]bool{"cached": true})
		default:
			t.Errorf("a peer that holds the closure was sent %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer peerSrv.Close()
	hostport := strings.TrimPrefix(peerSrv.URL, "http://")
	peer := &PeerHealth{Host: strings.Split(hostport, ":")[0], Port: mustPort(t, hostport), Status: "healthy", ClosureFormats: "narpack1"}
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{hostport: peer}
	s.peersMu.Unlock()

	packs := 0
	oldLocal, oldSigned := localArchive, buildSignedClosure
	localArchive = func(*narCache, string) (string, bool) { packs++; return "", false }
	buildSignedClosure = func(*Server, *PeerHealth, string) (string, func(), bool) { packs++; return "", nil, false }
	defer func() { localArchive, buildSignedClosure = oldLocal, oldSigned }()

	if got := s.broadcastClosure("/nix/store/held-env"); len(got) != 0 {
		t.Errorf("no refusal expected, got %q", got)
	}
	if packs != 0 {
		t.Errorf("packed the closure %d time(s) for an empty recipient list", packs)
	}
}
