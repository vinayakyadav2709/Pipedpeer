package daemonapi

import (
	"crypto/rand"
	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Cases from the review of the peer cache: each fix has the finding and its
// opposite.
func TestNixCacheReview(t *testing.T) {
	good := ncNarinfo(t)

	t.Run("names other than .narinfo are 404, bad narinfo hashes stay 400", func(t *testing.T) {
		e := newNCEnv(t, true, nil)
		for path, want := range map[string]int{
			"/abc.ls":                 404,
			"/" + ncHash + ".ls":      404,
			"/log/x":                  404,
			"/realisations/x":         404,
			"/abc.narinfo":            400,
			"/" + ncHash + ".narinfo": 404, // no peers: a miss
		} {
			if w := e.do("GET", path); w.Code != want {
				t.Errorf("%s: got %d, want %d", path, w.Code, want)
			}
		}
	})

	t.Run("a peer with a different NarHash is not a NAR fallback", func(t *testing.T) {
		fallbackGrace = 500 * time.Millisecond
		t.Cleanup(func() { fallbackGrace = 100 * time.Millisecond })
		sk, pk, err := signature.GenerateKeypair("test-1", rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		signed := func(hashFrom, hashTo string) string {
			ni, err := narinfo.Parse(strings.NewReader(strings.Replace(good, hashFrom, hashTo, 1)))
			if err != nil {
				t.Fatal(err)
			}
			sig, err := sk.Sign(rand.Reader, ni.Fingerprint())
			if err != nil {
				t.Fatal(err)
			}
			ni.Signatures = []signature.Signature{sig}
			return ni.String()
		}
		same, other := signed("", ""), signed("NarHash: sha256:0i3p", "NarHash: sha256:1i3p")
		if other == same {
			t.Fatal("test setup: NarHash not replaced")
		}
		run := func(second string) (int, string) {
			a := newNCPeer(t, 200, same, 0, 500, "")
			b := newNCPeer(t, 200, second, 50*time.Millisecond, 200, "FROMB")
			e := newNCEnv(t, true, ncPeers(t, a, b))
			trustedKeys = func() []signature.PublicKey { return []signature.PublicKey{pk} }
			if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 200 {
				t.Fatalf("narinfo: %d", w.Code)
			}
			w := e.do("GET", "/nar/"+ncHash+".nar.zst")
			return w.Code, w.Body.String()
		}
		if code, body := run(other); code != 404 {
			t.Errorf("a different build was served: %d %q", code, body)
		}
		if code, body := run(same); code != 200 || body != "FROMB" {
			t.Errorf("same build as fallback: %d %q", code, body)
		}
	})

	t.Run("a hit is forgotten when every NAR source fails", func(t *testing.T) {
		a := newNCPeer(t, 200, good, 0, 500, "")
		e := newNCEnv(t, true, ncPeers(t, a))
		e.do("GET", "/"+ncHash+".narinfo")
		if w := e.do("GET", "/nar/"+ncHash+".nar.zst"); w.Code != 404 {
			t.Fatal(w.Code)
		}
		before := a.calls.Load()
		e.do("GET", "/"+ncHash+".narinfo")
		if a.calls.Load() == before {
			t.Error("the failed hit was still served from memory")
		}
		// Opposite: a working source keeps its hit.
		b := newNCPeer(t, 200, good, 0, 200, "OK")
		e2 := newNCEnv(t, true, ncPeers(t, b))
		e2.do("GET", "/"+ncHash+".narinfo")
		e2.do("GET", "/nar/"+ncHash+".nar.zst")
		n := b.calls.Load()
		e2.do("GET", "/"+ncHash+".narinfo")
		if b.calls.Load() != n {
			t.Error("a good hit was asked again")
		}
	})

	t.Run("one node under two addresses is asked once", func(t *testing.T) {
		a := newNCPeer(t, 404, "", 0, 200, "")
		peers := map[string]*PeerHealth{"x": ncHealth(t, "N1", a, nil), "y": ncHealth(t, "N1", a, nil)}
		e := newNCEnv(t, true, peers)
		e.do("GET", "/"+ncHash+".narinfo")
		if a.calls.Load() != 1 {
			t.Errorf("asked %d times", a.calls.Load())
		}
		b := newNCPeer(t, 404, "", 0, 200, "")
		e2 := newNCEnv(t, true, map[string]*PeerHealth{"x": ncHealth(t, "N1", b, nil), "y": ncHealth(t, "N2", b, nil)})
		e2.do("GET", "/"+ncHash+".narinfo")
		if b.calls.Load() != 2 {
			t.Errorf("two nodes asked %d times", b.calls.Load())
		}
	})

	t.Run("lookups over the fan-out cap answer a miss without remembering it", func(t *testing.T) {
		a := newNCPeer(t, 200, good, 0, 200, "")
		e := newNCEnv(t, true, ncPeers(t, a))
		for i := 0; i < maxFanOuts; i++ {
			fanOutSem <- struct{}{}
		}
		w := e.do("GET", "/"+ncHash+".narinfo")
		for i := 0; i < maxFanOuts; i++ {
			<-fanOutSem
		}
		if w.Code != 404 || a.calls.Load() != 0 {
			t.Fatalf("over the cap: %d, %d peer calls", w.Code, a.calls.Load())
		}
		if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 200 {
			t.Fatalf("after the cap cleared: %d", w.Code)
		}
	})
}

func TestWarnPeerRateLimit(t *testing.T) {
	e := newNCEnv(t, true, nil)
	peerWarnMu.Lock()
	peerWarnLast = map[string]time.Time{}
	peerWarnMu.Unlock()
	p := PeerHealth{NodeID: "dead"}
	count := func() int { peerWarnMu.Lock(); defer peerWarnMu.Unlock(); return len(peerWarnLast) }
	warnPeer(p, nil, "x", nil)
	first := peerWarnLast["dead"]
	e.advance(10 * time.Second)
	warnPeer(p, nil, "x", nil)
	if !peerWarnLast["dead"].Equal(first) {
		t.Error("warned again inside 30 s")
	}
	e.advance(25 * time.Second)
	warnPeer(p, nil, "x", nil)
	if peerWarnLast["dead"].Equal(first) {
		t.Error("did not warn after 30 s")
	}
	warnPeer(PeerHealth{NodeID: "other"}, nil, "x", nil)
	if count() != 2 {
		t.Error("another peer shares the limit")
	}
}

var _ = http.StatusOK
