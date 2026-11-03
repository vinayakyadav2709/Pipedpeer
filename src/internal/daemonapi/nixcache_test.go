package daemonapi

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/peercache"
)

const (
	ncHash   = "byi2zpy2bgcf3dr6y0l8m50rmjj8z7q1"
	ncOther  = "0d8g8n0a11v6f5m2h416ajyxmnkwc3md"
	ncPubKey = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="
)

func ncNarinfo(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../peercache/testdata/narinfo-2.1.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// ncPeer is a fake peer daemon.
type ncPeer struct {
	srv    *httptest.Server
	calls  atomic.Int32
	narReq atomic.Int32
}

// newNCPeer serves narinfo with status/body (delay first; a negative delay
// hangs until the request is cancelled) and nar with narStatus/narBody.
func newNCPeer(t *testing.T, status int, body string, delay time.Duration, narStatus int, narBody string) *ncPeer {
	p := &ncPeer{}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/store/narinfo/"):
			p.calls.Add(1)
			if delay < 0 {
				<-r.Context().Done()
				return
			}
			time.Sleep(delay)
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		case strings.HasPrefix(r.URL.Path, "/v1/store/nar/"):
			p.narReq.Add(1)
			w.WriteHeader(narStatus)
			_, _ = w.Write([]byte(narBody))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(p.srv.Close)
	return p
}

type ncEnv struct {
	s      *Server
	h      http.Handler
	now    *time.Time
	nowMu  sync.Mutex
	system string
}

func newNCEnv(t *testing.T, trust bool, peers map[string]*PeerHealth) *ncEnv {
	t.Helper()
	e := &ncEnv{system: "x86_64-linux"}
	n := time.Now()
	e.now = &n
	oldNow, oldM, oldS, oldK := cacheNow, localMachine, localSystem, trustedKeys
	cacheNow = func() time.Time { e.nowMu.Lock(); defer e.nowMu.Unlock(); return *e.now }
	localMachine = func() string { return "m-local" }
	localSystem = func() string { return e.system }
	trustedKeys = func() []signature.PublicKey {
		if !trust {
			return nil
		}
		k, err := signature.ParsePublicKey(ncPubKey)
		if err != nil {
			t.Fatal(err)
		}
		return []signature.PublicKey{k}
	}
	t.Cleanup(func() { cacheNow, localMachine, localSystem, trustedKeys = oldNow, oldM, oldS, oldK })
	e.s = New("self")
	e.s.peersMu.Lock()
	e.s.peerHealths = peers
	e.s.peersMu.Unlock()
	e.h = e.s.nixCacheRouter()
	return e
}

func (e *ncEnv) advance(d time.Duration) {
	e.nowMu.Lock()
	*e.now = e.now.Add(d)
	e.nowMu.Unlock()
}

func (e *ncEnv) do(method, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	r.RemoteAddr = "127.0.0.1:5555"
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	return w
}

func ncHealth(t *testing.T, id string, p *ncPeer, mod func(*PeerHealth)) *PeerHealth {
	hostport := strings.TrimPrefix(p.srv.URL, "http://")
	ph := &PeerHealth{Host: strings.Split(hostport, ":")[0], Port: mustPort(t, hostport), NodeID: id,
		Status: "healthy", Arch: "x86_64-linux"}
	if mod != nil {
		mod(ph)
	}
	return ph
}

func ncPeers(t *testing.T, ps ...*ncPeer) map[string]*PeerHealth {
	m := map[string]*PeerHealth{}
	for i, p := range ps {
		id := string(rune('A' + i))
		m[id] = ncHealth(t, id, p, nil)
	}
	return m
}

func TestNixCacheInfoAndAuth(t *testing.T) {
	// 1: loopback, no token.
	e := newNCEnv(t, true, nil)
	if w := e.do("GET", "/nix-cache-info"); w.Code != 200 || w.Body.String() != peercache.CacheInfo {
		t.Fatalf("info: %d %q", w.Code, w.Body.String())
	}

	if err := authtoken.Set("secret"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authtoken.Set("") })
	var served atomic.Int32
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { served.Add(1) })
	h := exceptLocalNixCache(authtoken.Middleware)(next)
	tryHost := func(method, path, remote, host string) int {
		r := httptest.NewRequest(method, path, nil)
		r.RemoteAddr = remote
		r.Host = host
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	try := func(method, path, remote string) int { return tryHost(method, path, remote, "127.0.0.1:4242") }
	// 2: not loopback.
	if c := try("GET", "/nix-cache/"+ncHash+".narinfo", "192.168.1.5:1"); c != 401 {
		t.Errorf("remote: %d", c)
	}
	// 3: a write from loopback is refused as read-only (spec 3.6); from
	// elsewhere it meets the token check.
	if c := try("PUT", "/nix-cache/"+ncHash+".narinfo", "127.0.0.1:1"); c != 405 {
		t.Errorf("put: %d", c)
	}
	if c := try("PUT", "/nix-cache/"+ncHash+".narinfo", "192.168.1.5:1"); c != 401 {
		t.Errorf("remote put: %d", c)
	}
	// 3b: DNS rebinding presents a foreign Host from loopback.
	if c := tryHost("GET", "/nix-cache/nix-cache-info", "127.0.0.1:1", "evil.example:4242"); c != 401 {
		t.Errorf("rebound host: %d", c)
	}
	// 4: other routes keep their auth; so does a traversal out of the prefix.
	if c := try("GET", "/v1/nodes", "127.0.0.1:1"); c != 401 {
		t.Errorf("nodes: %d", c)
	}
	if c := try("GET", "/nix-cache/../v1/nodes", "127.0.0.1:1"); c != 401 {
		t.Errorf("traversal: %d", c)
	}
	if served.Load() != 0 {
		t.Errorf("served %d guarded requests", served.Load())
	}
	// and the exemption itself, plus HEAD.
	if c := try("GET", "/nix-cache/nix-cache-info", "127.0.0.1:1"); c != 200 {
		t.Errorf("exempt GET: %d", c)
	}
	if c := try("HEAD", "/nix-cache/"+ncHash+".narinfo", "[::1]:1"); c != 200 {
		t.Errorf("exempt HEAD: %d", c)
	}
	if c := tryHost("GET", "/nix-cache/nix-cache-info", "127.0.0.1:1", "localhost:4242"); c != 200 {
		t.Errorf("localhost host: %d", c)
	}
}

func TestNixCacheFanOut(t *testing.T) {
	good := ncNarinfo(t)
	t.Run("5 first 404 second valid", func(t *testing.T) {
		a := newNCPeer(t, 404, "", 0, 200, "")
		b := newNCPeer(t, 200, good, 0, 200, "")
		e := newNCEnv(t, true, ncPeers(t, a, b))
		w := e.do("GET", "/"+ncHash+".narinfo")
		if w.Code != 200 || w.Body.String() != good {
			t.Fatalf("%d %q", w.Code, w.Body.String())
		}
	})
	t.Run("6 untrusted signature loses to valid", func(t *testing.T) {
		// A serves a narinfo with a bad Sig line; B the real one.
		bad := strings.Replace(good, "Sig: cache.nixos.org-1:L1O9", "Sig: cache.nixos.org-1:L1O8", 1)
		a := newNCPeer(t, 200, bad, 0, 200, "")
		b := newNCPeer(t, 200, good, 50*time.Millisecond, 200, "")
		e := newNCEnv(t, true, ncPeers(t, a, b))
		if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 200 || w.Body.String() != good {
			t.Fatalf("%d", w.Code)
		}
	})
	t.Run("6b no trusted key at all", func(t *testing.T) {
		a := newNCPeer(t, 200, good, 0, 200, "")
		e := newNCEnv(t, false, ncPeers(t, a))
		if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 404 {
			t.Fatalf("%d", w.Code)
		}
	})
	t.Run("7 narinfo for another hash", func(t *testing.T) {
		a := newNCPeer(t, 200, good, 0, 200, "")
		e := newNCEnv(t, true, ncPeers(t, a))
		if w := e.do("GET", "/"+ncOther+".narinfo"); w.Code != 404 {
			t.Fatalf("%d", w.Code)
		}
	})
	t.Run("8-9 all 404 remembered 30s", func(t *testing.T) {
		a := newNCPeer(t, 404, "", 0, 200, "")
		b := newNCPeer(t, 404, "", 0, 200, "")
		e := newNCEnv(t, true, ncPeers(t, a, b))
		for i := 0; i < 2; i++ {
			if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 404 {
				t.Fatal(w.Code)
			}
		}
		if a.calls.Load() != 1 || b.calls.Load() != 1 {
			t.Fatalf("calls %d %d", a.calls.Load(), b.calls.Load())
		}
		e.advance(31 * time.Second)
		e.do("GET", "/"+ncHash+".narinfo")
		if a.calls.Load() != 2 || b.calls.Load() != 2 {
			t.Fatalf("after 31s: calls %d %d", a.calls.Load(), b.calls.Load())
		}
	})
	t.Run("10 hanging peer is cancelled", func(t *testing.T) {
		a := newNCPeer(t, 200, good, -1, 200, "")
		b := newNCPeer(t, 200, good, 100*time.Millisecond, 200, "")
		e := newNCEnv(t, true, ncPeers(t, a, b))
		start := time.Now()
		w := e.do("GET", "/"+ncHash+".narinfo")
		if d := time.Since(start); w.Code != 200 || d > 400*time.Millisecond {
			t.Fatalf("%d after %v", w.Code, d)
		}
	})
	t.Run("11 all hang", func(t *testing.T) {
		a := newNCPeer(t, 200, good, -1, 200, "")
		e := newNCEnv(t, true, ncPeers(t, a))
		start := time.Now()
		w := e.do("GET", "/"+ncHash+".narinfo")
		d := time.Since(start)
		if w.Code != 404 || d < 2900*time.Millisecond || d > 4*time.Second {
			t.Fatalf("%d after %v", w.Code, d)
		}
		// a timeout is not a miss: asked again at once
		e.do("GET", "/"+ncHash+".narinfo")
		if a.calls.Load() != 2 {
			t.Fatalf("timeout was remembered as a miss: %d calls", a.calls.Load())
		}
	})
	t.Run("12 singleflight", func(t *testing.T) {
		a := newNCPeer(t, 200, good, 100*time.Millisecond, 200, "")
		b := newNCPeer(t, 404, "", 100*time.Millisecond, 200, "")
		e := newNCEnv(t, true, ncPeers(t, a, b))
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 200 {
					t.Errorf("code %d", w.Code)
				}
			}()
		}
		wg.Wait()
		if a.calls.Load() != 1 || b.calls.Load() > 1 {
			t.Fatalf("calls %d %d", a.calls.Load(), b.calls.Load())
		}
	})
	t.Run("18 bad hash", func(t *testing.T) {
		a := newNCPeer(t, 200, good, 0, 200, "")
		e := newNCEnv(t, true, ncPeers(t, a))
		if w := e.do("GET", "/"+ncHash[:31]+".narinfo"); w.Code != 400 {
			t.Fatal(w.Code)
		}
		if a.calls.Load() != 0 {
			t.Fatal("peer called")
		}
	})
	t.Run("19 no peers", func(t *testing.T) {
		e := newNCEnv(t, true, nil)
		if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 404 {
			t.Fatal(w.Code)
		}
	})
	t.Run("20 HEAD", func(t *testing.T) {
		a := newNCPeer(t, 200, good, 0, 200, "")
		e := newNCEnv(t, true, ncPeers(t, a))
		if w := e.do("HEAD", "/"+ncHash+".narinfo"); w.Code != 200 || w.Body.Len() != 0 {
			t.Fatalf("%d %d", w.Code, w.Body.Len())
		}
	})
}

func TestNixCachePeerSelection(t *testing.T) {
	good := ncNarinfo(t)
	self := newNCPeer(t, 200, good, 0, 200, "")
	sameMachine := newNCPeer(t, 200, good, 0, 200, "")
	foreign := newNCPeer(t, 200, good, 0, 200, "")
	oldPeer := newNCPeer(t, 200, good, 0, 200, "")
	sameArch := newNCPeer(t, 404, "", 0, 200, "")
	intro := newNCPeer(t, 200, good, 0, 200, "")
	phone := newNCPeer(t, 200, good, 0, 200, "")
	down := newNCPeer(t, 200, good, 0, 200, "")

	peers := map[string]*PeerHealth{
		"self": ncHealth(t, "self", self, nil),
		"mach": ncHealth(t, "mach", sameMachine, func(p *PeerHealth) { p.Machine = "m-local" }),
		"fore": ncHealth(t, "fore", foreign, func(p *PeerHealth) { p.Arch = "aarch64-linux" }),
		"old":  ncHealth(t, "old", oldPeer, func(p *PeerHealth) { p.Arch = "" }),
		"same": ncHealth(t, "same", sameArch, nil),
		"down": ncHealth(t, "down", down, func(p *PeerHealth) { p.Status = "unreachable" }),
	}
	e := newNCEnv(t, true, peers)
	if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 404 {
		t.Fatalf("13/14b/14c: %d", w.Code)
	}
	for name, p := range map[string]*ncPeer{"self": self, "same machine": sameMachine, "foreign arch": foreign, "old": oldPeer, "down": down} {
		if p.calls.Load() != 0 {
			t.Errorf("%s was asked", name)
		}
	}
	if sameArch.calls.Load() != 1 {
		t.Errorf("same-arch peer asked %d times", sameArch.calls.Load())
	}

	// 14, 14d: introducer-only and phone peers serve like any other.
	for name, p := range map[string]*ncPeer{"introducer": intro, "phone": phone} {
		e := newNCEnv(t, true, map[string]*PeerHealth{"x": ncHealth(t, "x", p, func(h *PeerHealth) {
			if name == "introducer" {
				h.Role = "introducer-only"
			}
		})})
		if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 200 {
			t.Errorf("%s: %d", name, w.Code)
		}
	}
}

func TestNixCacheNar(t *testing.T) {
	good := ncNarinfo(t)
	t.Run("15 streams the winner's bytes", func(t *testing.T) {
		b := newNCPeer(t, 200, good, 0, 200, "NARBYTES")
		e := newNCEnv(t, true, ncPeers(t, newNCPeer(t, 404, "", 0, 200, ""), b))
		e.do("GET", "/"+ncHash+".narinfo")
		w := e.do("GET", "/nar/"+ncHash+".nar.zst")
		if w.Code != 200 || w.Body.String() != "NARBYTES" {
			t.Fatalf("%d %q", w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/zstd" {
			t.Errorf("content type %q", ct)
		}
	})
	t.Run("16 falls to the next peer", func(t *testing.T) {
		b := newNCPeer(t, 200, good, 0, 500, "")
		c := newNCPeer(t, 200, good, 0, 200, "FROMC")
		e := newNCEnv(t, true, ncPeers(t, b, c))
		e.do("GET", "/"+ncHash+".narinfo")
		time.Sleep(200 * time.Millisecond) // the second peer's answer joins as a source
		w := e.do("GET", "/nar/"+ncHash+".nar.zst")
		if w.Code != 200 || w.Body.String() != "FROMC" {
			t.Fatalf("%d %q", w.Code, w.Body.String())
		}
	})
	t.Run("17 no remembered peers fans out first", func(t *testing.T) {
		b := newNCPeer(t, 200, good, 0, 200, "NARB")
		e := newNCEnv(t, true, ncPeers(t, b))
		if w := e.do("GET", "/nar/"+ncHash+".nar.zst"); w.Code != 200 || w.Body.String() != "NARB" {
			t.Fatalf("%d %q", w.Code, w.Body.String())
		}
		e2 := newNCEnv(t, true, ncPeers(t, newNCPeer(t, 404, "", 0, 200, "")))
		if w := e2.do("GET", "/nar/"+ncHash+".nar.zst"); w.Code != 404 {
			t.Fatalf("none: %d", w.Code)
		}
	})
	t.Run("bad nar name", func(t *testing.T) {
		e := newNCEnv(t, true, nil)
		if w := e.do("GET", "/nar/abc.nar.zst"); w.Code != 400 {
			t.Fatal(w.Code)
		}
	})
}
