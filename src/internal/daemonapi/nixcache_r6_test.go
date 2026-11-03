package daemonapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
)

// H1: a busy peer slot waits (nix asks 16 NARs at once) and only then 503s.
func TestPeerSlotWaits(t *testing.T) {
	s := &Server{}
	e := &peerEnv{globOut: []string{peerBashPath}, infoOut: [][]byte{peerFixture(t)}, dumpData: []byte("x")}
	e.install(t)
	old := peerSlotWait
	peerSlotWait = 2 * time.Second
	t.Cleanup(func() { peerSlotWait = old })
	for i := 0; i < maxPeerStoreJobs; i++ {
		peerNarSem <- struct{}{}
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		<-peerNarSem
	}()
	rec := narReq(s, peerBashHash)
	for i := 0; i < maxPeerStoreJobs-1; i++ {
		<-peerNarSem
	}
	if rec.Code != 200 {
		t.Errorf("a slot freed within the wait: %d, want 200", rec.Code)
	}
}

// H1/H2: peer answers 503 -> nix gets 503 (retried, not cached), never a 404.
func TestBusyPeerIsNot404(t *testing.T) {
	good := ncNarinfo(t)
	busy := newNCPeer(t, 503, "busy", 0, 503, "busy")
	e := newNCEnv(t, true, ncPeers(t, busy))
	if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 503 {
		t.Errorf("narinfo from a busy peer: %d, want 503", w.Code)
	}
	// Opposite: a peer that says 404 is a real miss.
	none := newNCPeer(t, 404, "", 0, 404, "")
	e2 := newNCEnv(t, true, ncPeers(t, none))
	if w := e2.do("GET", "/"+ncHash+".narinfo"); w.Code != 404 {
		t.Errorf("narinfo miss: %d, want 404", w.Code)
	}
	// 503 is not remembered: the next ask goes to the peer again.
	n := busy.calls.Load()
	e.do("GET", "/"+ncHash+".narinfo")
	if busy.calls.Load() == n {
		t.Error("a busy answer was remembered")
	}
	// NAR: every source busy -> 503; every source 500 -> 404 (as before).
	nb := newNCPeer(t, 200, good, 0, 503, "busy")
	e3 := newNCEnv(t, true, ncPeers(t, nb))
	e3.do("GET", "/"+ncHash+".narinfo")
	if w := e3.do("GET", "/nar/"+ncHash+".nar.zst"); w.Code != 503 {
		t.Errorf("NAR from a busy peer: %d, want 503", w.Code)
	}
	nf := newNCPeer(t, 200, good, 0, 500, "")
	e4 := newNCEnv(t, true, ncPeers(t, nf))
	e4.do("GET", "/"+ncHash+".narinfo")
	if w := e4.do("GET", "/nar/"+ncHash+".nar.zst"); w.Code != 404 {
		t.Errorf("NAR from a failing peer: %d, want 404", w.Code)
	}
}

// H1: over the fan-out cap a lookup waits for a slot.
func TestFanOutWaitsForSlot(t *testing.T) {
	good := ncNarinfo(t)
	a := newNCPeer(t, 200, good, 0, 200, "")
	e := newNCEnv(t, true, ncPeers(t, a))
	old := fanOutWait
	fanOutWait = 2 * time.Second
	t.Cleanup(func() { fanOutWait = old })
	for i := 0; i < maxFanOuts; i++ {
		fanOutSem <- struct{}{}
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		<-fanOutSem
	}()
	w := e.do("GET", "/"+ncHash+".narinfo")
	for i := 0; i < maxFanOuts-1; i++ {
		<-fanOutSem
	}
	if w.Code != 200 {
		t.Errorf("slot freed within the wait: %d, want 200", w.Code)
	}
}

// M5: a page's cross-origin request is not the local nix.
func TestLoopbackExemptionRejectsBrowserRequests(t *testing.T) {
	if err := authtoken.Set("secret"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authtoken.Set("") })
	h := exceptLocalNixCache(authtoken.Middleware)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	try := func(hdr map[string]string) int {
		r := httptest.NewRequest("GET", "/nix-cache/"+ncHash+".narinfo", nil)
		r.RemoteAddr, r.Host = "127.0.0.1:5555", "127.0.0.1:4242"
		for k, v := range hdr {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if got := try(map[string]string{"Sec-Fetch-Site": "cross-site"}); got != 401 {
		t.Errorf("Sec-Fetch-Site cross-site: %d, want 401", got)
	}
	if got := try(map[string]string{"Origin": "http://evil.example"}); got != 401 {
		t.Errorf("Origin: %d, want 401", got)
	}
	if got := try(nil); got != 200 {
		t.Errorf("plain nix request: %d, want 200", got)
	}
}

// M3: one node under several addresses is asked on its LAN address.
func TestCachePeersPrefersLAN(t *testing.T) {
	for i := 0; i < 20; i++ { // map order is random
		e := newNCEnv(t, true, map[string]*PeerHealth{
			"a": {Host: "127.0.0.1", Port: 9001, NodeID: "N1", Status: "healthy", Arch: "x86_64-linux"},
			"b": {Host: "192.168.1.5", Port: 4242, NodeID: "N1", Status: "healthy", Arch: "x86_64-linux"},
			"c": {Host: "203.0.113.9", Port: 4242, NodeID: "N1", Status: "healthy", Arch: "x86_64-linux"},
		})
		got := e.s.cachePeers()
		if len(got) != 1 || got[0].Host != "192.168.1.5" {
			t.Fatalf("picked %+v, want the private address", got)
		}
	}
	// Opposite: with only the forwarder address, it is used.
	e := newNCEnv(t, true, map[string]*PeerHealth{"a": {Host: "127.0.0.1", Port: 9001, NodeID: "N1", Status: "healthy", Arch: "x86_64-linux"}})
	if got := e.s.cachePeers(); len(got) != 1 {
		t.Errorf("forwarder-only node dropped: %+v", got)
	}
}

// M4: a slower source with the same NarHash joins after the winner answered,
// and the winner does not hold the fan-out slot while it waits.
func TestLateSourceJoinsWithoutHoldingSlot(t *testing.T) {
	good := ncNarinfo(t)
	a := newNCPeer(t, 200, good, 0, 500, "")
	b := newNCPeer(t, 200, good, 400*time.Millisecond, 200, "FROMB")
	e := newNCEnv(t, true, ncPeers(t, a, b))
	start := time.Now()
	if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code != 200 {
		t.Fatalf("narinfo %d", w.Code)
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Error("the winner waited for the slow peer")
	}
	if len(fanOutSem) != 0 {
		t.Errorf("fan-out slot still held: %d", len(fanOutSem))
	}
	time.Sleep(600 * time.Millisecond)
	if w := e.do("GET", "/nar/"+ncHash+".nar.zst"); w.Code != 200 || w.Body.String() != "FROMB" {
		t.Errorf("late source not used: %d %q", w.Code, w.Body.String())
	}
}

// M6: a NAR stream that breaks mid-body drops that source.
func TestBrokenStreamDropsSource(t *testing.T) {
	good := ncNarinfo(t)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/store/narinfo/") {
			_, _ = w.Write([]byte(good))
			return
		}
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("short"))
	}))
	t.Cleanup(bad.Close)
	hp := bad.Listener.Addr().String()
	e := newNCEnv(t, true, map[string]*PeerHealth{"x": {Host: "127.0.0.1", Port: mustPort(t, hp), NodeID: "BAD", Status: "healthy", Arch: "x86_64-linux"}})
	e.do("GET", "/"+ncHash+".narinfo")
	e.do("GET", "/nar/"+ncHash+".nar.zst")
	e.s.peersMu.Lock()
	e.s.peerHealths = nil
	e.s.peersMu.Unlock()
	if w := e.do("GET", "/"+ncHash+".narinfo"); w.Code == 200 {
		t.Error("the broken source's hit is still served")
	}
}
