package daemonapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/klauspost/compress/zstd"
	"github.com/nix-community/go-nix/pkg/narinfo"
)

const (
	peerBashHash = "byi2zpy2bgcf3dr6y0l8m50rmjj8z7q1"
	peerBashPath = "/nix/store/" + peerBashHash + "-bash-5.3p15"
)

var sigsRe = regexp.MustCompile(`"signatures":\[[^\]]*\]`)

func peerFixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../peercache/testdata/pathinfo-1.1.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type peerEnv struct {
	globOut     []string
	infoCalls   int32
	infoOut     [][]byte // successive path-info answers; the last repeats
	infoErr     error
	signCalls   int32
	signErr     error
	dumpData    []byte
	dumpErr     error
	dumpRunning int32
	dumpMax     int32
	dumpGate    chan struct{}
}

func (e *peerEnv) install(t *testing.T) {
	t.Helper()
	oldG, oldP, oldD, oldS := storeGlob, pathInfoCmd, dumpCmd, ensureSigned
	t.Cleanup(func() { storeGlob, pathInfoCmd, dumpCmd, ensureSigned = oldG, oldP, oldD, oldS })
	storeGlob = func(string) ([]string, error) { return e.globOut, nil }
	pathInfoCmd = func(context.Context, string) ([]byte, error) {
		n := int(atomic.AddInt32(&e.infoCalls, 1)) - 1
		if e.infoErr != nil {
			return nil, e.infoErr
		}
		if n >= len(e.infoOut) {
			n = len(e.infoOut) - 1
		}
		return e.infoOut[n], nil
	}
	ensureSigned = func(context.Context, []string) error {
		atomic.AddInt32(&e.signCalls, 1)
		return e.signErr
	}
	dumpCmd = func(ctx context.Context, path string) (io.ReadCloser, func() error, error) {
		if e.dumpErr != nil {
			return nil, nil, e.dumpErr
		}
		cur := atomic.AddInt32(&e.dumpRunning, 1)
		for {
			m := atomic.LoadInt32(&e.dumpMax)
			if cur <= m || atomic.CompareAndSwapInt32(&e.dumpMax, m, cur) {
				break
			}
		}
		if e.dumpGate != nil {
			<-e.dumpGate
		}
		return io.NopCloser(bytes.NewReader(e.dumpData)), func() error {
			atomic.AddInt32(&e.dumpRunning, -1)
			return nil
		}, nil
	}
}

func peerGetRoute(s *Server, h http.HandlerFunc, route, hash string) *httptest.ResponseRecorder {
	r := chi.NewRouter()
	r.Get(route, h)
	req := httptest.NewRequest(http.MethodGet, strings.Replace(route, "{hash}", hash, 1), nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func narinfoReq(s *Server, hash string) *httptest.ResponseRecorder {
	return peerGetRoute(s, s.handlePeerNarinfo, "/v1/store/narinfo/{hash}", hash)
}

func narReq(s *Server, hash string) *httptest.ResponseRecorder {
	return peerGetRoute(s, s.handlePeerNar, "/v1/store/nar/{hash}", hash)
}

func TestPeerNarinfo(t *testing.T) {
	fx := peerFixture(t)
	unsigned := sigsRe.ReplaceAll(fx, []byte(`"signatures":[]`))
	signed := bytes.Replace(unsigned, []byte(`"signatures":[]`), []byte(`"signatures":["peer-1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="]`), 1)
	if bytes.Equal(unsigned, fx) {
		t.Fatal("fixture has no signatures field to strip")
	}
	s := &Server{}

	t.Run("1 one glob hit", func(t *testing.T) {
		e := &peerEnv{globOut: []string{"/nix/store/" + peerBashHash + "-bash-5.3p15"}, infoOut: [][]byte{fx}}
		e.install(t)
		rec := narinfoReq(s, peerBashHash)
		if rec.Code != 200 {
			t.Fatalf("code %d: %s", rec.Code, rec.Body)
		}
		ni, err := narinfo.Parse(rec.Body)
		if err != nil {
			t.Fatal(err)
		}
		if ni.StorePath != peerBashPath || len(ni.Signatures) == 0 || ni.Signatures[0].Name != "cache.nixos.org-1" {
			t.Fatalf("narinfo wrong: %+v", ni)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "text/x-nix-narinfo" {
			t.Fatalf("content type %q", ct)
		}
	})
	t.Run("2 drv only", func(t *testing.T) {
		e := &peerEnv{globOut: []string{"/nix/store/" + peerBashHash + "-x.drv"}, infoOut: [][]byte{fx}}
		e.install(t)
		if rec := narinfoReq(s, peerBashHash); rec.Code != 404 || e.infoCalls != 0 {
			t.Fatalf("code %d calls %d", rec.Code, e.infoCalls)
		}
	})
	t.Run("3 lock skipped", func(t *testing.T) {
		e := &peerEnv{globOut: []string{
			"/nix/store/" + peerBashHash + "-bash-5.3p15.lock",
			"/nix/store/" + peerBashHash + "-bash-5.3p15",
		}, infoOut: [][]byte{fx}}
		e.install(t)
		if rec := narinfoReq(s, peerBashHash); rec.Code != 200 {
			t.Fatalf("code %d", rec.Code)
		}
	})
	t.Run("4 no glob hit", func(t *testing.T) {
		e := &peerEnv{infoOut: [][]byte{fx}}
		e.install(t)
		if rec := narinfoReq(s, peerBashHash); rec.Code != 404 || e.infoCalls != 0 {
			t.Fatalf("code %d", rec.Code)
		}
	})
	t.Run("5 bad hash", func(t *testing.T) {
		e := &peerEnv{infoOut: [][]byte{fx}}
		e.install(t)
		if rec := narinfoReq(s, "abc"); rec.Code != 400 {
			t.Fatalf("code %d", rec.Code)
		}
	})
	t.Run("6 path-info fails", func(t *testing.T) {
		e := &peerEnv{globOut: []string{peerBashPath}, infoErr: errors.New("exit status 1")}
		e.install(t)
		if rec := narinfoReq(s, peerBashHash); rec.Code != 404 {
			t.Fatalf("code %d", rec.Code)
		}
	})
	t.Run("7 unsigned then signed", func(t *testing.T) {
		e := &peerEnv{globOut: []string{peerBashPath}, infoOut: [][]byte{unsigned, signed}}
		e.install(t)
		rec := narinfoReq(s, peerBashHash)
		if rec.Code != 200 || e.signCalls != 1 || !strings.Contains(rec.Body.String(), "Sig: peer-1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==") {
			t.Fatalf("code %d signs %d body %s", rec.Code, e.signCalls, rec.Body)
		}
	})
	t.Run("8 signing fails", func(t *testing.T) {
		e := &peerEnv{globOut: []string{peerBashPath}, infoOut: [][]byte{unsigned}, signErr: errors.New("no key")}
		e.install(t)
		rec := narinfoReq(s, peerBashHash)
		if rec.Code != 200 || strings.Contains(rec.Body.String(), "Sig:") || e.signCalls != 1 {
			t.Fatalf("code %d signs %d body %s", rec.Code, e.signCalls, rec.Body)
		}
	})
}

func TestPeerNar(t *testing.T) {
	s := &Server{}
	want := bytes.Repeat([]byte("nar-bytes "), 1000)

	t.Run("9 zstd body", func(t *testing.T) {
		e := &peerEnv{globOut: []string{peerBashPath}, dumpData: want}
		e.install(t)
		rec := narReq(s, peerBashHash)
		if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/zstd" {
			t.Fatalf("code %d type %q", rec.Code, rec.Header().Get("Content-Type"))
		}
		zr, err := zstd.NewReader(rec.Body)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(zr)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("decode: err %v, equal %v", err, bytes.Equal(got, want))
		}
	})
	t.Run("10 dump fails to start", func(t *testing.T) {
		e := &peerEnv{globOut: []string{peerBashPath}, dumpErr: errors.New("no nix-store")}
		e.install(t)
		if rec := narReq(s, peerBashHash); rec.Code != 500 {
			t.Fatalf("code %d", rec.Code)
		}
	})
	t.Run("11 at most 8 dumps", func(t *testing.T) {
		e := &peerEnv{globOut: []string{peerBashPath}, dumpData: []byte("x"), dumpGate: make(chan struct{})}
		e.install(t)
		var wg sync.WaitGroup
		for i := 0; i < 9; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); narReq(s, peerBashHash) }()
		}
		deadline := time.Now().Add(2 * time.Second)
		for atomic.LoadInt32(&e.dumpRunning) < maxPeerStoreJobs && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond) // give the 9th a chance to wrongly start
		if got := atomic.LoadInt32(&e.dumpRunning); got != maxPeerStoreJobs {
			t.Fatalf("running %d, want %d", got, maxPeerStoreJobs)
		}
		close(e.dumpGate)
		wg.Wait()
		if e.dumpMax > maxPeerStoreJobs {
			t.Fatalf("max concurrent %d", e.dumpMax)
		}
	})
	t.Run("bad hash", func(t *testing.T) {
		e := &peerEnv{}
		e.install(t)
		if rec := narReq(s, "../x"); rec.Code != 400 && rec.Code != 404 {
			t.Fatalf("code %d", rec.Code)
		}
	})
}

// Spec: a full semaphore answers 503 at once (not a wait), and the two
// routes have separate slots.
func TestPeerStoreSemaphoreFull(t *testing.T) {
	s := &Server{}
	e := &peerEnv{globOut: []string{peerBashPath}, infoOut: [][]byte{peerFixture(t)}, dumpData: []byte("x")}
	e.install(t)
	fill := func(sem chan struct{}) func() {
		for i := 0; i < maxPeerStoreJobs; i++ {
			sem <- struct{}{}
		}
		return func() {
			for i := 0; i < maxPeerStoreJobs; i++ {
				<-sem
			}
		}
	}
	done := fill(peerNarSem)
	if rec := narReq(s, peerBashHash); rec.Code != 503 {
		t.Errorf("NAR with all NAR slots taken: %d", rec.Code)
	}
	if rec := narinfoReq(s, peerBashHash); rec.Code == 503 {
		t.Error("a full NAR pool starved the narinfo route")
	}
	done()
	done = fill(peerInfoSem)
	if rec := narinfoReq(s, peerBashHash); rec.Code != 503 {
		t.Errorf("narinfo with all slots taken: %d", rec.Code)
	}
	if rec := narReq(s, peerBashHash); rec.Code != 200 {
		t.Errorf("NAR with free NAR slots: %d", rec.Code)
	}
	done()
}
