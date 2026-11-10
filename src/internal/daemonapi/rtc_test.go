package daemonapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/rtctransport"
)

// rtcPeer is a daemon with WebRTC on: its routes, signalling included, at an
// httptest server, and its API over the data channels that signalling opens.
// overRTC counts the requests that arrived on a data channel.
type rtcPeer struct {
	signal  *httptest.Server
	overRTC atomic.Int64
	remote  atomic.Value
}

func newRTCPeer(t *testing.T) *rtcPeer {
	t.Helper()
	s := nodeTestServer(t, "peer-b")
	s.EnableWebRTC(rtctransport.Config{})
	p := &rtcPeer{signal: httptest.NewServer(s.Handler())}
	t.Cleanup(p.signal.Close)
	h := s.Handler()
	go http.Serve(s.rtcListener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.overRTC.Add(1)
		p.remote.Store(r.RemoteAddr)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { s.rtcListener.Close() })
	return p
}

func TestWebRTCTransportReachesPeerOverDataChannel(t *testing.T) {
	p := newRTCPeer(t)
	client := &http.Client{Transport: WebRTCTransport(rtctransport.Config{}, dialTo(p.signal))}

	resp, err := client.Get("http://peer-b.invalid/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if p.overRTC.Load() == 0 {
		t.Fatal("request succeeded but did not travel over a WebRTC data channel")
	}
	if remote := p.remote.Load().(string); isLoopback(remote) {
		t.Fatalf("a data channel signalled over loopback reads as loopback (%s)", remote)
	}
}

// Signalling is a peer route like any other: without the token an offer is
// refused, and with it the data channel carries the token too.
func TestWebRTCSignallingNeedsTheToken(t *testing.T) {
	p := newRTCPeer(t)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	if err := authtoken.Set("s3cret"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := authtoken.Set(""); err != nil {
			t.Errorf("clearing the test token: %v", err)
		}
	})

	resp, err := http.Post(p.signal.URL+rtctransport.SignalPath, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("offer without the token got %d, want 401", resp.StatusCode)
	}

	client := &http.Client{Transport: WebRTCTransport(rtctransport.Config{}, authtoken.Wrap(dialTo(p.signal)))}
	resp, err = client.Get("http://peer-b.invalid/v1/reserved")
	if err != nil {
		t.Fatalf("GET /v1/reserved: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	if p.overRTC.Load() == 0 {
		t.Fatal("request succeeded but did not travel over a WebRTC data channel")
	}
}

// Only daemon API calls go over WebRTC: anything else in the process may be
// for a host that is no daemon and has no signalling route.
func TestWebRTCTransportLeavesOtherCallsAlone(t *testing.T) {
	var hits atomic.Int64
	srv := fallbackServer(t, &hits)
	client := &http.Client{Transport: WebRTCTransport(rtctransport.Config{}, dialTo(srv))}

	resp, err := client.Get("http://example.invalid/some/file")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if hits.Load() != 1 {
		t.Fatalf("plain call reached its host %d time(s), want 1", hits.Load())
	}
}

func TestSignalRouteAbsentByDefault(t *testing.T) {
	s := nodeTestServer(t, "plain")
	if s.rtcListener != nil {
		t.Fatal("rtc listener allocated with webrtc disabled")
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, rtctransport.SignalPath, strings.NewReader("{}")))
	if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST %s = %d, want 404/405", rtctransport.SignalPath, rec.Code)
	}
}
