package daemonapi

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/rtctransport"
)

// dialTo is a transport whose every connection goes to srv, whatever host the
// request names, so two transports can stand for two routes to one peer.
func dialTo(srv *httptest.Server) *http.Transport {
	return &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
		},
	}
}

func fallbackServer(t *testing.T, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		hits.Add(1)
		io.WriteString(w, "from fallback")
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A request whose connection could not even be opened never left this
// machine, so it is safe to send over plain HTTP instead, POST or not.
func TestFallbackReplaysWhenDialFailed(t *testing.T) {
	var fallbackHits atomic.Int64
	primary := markDialFailures(&http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("no route over webrtc")
		},
	})
	client := &http.Client{Transport: &fallbackTransport{primary: primary, fallback: dialTo(fallbackServer(t, &fallbackHits))}}

	resp, err := client.Post("http://peer.invalid/v1/pool/map", "application/json", strings.NewReader(`{"items":[1]}`))
	if err != nil {
		t.Fatalf("POST after a failed dial: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "from fallback" || fallbackHits.Load() != 1 {
		t.Fatalf("body %q, fallback hit %d time(s), want one replay", body, fallbackHits.Load())
	}
}

// A peer lost after the request was written may already have acted on it. A
// POST is then not sent again; a GET, which changes nothing, still is.
func TestFallbackDoesNotReplaySentPost(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int64
	lost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		primaryHits.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}))
	defer lost.Close()
	client := &http.Client{Transport: &fallbackTransport{
		primary:  markDialFailures(dialTo(lost)),
		fallback: dialTo(fallbackServer(t, &fallbackHits)),
	}}

	resp, err := client.Post("http://peer.invalid/v1/pool/map", "application/json", strings.NewReader(`{"items":[1]}`))
	if err == nil {
		resp.Body.Close()
		t.Fatal("a POST whose peer was lost after sending returned no error")
	}
	if primaryHits.Load() != 1 || fallbackHits.Load() != 0 {
		t.Fatalf("POST reached the peer %d time(s) and was replayed %d time(s), want 1 and 0", primaryHits.Load(), fallbackHits.Load())
	}

	resp, err = client.Get("http://peer.invalid/health")
	if err != nil {
		t.Fatalf("GET after a lost peer: %v", err)
	}
	resp.Body.Close()
	if fallbackHits.Load() != 1 {
		t.Fatalf("GET was replayed %d time(s), want 1", fallbackHits.Load())
	}
}

// Every offer builds a peer connection, so only so many may be in negotiation
// at once. The rest are told to come back.
func TestSignalEndpointLimitsPendingOffers(t *testing.T) {
	entered := make(chan struct{}, maxPendingOffers+1)
	release := make(chan struct{})
	limited := limitPending(maxPendingOffers, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
	}))
	srv := httptest.NewServer(limited)
	defer srv.Close()

	var wg sync.WaitGroup
	for i := 0; i < maxPendingOffers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if resp, err := http.Post(srv.URL+rtctransport.SignalPath, "application/json", strings.NewReader("{}")); err == nil {
				resp.Body.Close()
			}
		}()
	}
	for i := 0; i < maxPendingOffers; i++ {
		<-entered
	}

	resp, err := http.Post(srv.URL+rtctransport.SignalPath, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("offer past the limit got %d (Retry-After %q), want 429", resp.StatusCode, resp.Header.Get("Retry-After"))
	}

	close(release)
	wg.Wait()
	resp, err = http.Post(srv.URL+rtctransport.SignalPath, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("offer after the others finished got %d, want 200", resp.StatusCode)
	}
}
