package rtctransport

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testWait = 10 * time.Second

type pair struct {
	listener *Listener
	dialer   *Dialer
	addr     string
	offers   atomic.Int32
	// holdSignal, when non-nil, blocks every offer until it is closed.
	holdSignal chan struct{}
}

func newPair(t testing.TB) *pair {
	t.Helper()
	p := &pair{listener: NewListener(Config{}), dialer: NewDialer(Config{})}
	mux := http.NewServeMux()
	mux.Handle(SignalPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.offers.Add(1)
		if p.holdSignal != nil {
			select {
			case <-p.holdSignal:
			case <-r.Context().Done():
				return
			}
		}
		p.listener.SignalHandler().ServeHTTP(w, r)
	}))
	signal := httptest.NewServer(mux)
	p.addr = strings.TrimPrefix(signal.URL, "http://")
	t.Cleanup(func() {
		_ = p.dialer.Close()
		_ = p.listener.Close()
		signal.Close()
	})
	return p
}

// connPair returns the two ends of one data-channel connection.
func (p *pair) connPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	client, err := p.dialer.DialContext(ctx, "webrtc", p.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := p.listener.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	select {
	case server = <-accepted:
	case <-ctx.Done():
		t.Fatal("accept: timed out")
	}
	t.Cleanup(func() { _ = server.Close() })
	return client, server
}

func readResult(c net.Conn) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 16))
		done <- err
	}()
	return done
}

func TestReadDeadlineUnblocksRead(t *testing.T) {
	client, _ := newPair(t).connPair(t)
	if err := client.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readResult(client):
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("Read after deadline = %v, want a timeout net.Error", err)
		}
	case <-time.After(testWait):
		t.Fatal("Read ignored its deadline")
	}

	if err := client.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestReadWorksAfterDeadlineIsCleared(t *testing.T) {
	client, server := newPair(t).connPair(t)
	_ = client.SetReadDeadline(time.Now().Add(-time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read with an expired deadline succeeded")
	}
	_ = client.SetReadDeadline(time.Time{})
	if _, err := server.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(client, buf); err != nil || buf[0] != 'x' {
		t.Fatalf("Read after clearing the deadline = %q, %v", buf, err)
	}
}

// The peer here never reads, so it never answers the stream reset: only the
// local Close can wake the reader.
func TestCloseUnblocksPendingRead(t *testing.T) {
	client, _ := newPair(t).connPair(t)
	done := readResult(client)
	time.Sleep(50 * time.Millisecond)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Read after Close = %v, want net.ErrClosed", err)
		}
	case <-time.After(testWait):
		t.Fatal("Read still blocked after Close")
	}
}

func TestWriteBlocksUntilPeerReads(t *testing.T) {
	client, server := newPair(t).connPair(t)
	payload := make([]byte, 16<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	wrote := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		wrote <- err
	}()
	select {
	case err := <-wrote:
		t.Fatalf("Write of %d bytes returned (%v) while the peer read nothing: the sender buffered it all", len(payload), err)
	case <-time.After(time.Second):
	}

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatalf("peer read: %v", err)
	}
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
	case <-time.After(testWait):
		t.Fatal("Write still blocked after the peer read everything")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload corrupted in transit")
	}
}

func TestWriteDeadlineUnblocksBlockedWrite(t *testing.T) {
	client, _ := newPair(t).connPair(t)
	if err := client.SetWriteDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	wrote := make(chan error, 1)
	go func() {
		_, err := client.Write(make([]byte, 16<<20))
		wrote <- err
	}()
	select {
	case err := <-wrote:
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("Write past its deadline = %v, want a timeout net.Error", err)
		}
	case <-time.After(testWait):
		t.Fatal("Write ignored its deadline")
	}
}

func TestCloseUnblocksPendingWrite(t *testing.T) {
	client, _ := newPair(t).connPair(t)
	wrote := make(chan error, 1)
	go func() {
		_, err := client.Write(make([]byte, 16<<20))
		wrote <- err
	}()
	time.Sleep(200 * time.Millisecond)
	_ = client.Close()
	select {
	case err := <-wrote:
		if err == nil {
			t.Fatal("Write of 16 MiB to a peer that never reads reported success")
		}
	case <-time.After(testWait):
		t.Fatal("Write still blocked after Close")
	}
}

// net/http parks a server connection in a background read between requests
// and aborts it with a read deadline; without working deadlines the
// connection never becomes idle, so idle timeouts and graceful shutdown
// cannot see it.
func TestServerConnGoesIdleAfterResponse(t *testing.T) {
	p := newPair(t)
	idle := make(chan struct{}, 1)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}),
		ConnState: func(_ net.Conn, s http.ConnState) {
			if s == http.StateIdle {
				select {
				case idle <- struct{}{}:
				default:
				}
			}
		},
	}
	go func() { _ = srv.Serve(p.listener) }()

	tr := NewHTTPTransport(p.dialer)
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: testWait}
	resp, err := client.Get("http://" + p.addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	select {
	case <-idle:
	case <-time.After(3 * time.Second):
		t.Fatal("server connection never became idle after its response was sent")
	}
}

func TestDialSurvivesAnotherCallersCancellation(t *testing.T) {
	p := newPair(t)
	p.holdSignal = make(chan struct{})

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := p.dialer.DialContext(firstCtx, "webrtc", p.addr)
		first <- err
	}()
	for p.offers.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	second := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		c, err := p.dialer.DialContext(ctx, "webrtc", p.addr)
		if err == nil {
			_ = c.Close()
		}
		second <- err
	}()
	time.Sleep(50 * time.Millisecond)

	cancelFirst()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled dial = %v, want context.Canceled", err)
		}
	case <-time.After(testWait):
		t.Fatal("cancelled dial did not return")
	}
	close(p.holdSignal)
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("dial with a live context failed because another caller cancelled: %v", err)
		}
	case <-time.After(testWait):
		t.Fatal("second dial did not return")
	}
}

func TestDialHonoursItsOwnDeadlineWhileConnecting(t *testing.T) {
	p := newPair(t)
	p.holdSignal = make(chan struct{})
	defer close(p.holdSignal)

	go func() { _, _ = p.dialer.DialContext(context.Background(), "webrtc", p.addr) }()
	for p.offers.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := p.dialer.DialContext(ctx, "webrtc", p.addr)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("dial = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("dial waited on another caller's connect past its own deadline")
	}
}

func TestDialerCloseDoesNotWaitOutASlowConnect(t *testing.T) {
	p := newPair(t)
	p.holdSignal = make(chan struct{})
	defer close(p.holdSignal)

	dialed := make(chan error, 1)
	go func() {
		_, err := p.dialer.DialContext(context.Background(), "webrtc", p.addr)
		dialed <- err
	}()
	for p.offers.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	closed := make(chan struct{})
	go func() {
		_ = p.dialer.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked on a connect that was still signaling")
	}
	select {
	case err := <-dialed:
		if err == nil {
			t.Fatal("dial succeeded on a closed dialer")
		}
	case <-time.After(testWait):
		t.Fatal("dial did not return after Close")
	}
}

func TestPeerLossFailsRequestWaitingForHeaders(t *testing.T) {
	p := newPair(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	go func() {
		_ = http.Serve(p.listener, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(entered)
			<-release
		}))
	}()
	tr := NewHTTPTransport(p.dialer)
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr}

	done := make(chan error, 1)
	go func() {
		resp, err := client.Get("http://" + p.addr + "/")
		if err == nil {
			resp.Body.Close()
		}
		done <- err
	}()
	<-entered
	_ = p.listener.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("request succeeded although the peer connection was torn down before any response")
		}
	case <-time.After(testWait):
		t.Fatal("request hung after the peer connection was torn down")
	}
}

func TestPeerLossMidBodyIsAnError(t *testing.T) {
	for _, framing := range []string{"content-length", "chunked"} {
		t.Run(framing, func(t *testing.T) {
			p := newPair(t)
			// Well under streamWindow, so the handler is not held back by
			// a client that has not started reading the body.
			half := bytes.Repeat([]byte("x"), 64<<10)
			sent := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			go func() {
				_ = http.Serve(p.listener, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					if framing == "content-length" {
						w.Header().Set("Content-Length", strconv.Itoa(2*len(half)))
					}
					_, _ = w.Write(half)
					w.(http.Flusher).Flush()
					close(sent)
					<-release
				}))
			}()
			tr := NewHTTPTransport(p.dialer)
			t.Cleanup(tr.CloseIdleConnections)
			client := &http.Client{Transport: tr}

			resp, err := client.Get("http://" + p.addr + "/")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			<-sent
			_ = p.listener.Close()

			type result struct {
				n   int
				err error
			}
			done := make(chan result, 1)
			go func() {
				body, err := io.ReadAll(resp.Body)
				done <- result{len(body), err}
			}()
			select {
			case r := <-done:
				if r.err == nil {
					t.Fatalf("read %d of %d bytes and reported success", r.n, 2*len(half))
				}
			case <-time.After(testWait):
				t.Fatal("body read hung after the peer connection was torn down")
			}
		})
	}
}

// A server that answers and closes at once must not lose the tail of the
// response to the stream reset.
func TestResponseSurvivesImmediateServerClose(t *testing.T) {
	p := newPair(t)
	body := make([]byte, 4<<20)
	if _, err := rand.Read(body); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = http.Serve(p.listener, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Connection", "close")
			_, _ = w.Write(body)
		}))
	}()
	tr := NewHTTPTransport(p.dialer)
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}
	for range 3 {
		resp, err := client.Get("http://" + p.addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("got %d bytes, want %d", len(got), len(body))
		}
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }

// Every connection to a peer shares one SCTP receive window, so an upload
// whose handler never reads its body must not starve the others.
func TestStalledRequestDoesNotBlockOtherRequests(t *testing.T) {
	p := newPair(t)
	release := make(chan struct{})
	defer close(release)
	mux := http.NewServeMux()
	mux.HandleFunc("/stall", func(http.ResponseWriter, *http.Request) { <-release })
	mux.HandleFunc("/fast", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	go func() { _ = http.Serve(p.listener, mux) }()
	tr := NewHTTPTransport(p.dialer)
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr}

	for range 4 {
		go func() {
			resp, err := client.Post("http://"+p.addr+"/stall", "application/octet-stream", io.LimitReader(zeros{}, 64<<20))
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)

	for i := range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+p.addr+"/fast", nil)
		resp, err := client.Do(req)
		if err != nil {
			cancel()
			t.Fatalf("request %d behind stalled uploads: %v", i, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		cancel()
	}
}

// A peer connection has a fixed supply of channel IDs and never reuses one,
// so the dialer must move to a fresh peer connection before they run out,
// without disturbing connections still open on the old one.
func TestPeerConnectionIsReplacedAfterItsDialBudget(t *testing.T) {
	p := newPair(t)
	p.dialer.maxDials = 3
	go func() {
		for {
			c, err := p.listener.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(c, c) }()
		}
	}()
	dial := func() net.Conn {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		c, err := p.dialer.DialContext(ctx, "webrtc", p.addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	echo := func(c net.Conn, msg string) {
		t.Helper()
		_ = c.SetDeadline(time.Now().Add(testWait))
		if _, err := c.Write([]byte(msg)); err != nil {
			t.Fatalf("write: %v", err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(c, got); err != nil || string(got) != msg {
			t.Fatalf("echo = %q, %v; want %q", got, err, msg)
		}
	}

	old := []net.Conn{dial(), dial(), dial()}
	p.dialer.mu.Lock()
	var first *peer
	for retired := range p.dialer.draining {
		first = retired
	}
	p.dialer.mu.Unlock()
	if first == nil {
		t.Fatal("the peer connection was not retired after its dial budget")
	}

	fresh := dial()
	if n := p.offers.Load(); n != 2 {
		t.Fatalf("offers after exceeding the dial budget = %d, want 2", n)
	}
	echo(fresh, "on the new peer connection")
	echo(old[0], "still alive on the old one")

	_ = old[0].Close()
	_ = old[1].Close()
	select {
	case <-first.dead:
		t.Fatal("old peer connection closed while a connection was still open on it")
	case <-time.After(200 * time.Millisecond):
	}
	echo(old[2], "last one on the old peer connection")
	_ = old[2].Close()
	select {
	case <-first.dead:
	case <-time.After(testWait):
		t.Fatal("old peer connection was not closed after its last connection closed")
	}
	echo(fresh, "unaffected")
}

// fakeChannel feeds scripted messages to a conn's pump.
type fakeChannel struct {
	in     chan fakeMessage
	closed chan struct{}
	once   sync.Once
}

type fakeMessage struct {
	data   []byte
	isText bool
}

func newFakeChannel() *fakeChannel {
	return &fakeChannel{in: make(chan fakeMessage, 64), closed: make(chan struct{})}
}

func (f *fakeChannel) ReadDataChannel(p []byte) (int, bool, error) {
	select {
	case m := <-f.in:
		return copy(p, m.data), m.isText, nil
	case <-f.closed:
		return 0, false, io.EOF
	}
}
func (f *fakeChannel) WriteDataChannel(p []byte, _ bool) (int, error) { return len(p), nil }
func (f *fakeChannel) SetReadDeadline(time.Time) error                { return nil }
func (f *fakeChannel) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

func TestPeerThatIgnoresItsWindowIsCutOff(t *testing.T) {
	raw := newFakeChannel()
	c := newConn(raw, "local", "remote", nil)
	t.Cleanup(func() { _ = c.Close() })
	msg := make([]byte, maxWriteChunk)
	for range streamWindow/maxWriteChunk + 1 {
		raw.in <- fakeMessage{data: msg}
	}
	select {
	case <-raw.closed:
	case <-time.After(testWait):
		t.Fatal("a peer that sent past its window was not cut off")
	}
	if _, err := io.Copy(io.Discard, c); !errors.Is(err, errWindowExceeded) {
		t.Fatalf("Read = %v, want errWindowExceeded", err)
	}
}

func TestPeerThatInventsCreditIsCutOff(t *testing.T) {
	for _, credit := range []string{"1", "-5", "nonsense", "99999999999999999999"} {
		raw := newFakeChannel()
		c := newConn(raw, "local", "remote", nil)
		raw.in <- fakeMessage{data: []byte(credit), isText: true}
		select {
		case <-raw.closed:
		case <-time.After(testWait):
			t.Fatalf("credit %q for bytes never sent was accepted", credit)
		}
		if _, err := c.Write(make([]byte, 2*streamWindow)); !errors.Is(err, errBadCredit) {
			t.Fatalf("credit %q: Write = %v, want errBadCredit", credit, err)
		}
		_ = c.Close()
	}
}
