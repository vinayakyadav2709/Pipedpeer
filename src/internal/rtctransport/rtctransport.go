// Package rtctransport carries byte streams over WebRTC data channels so the
// daemon's HTTP/1.1 API can run peer to peer without changing handlers or clients.
package rtctransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

const SignalPath = "/v1/rtc/offer"

const (
	bootstrapLabel = "bootstrap"
	streamLabel    = "http"
	maxSignalBody  = 1 << 20 // bytes
	connectTimeout = 30 * time.Second
	// A channel ID is never reused, each new channel scans the used ones, and
	// a peer connection has about 32k for its dialing side. A Dialer moves to
	// a fresh peer connection long before that.
	maxDialsPerPeer = 4096

	idleConnsPerPeer = 8
	idleConnTimeout  = 90 * time.Second
)

type Config struct {
	ICEServers []string // STUN/TURN URLs; empty means host candidates only (LAN)
	// Signal carries a Dialer's offers. The answer's DTLS fingerprint is what
	// authenticates the data channels, so they are as trusted as this route.
	// Nil means plain HTTP.
	Signal http.RoundTripper
}

func newPeerConnection(cfg Config) (*webrtc.PeerConnection, error) {
	var se webrtc.SettingEngine
	se.DetachDataChannels()
	// Two daemons on one host have no other route to each other.
	se.SetIncludeLoopbackCandidate(true)
	var rtcCfg webrtc.Configuration
	if len(cfg.ICEServers) > 0 {
		rtcCfg.ICEServers = []webrtc.ICEServer{{URLs: cfg.ICEServers}}
	}
	return webrtc.NewAPI(webrtc.WithSettingEngine(se)).NewPeerConnection(rtcCfg)
}

func isDead(s webrtc.PeerConnectionState) bool {
	return s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed
}

type Addr string

func (Addr) Network() string  { return "webrtc" }
func (a Addr) String() string { return string(a) }

// Listener is a net.Listener whose connections are WebRTC data channels opened by remote peers.
type Listener struct {
	cfg    Config
	conns  chan net.Conn
	closed chan struct{}

	mu    sync.Mutex
	peers map[*webrtc.PeerConnection]struct{}
	done  bool
}

func NewListener(cfg Config) *Listener {
	return &Listener{
		cfg:    cfg,
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
		peers:  make(map[*webrtc.PeerConnection]struct{}),
	}
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *Listener) Close() error {
	l.mu.Lock()
	if l.done {
		l.mu.Unlock()
		return nil
	}
	l.done = true
	peers := l.peers
	l.peers = nil
	l.mu.Unlock()

	close(l.closed)
	var errs []error
	for pc := range peers {
		errs = append(errs, pc.Close())
	}
	return errors.Join(errs...)
}

func (l *Listener) Addr() net.Addr { return Addr("listener") }

// SignalHandler serves POST SignalPath: body is a JSON webrtc.SessionDescription offer, response is the JSON answer.
func (l *Listener) SignalHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var offer webrtc.SessionDescription
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSignalBody)).Decode(&offer); err != nil {
			http.Error(w, "invalid offer: "+err.Error(), http.StatusBadRequest)
			return
		}
		answer, err := l.answer(r.Context(), offer, Addr(r.RemoteAddr))
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, net.ErrClosed) {
				status = http.StatusServiceUnavailable
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(answer)
	})
}

func (l *Listener) answer(ctx context.Context, offer webrtc.SessionDescription, remote Addr) (*webrtc.SessionDescription, error) {
	pc, err := newPeerConnection(l.cfg)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.done {
		l.mu.Unlock()
		_ = pc.Close()
		return nil, net.ErrClosed
	}
	l.peers[pc] = struct{}{}
	l.mu.Unlock()

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if !isDead(s) {
			return
		}
		l.mu.Lock()
		delete(l.peers, pc)
		l.mu.Unlock()
		if s == webrtc.PeerConnectionStateFailed {
			_ = pc.Close()
		}
	})
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if dc.Label() == bootstrapLabel {
			return
		}
		dc.OnOpen(func() {
			raw, err := dc.DetachWithDeadline()
			if err != nil {
				_ = dc.Close()
				return
			}
			c := newConn(raw, Addr("listener"), remote, nil)
			select {
			case l.conns <- c:
			case <-l.closed:
				_ = c.Close()
			}
		})
	})

	fail := func(err error) (*webrtc.SessionDescription, error) {
		_ = pc.Close()
		return nil, err
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		return fail(err)
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return fail(err)
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		return fail(err)
	}
	select {
	case <-gathered:
	case <-ctx.Done():
		return fail(ctx.Err())
	case <-l.closed:
		return fail(net.ErrClosed)
	}
	return pc.LocalDescription(), nil
}

// peer is one peer connection shared by every dial to an address. pc and err
// are set before ready closes and never change after.
type peer struct {
	ready chan struct{}
	pc    *webrtc.PeerConnection
	err   error
	dead  chan struct{}

	// Guarded by Dialer.mu.
	dials   int // channels ever opened on pc
	open    int // connections handed out and not yet closed
	retired bool
}

// Dialer opens data-channel connections to peers, signaling at http://<addr>SignalPath over Config.Signal.
type Dialer struct {
	cfg      Config
	signal   *http.Client
	maxDials int
	// Connects run under this, not under a caller's context: the peer
	// connection outlives the dial that happened to start it.
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	peers map[string]*peer
	// Retired peers that still carry connections; each is closed when its
	// last connection is.
	draining map[*peer]struct{}
	closed   bool
}

func NewDialer(cfg Config) *Dialer {
	ctx, cancel := context.WithCancel(context.Background())
	signal := cfg.Signal
	if signal == nil {
		signal = &http.Transport{DisableKeepAlives: true}
	}
	return &Dialer{
		cfg:      cfg,
		signal:   &http.Client{Transport: signal},
		maxDials: maxDialsPerPeer,
		ctx:      ctx,
		cancel:   cancel,
		peers:    make(map[string]*peer),
		draining: make(map[*peer]struct{}),
	}
}

// DialContext has the http.Transport.DialContext shape; addr is the peer's host:port.
func (d *Dialer) DialContext(ctx context.Context, _ string, addr string) (net.Conn, error) {
	p, err := d.acquire(ctx, addr)
	if err != nil {
		return nil, err
	}
	raw, err := openChannel(ctx, p, addr)
	if err != nil {
		d.release(p)
		return nil, err
	}
	return newConn(raw, Addr("dialer"), Addr(addr), func() { d.release(p) }), nil
}

func openChannel(ctx context.Context, p *peer, addr string) (channel, error) {
	dc, err := p.pc.CreateDataChannel(streamLabel, nil)
	if err != nil {
		return nil, err
	}
	opened := make(chan struct{})
	dc.OnOpen(func() { close(opened) })
	select {
	case <-opened:
	case <-ctx.Done():
		_ = dc.Close()
		return nil, ctx.Err()
	case <-p.dead:
		return nil, fmt.Errorf("rtctransport: peer connection to %s lost", addr)
	}
	raw, err := dc.DetachWithDeadline()
	if err != nil {
		_ = dc.Close()
		return nil, err
	}
	return raw, nil
}

// acquire returns a connected peer with one connection counted against it;
// the caller owes a release.
func (d *Dialer) acquire(ctx context.Context, addr string) (*peer, error) {
	for {
		p, err := d.peer(ctx, addr)
		if err != nil {
			return nil, err
		}
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			return nil, net.ErrClosed
		}
		if p.retired {
			d.mu.Unlock()
			continue
		}
		p.open++
		p.dials++
		if p.dials >= d.maxDials {
			p.retired = true
			d.draining[p] = struct{}{}
			if d.peers[addr] == p {
				delete(d.peers, addr)
			}
		}
		d.mu.Unlock()
		return p, nil
	}
}

func (d *Dialer) release(p *peer) {
	d.mu.Lock()
	p.open--
	drained := p.retired && p.open == 0
	if drained {
		delete(d.draining, p)
	}
	d.mu.Unlock()
	if drained {
		_ = p.pc.Close()
	}
}

func (d *Dialer) peer(ctx context.Context, addr string) (*peer, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, net.ErrClosed
	}
	p := d.peers[addr]
	if p == nil {
		p = &peer{ready: make(chan struct{}), dead: make(chan struct{})}
		d.peers[addr] = p
		go func() {
			p.pc, p.err = d.connect(addr, p)
			if p.err != nil {
				d.forget(addr, p)
			}
			close(p.ready)
		}()
	}
	d.mu.Unlock()

	select {
	case <-p.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if p.err != nil {
		return nil, p.err
	}
	return p, nil
}

func (d *Dialer) forget(addr string, p *peer) {
	d.mu.Lock()
	if d.peers[addr] == p {
		delete(d.peers, addr)
	}
	d.mu.Unlock()
}

func (d *Dialer) connect(addr string, p *peer) (*webrtc.PeerConnection, error) {
	ctx, cancel := context.WithTimeout(d.ctx, connectTimeout)
	defer cancel()

	pc, err := newPeerConnection(d.cfg)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*webrtc.PeerConnection, error) {
		_ = pc.Close()
		return nil, fmt.Errorf("rtctransport: connect %s: %w", addr, err)
	}
	var deadOnce sync.Once
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if !isDead(s) {
			return
		}
		d.forget(addr, p)
		deadOnce.Do(func() { close(p.dead) })
		if s == webrtc.PeerConnectionStateFailed {
			_ = pc.Close()
		}
	})

	// Without a data channel the offer carries no SCTP section.
	if _, err := pc.CreateDataChannel(bootstrapLabel, nil); err != nil {
		return fail(err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return fail(err)
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		return fail(err)
	}
	select {
	case <-gathered:
	case <-ctx.Done():
		return fail(ctx.Err())
	}

	answer, err := d.exchange(ctx, addr, pc.LocalDescription())
	if err != nil {
		return fail(err)
	}
	if err := pc.SetRemoteDescription(*answer); err != nil {
		return fail(err)
	}
	return pc, nil
}

func (d *Dialer) exchange(ctx context.Context, addr string, offer *webrtc.SessionDescription) (*webrtc.SessionDescription, error) {
	body, err := json.Marshal(offer)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+SignalPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.signal.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSignalBody))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("signal %s: %s", resp.Status, bytes.TrimSpace(data))
	}
	var answer webrtc.SessionDescription
	if err := json.Unmarshal(data, &answer); err != nil {
		return nil, fmt.Errorf("decode answer: %w", err)
	}
	return &answer, nil
}

func (d *Dialer) Close() error {
	d.mu.Lock()
	d.closed = true
	peers, draining := d.peers, d.draining
	d.peers, d.draining = make(map[string]*peer), make(map[*peer]struct{})
	d.mu.Unlock()

	d.cancel()
	var errs []error
	for _, p := range peers {
		// A connect in flight was just cancelled; wait for it so its
		// connection is closed, not leaked.
		<-p.ready
		if p.pc != nil {
			errs = append(errs, p.pc.Close())
		}
	}
	for p := range draining {
		errs = append(errs, p.pc.Close())
	}
	return errors.Join(errs...)
}

// NewHTTPTransport returns a transport that sends HTTP requests over data channels from d.
func NewHTTPTransport(d *Dialer) *http.Transport {
	return &http.Transport{
		DialContext:         d.DialContext,
		MaxIdleConnsPerHost: idleConnsPerPeer,
		IdleConnTimeout:     idleConnTimeout,
	}
}
