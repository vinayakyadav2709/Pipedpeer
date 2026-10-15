package internet

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/portmap"
)

// These run real managers over loopback: real QUIC, real punching, a real
// introducer inside one of the daemons. The keeper is stubbed so no test asks
// the machine's router for a port.

func newKey(t *testing.T) identity.KeyPair {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return identity.KeyPair{Public: pub, Private: priv}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

// testLog forwards to t.Logf until the test ends; goroutines outlive it.
type testLog struct {
	mu   sync.Mutex
	t    *testing.T
	done bool
	name string
}

func (l *testLog) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.done {
		l.t.Logf(l.name+": "+format, args...)
	}
}

// stubKeeper and shortTimers set the package knobs for one test.
func stubKeeper(t *testing.T) {
	prevKeeper, prevSilent := newKeeper, silentAfter
	newKeeper = func(context.Context, uint16, func(string, ...any)) *portmap.Keeper { return &portmap.Keeper{} }
	t.Cleanup(func() { newKeeper, silentAfter = prevKeeper, prevSilent })
}

// run starts a manager and stops it, waiting for Run to return, at cleanup.
func run(t *testing.T, name string, cfg Config) *Manager {
	t.Helper()
	cert := selfSigned(t)
	cfg.Cert = &cert
	cfg.Cluster, cfg.Secret = "c-test", "secret"
	if cfg.Poll == 0 {
		cfg.Poll = 200 * time.Millisecond
	}
	l := &testLog{t: t, name: name}
	cfg.Log = l.logf
	m := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	t.Cleanup(func() {
		l.mu.Lock()
		l.done = true
		l.mu.Unlock()
		cancel()
		<-done
	})
	return m
}

func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("not within %s: %s", within, what)
}

func linked(a, b *Manager) bool {
	_, ab := a.Peers()[b.node()]
	_, ba := b.Peers()[a.node()]
	return ab && ba
}

// TestTwoIntroducersOneDeadStillJoin (F34/F36/F37). A member registers with
// every introducer, so a dead one costs nothing while another answers - and
// the dead one is reported, not silently retried forever.
func TestTwoIntroducersOneDeadStillJoin(t *testing.T) {
	stubKeeper(t)
	silentAfter = 600 * time.Millisecond

	// Bound and never read: an introducer that does not answer.
	dead, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer dead.Close()

	introPort := freeUDPPort(t)
	a := run(t, "A", Config{Key: newKey(t), DirectPort: freeUDPPort(t),
		ServeIntroducer: true, IntroducerPort: introPort})
	live := net.JoinHostPort("127.0.0.1", itoa(introPort))
	b := run(t, "B", Config{Key: newKey(t), DirectPort: freeUDPPort(t),
		Introducers: []string{dead.LocalAddr().String(), live}})

	waitFor(t, 15*time.Second, "A and B link through the live introducer", func() bool { return linked(a, b) })

	waitFor(t, 5*time.Second, "the dead introducer is reported with its error", func() bool {
		var deadOK, liveOK bool
		for _, in := range b.Introducers() {
			switch in.Addr {
			case dead.LocalAddr().String():
				deadOK = in.LastErr != "" && in.LastReply.IsZero()
			case live:
				liveOK = in.LastErr == "" && !in.LastReply.IsZero() && in.Members == 1
			}
		}
		return deadOK && liveOK
	})
	if got := a.Introducers(); len(got) != 0 {
		t.Errorf("A configured no introducer but reports %+v; its own is not configured", got)
	}
}

// TestAPeerBackFromANewAddressLinksWithinOneRound (F33). A peer that changed
// address comes back while this side still holds the old connection, which
// looks alive until its idle timeout. The authenticated connection from the
// same key replaces it at once - about three minutes before, in the lab - and
// the forwarder stays, so the daemon sees no leave and join.
func TestAPeerBackFromANewAddressLinksWithinOneRound(t *testing.T) {
	stubKeeper(t)
	introPort := freeUDPPort(t)
	intro := net.JoinHostPort("127.0.0.1", itoa(introPort))
	a := run(t, "A", Config{Key: newKey(t), DirectPort: freeUDPPort(t),
		ServeIntroducer: true, IntroducerPort: introPort})

	keyB := newKey(t)
	ctx1, stop1 := context.WithCancel(context.Background())
	cert := selfSigned(t)
	b1 := New(Config{Key: keyB, DirectPort: freeUDPPort(t), Introducers: []string{intro},
		Cluster: "c-test", Secret: "secret", Cert: &cert, Poll: 200 * time.Millisecond})
	stopped := make(chan struct{})
	go func() { b1.Run(ctx1); close(stopped) }()
	defer func() { stop1(); <-stopped }()

	waitFor(t, 15*time.Second, "A and B link", func() bool { return linked(a, b1) })
	nodeB := b1.node()
	forwarder := a.Peers()[nodeB]

	// B vanishes without a goodbye: its socket goes first, so no close
	// reaches A and A's connection looks alive for the idle timeout. The
	// link is made old, so this is a return and not a simultaneous punch.
	_ = b1.endpoint.Conn().Close()
	stop1()
	<-stopped
	a.mu.Lock()
	old := a.peers[nodeB]
	old.since = time.Now().Add(-time.Minute)
	oldConn := old.conn
	a.mu.Unlock()
	if !old.alive() {
		t.Fatal("A noticed B leaving; the test needs the stale-but-alive case")
	}

	portB2 := freeUDPPort(t)
	run(t, "B2", Config{Key: keyB, DirectPort: portB2, Introducers: []string{intro}})

	// One register round is 200 ms here; the old connection would have
	// lingered 90 s. A punch race plus a dial fits well inside 5 s.
	waitFor(t, 5*time.Second, "A's link to B moves to B's new address", func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		l := a.peers[nodeB]
		return l != nil && l.conn != nil && l.conn != oldConn &&
			strings.HasSuffix(l.conn.RemoteAddr().String(), ":"+itoa(portB2))
	})
	time.Sleep(500 * time.Millisecond) // let a simultaneous second connection settle
	a.mu.Lock()
	l := a.peers[nodeB]
	a.mu.Unlock()
	if l == nil || !l.alive() {
		t.Fatal("the link to B died after the replacement")
	}
	if got := a.Peers()[nodeB]; got != forwarder {
		t.Errorf("the forwarder moved from %s to %s; the daemon saw a leave and a join", forwarder, got)
	}
}

// TestANewerConnectionReplacesAnOldLink: the replacement keeps the forwarder
// and closes the old connection; inside the race window the tie-break keeps
// one connection, the same one on both ends.
func TestANewerConnectionReplacesAnOldLink(t *testing.T) {
	m := New(Config{Key: newKey(t)})
	defer m.closeAll()
	ctx := context.Background()
	first, second := liveConn(t), liveConn(t)
	if err := m.linkUp(ctx, "peer-b", first, "punched"); err != nil {
		t.Fatal(err)
	}
	addr := m.Peers()["peer-b"]
	m.mu.Lock()
	m.peers["peer-b"].since = time.Now().Add(-time.Minute)
	m.mu.Unlock()

	if err := m.linkUp(ctx, "peer-b", second, pathInbound); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	cur := m.peers["peer-b"].conn
	m.mu.Unlock()
	if cur != second || m.Peers()["peer-b"] != addr {
		t.Fatal("a newer connection from the same key did not replace the old one in place")
	}
	select {
	case <-first.Context().Done():
	case <-time.After(5 * time.Second):
		t.Error("the replaced connection was left open")
	}

	// Inside the window: exactly one of (we dialled, they dialled) wins.
	if m.newWins("peer-b", true) == m.newWins("peer-b", false) {
		t.Error("the tie-break does not pick exactly one direction")
	}
}

// TestHintsJoinTheCandidates: a gossiped bare host:port is tried as a gossip
// candidate after what the peer published, without repeating an address.
func TestHintsJoinTheCandidates(t *testing.T) {
	m := New(Config{Key: newKey(t)})
	m.cands["peer-b"] = []string{"lan:10.0.0.2:4243"}
	m.Hint("peer-b", []string{"10.0.0.2:4243", "198.51.100.4:4243", "not an address"})
	m.mu.Lock()
	got := m.candidatesLocked("peer-b", time.Now())
	m.mu.Unlock()
	want := []string{"lan:10.0.0.2:4243", "gossip:198.51.100.4:4243"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("candidates = %v, want %v", got, want)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
