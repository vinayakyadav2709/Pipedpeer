package internet

import (
	"net/netip"
	"testing"
	"time"
)

// TestBothSidesRelinkAtOnceAfterAnOutage (FIX-RELINK). B's router stops
// forwarding, both links die and both sides earn a penalty for the failed
// redials. When B's packets reach the introducer again, B must not sit out
// the penalty: the introducer answering again retries A at once, and B's
// punch request has A punch back despite A's own penalty.
func TestBothSidesRelinkAtOnceAfterAnOutage(t *testing.T) {
	stubKeeper(t)
	introPort := freeUDPPort(t)
	intro := "127.0.0.1:" + itoa(introPort)
	portA, portB := freeUDPPort(t), freeUDPPort(t)
	a := run(t, "A", Config{Key: newKey(t), DirectPort: portA,
		ServeIntroducer: true, IntroducerPort: introPort})
	b := run(t, "B", Config{Key: newKey(t), DirectPort: portB, Introducers: []string{intro}})
	// Linked is not enough: both ends punch at once, and the losing
	// connection can still be on its way into linkUp on either side. One
	// that lands after the links are dropped below relinks without any
	// attempt, and the penalties are never tested.
	waitFor(t, 15*time.Second, "A and B link and stop dialling each other", func() bool {
		return linked(a, b) && idle(a, b) && idle(b, a)
	})

	// The outage: nothing between the two direct ports gets through either
	// way (the introducer, on its own port, still answers), penalties as
	// long as the cap on both sides for exactly the addresses they would
	// retry, then the links go.
	cut := func(port int) func(netip.AddrPort) bool {
		return func(from netip.AddrPort) bool { return int(from.Port()) == port }
	}
	a.endpoint.Block(cut(portB))
	b.endpoint.Block(cut(portA))
	penalise := func(m, peer *Manager) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.backoff[peer.node()] = backoffEntry{until: time.Now().Add(5 * time.Minute),
			tried: candKey(m.candidatesLocked(peer.node(), time.Now())), reason: "timeout"}
	}
	penalise(a, b)
	penalise(b, a)
	a.dropLink(b.node())
	b.dropLink(a.node())
	waitFor(t, 5*time.Second, "both links are gone", func() bool {
		_, ab := a.Peers()[b.node()]
		_, ba := b.Peers()[a.node()]
		return !ab && !ba
	})
	time.Sleep(time.Second) // several polls: the penalties hold
	if _, ab := a.Peers()[b.node()]; ab {
		t.Fatal("A relinked under its penalty; the test needs both sides penalised")
	}

	// The outage ends; B's registrations went unanswered for a while, and
	// one is answered.
	a.endpoint.Block(nil)
	b.endpoint.Block(nil)
	b.mu.Lock()
	for _, in := range b.intros {
		if in.addr == intro {
			in.waiting = time.Now().Add(-2 * lostAfter)
		}
	}
	b.mu.Unlock()

	waitFor(t, 5*time.Second, "A and B relink, not after the 5 min penalty", func() bool { return linked(a, b) })
}

// idle reports that m has no connection to peer in progress: no dial of its
// own, and no inbound connection still on its way to linkUp.
func idle(m, peer *Manager) bool {
	m.mu.Lock()
	trying := m.trying[peer.node()]
	m.mu.Unlock()
	return !trying && m.endpoint.Accepting() == 0
}
