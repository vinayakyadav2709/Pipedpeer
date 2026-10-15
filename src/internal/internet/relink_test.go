package internet

import (
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
	a := run(t, "A", Config{Key: newKey(t), DirectPort: freeUDPPort(t),
		ServeIntroducer: true, IntroducerPort: introPort})
	b := run(t, "B", Config{Key: newKey(t), DirectPort: freeUDPPort(t), Introducers: []string{intro}})
	waitFor(t, 15*time.Second, "A and B link", func() bool { return linked(a, b) })

	// The outage: penalties as long as the cap on both sides, for exactly the
	// addresses they would retry, then the links go.
	penalise := func(m, peer *Manager) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.backoff[peer.node()] = backoffEntry{until: time.Now().Add(5 * time.Minute),
			tried: candKey(m.candidatesLocked(peer.node(), time.Now())), reason: "timeout"}
	}
	penalise(a, b)
	penalise(b, a)
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

	// B's registrations went unanswered for a while, then one is answered.
	b.mu.Lock()
	for _, in := range b.intros {
		if in.addr == intro {
			in.waiting = time.Now().Add(-2 * lostAfter)
		}
	}
	b.mu.Unlock()

	waitFor(t, 5*time.Second, "A and B relink, not after the 5 min penalty", func() bool { return linked(a, b) })
}
