package internet

import (
	"net/netip"
	"testing"
	"time"
)

// E6: an invite's private LAN introducer, seen from another network that
// uses the same range, is a stranger's machine. Once it has been silent for
// silentAfter while another introducer answers, registrations to it stop;
// they resume when nothing else answers.
func TestASilentPrivateIntroducerIsNotRegisteredWithWhileAnotherAnswers(t *testing.T) {
	now := time.Now()
	lan := &introducer{addr: "192.168.0.2:4244", at: netip.MustParseAddrPort("192.168.0.2:4244"),
		waiting: now.Add(-2 * silentAfter)}
	pub := &introducer{addr: "203.0.113.7:4244", at: netip.MustParseAddrPort("203.0.113.7:4244"),
		lastReply: now}
	self := &introducer{addr: "127.0.0.1:4244", self: true, lastReply: now}
	m := &Manager{cfg: Config{Poll: 20 * time.Second}, intros: []*introducer{lan, pub, self}}

	if !m.strayLocked(lan, now) {
		t.Error("a private introducer silent for 2x silentAfter while a public one answers is still registered with")
	}
	if m.strayLocked(pub, now) {
		t.Error("the answering introducer was skipped")
	}
	if m.strayLocked(lan, lan.waiting.Add(silentAfter/2)) {
		t.Error("skipped before silentAfter")
	}
	pub.lastReply = time.Time{}
	if m.strayLocked(lan, now) {
		t.Error("skipped although only our own introducer answers: it may be the only way in")
	}
}
