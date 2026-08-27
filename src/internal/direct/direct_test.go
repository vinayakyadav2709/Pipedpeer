package direct

import (
	"context"
	"math"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func loopbackConn(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func addrOf(c *net.UDPConn) netip.AddrPort {
	return c.LocalAddr().(*net.UDPAddr).AddrPort()
}

// pump reads a socket and feeds a prober, which is what the daemon's own read
// loop will do once QUIC shares this socket.
func pump(t *testing.T, c *net.UDPConn, p *Prober) {
	t.Helper()
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := c.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			p.Deliver(buf[:n], from)
		}
	}()
}

// TestCandidatesAreTriedBestFirst. A LAN address costs nothing and no router
// can interfere with it; a mapped port is stable; a reflexive address is a
// guess needing the far side's cooperation. Offering them in the wrong order
// means preferring the path most likely to fail.
func TestCandidatesAreTriedBestFirst(t *testing.T) {
	got := ParseCandidates([]string{
		"reflex:203.0.113.1:41000",
		"mapped:203.0.113.1:38447",
		"lan:192.168.0.10:38447",
	})
	if len(got) != 3 {
		t.Fatalf("parsed %d of 3: %v", len(got), got)
	}
	want := []Kind{KindLAN, KindMapped, KindReflex}
	for i, k := range want {
		if got[i].Kind != k {
			t.Errorf("position %d is %s, want %s — order is %v", i, got[i].Kind, k, got)
		}
	}
}

// A candidate kind this build has never heard of is not an error: a newer
// daemon may offer one, and refusing the whole peer over it would make an
// upgrade on one machine break the cluster.
func TestAnUnknownCandidateKindIsSkippedNotFatal(t *testing.T) {
	got := ParseCandidates([]string{
		"quantum:203.0.113.1:1",
		"lan:192.168.0.10:38447",
		"garbage",
	})
	if len(got) != 1 || got[0].Kind != KindLAN {
		t.Errorf("got %v, want just the LAN candidate", got)
	}
}

// TestGatherFindsTheLANAddresses. Two machines on one network are the case a
// relay-only design serves worst and most often: their traffic goes out to
// the internet and back for no reason at all.
func TestGatherFindsTheLANAddresses(t *testing.T) {
	mapped := netip.MustParseAddrPort("203.0.113.9:38447")
	reflex := netip.MustParseAddrPort("203.0.113.9:41000")
	got := Gather(38447, mapped, reflex)

	var lans, maps, reflexes int
	for _, c := range got {
		switch c.Kind {
		case KindLAN:
			lans++
			if c.Addr.Addr().IsLoopback() {
				t.Errorf("loopback %s offered as a candidate; it reaches only this machine", c.Addr)
			}
			if c.Addr.Port() != 38447 {
				t.Errorf("LAN candidate %s is not on the shared port", c.Addr)
			}
		case KindMapped:
			maps++
		case KindReflex:
			reflexes++
		}
	}
	if maps != 1 || reflexes != 1 {
		t.Errorf("mapped=%d reflex=%d, want 1 each", maps, reflexes)
	}
	if lans == 0 {
		t.Log("no non-loopback interface on this machine; the LAN half is untested here")
	}
	// Best-first, whatever the machine has.
	for i := 1; i < len(got); i++ {
		if got[i].Kind.rank() < got[i-1].Kind.rank() {
			t.Errorf("candidates are not in rank order: %v", got)
			break
		}
	}
}

// TestTheRaceReturnsTheAddressThatAnswered is the ordinary punch: probes go
// to every candidate at once and the first reply wins.
func TestTheRaceReturnsTheAddressThatAnswered(t *testing.T) {
	// The peer, which answers probes.
	peerConn := loopbackConn(t)
	peer := NewProber(peerConn, "beefbeefbeefbeef")
	pump(t, peerConn, peer)

	meConn := loopbackConn(t)
	me := NewProber(meConn, "cafecafecafecafe")
	pump(t, meConn, me)

	// One address that answers and two that never will.
	cands := []Candidate{
		{Kind: KindLAN, Addr: netip.MustParseAddrPort("127.0.0.1:9")},
		{Kind: KindMapped, Addr: addrOf(peerConn)},
		{Kind: KindReflex, Addr: netip.MustParseAddrPort("127.0.0.1:11")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := me.Race(ctx, cands, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if got != addrOf(peerConn) {
		t.Errorf("race returned %s, want the peer at %s", got, addrOf(peerConn))
	}
}

// A race where nothing answers must end at the deadline rather than hanging:
// a peer behind a firewall that drops UDP is a normal thing to meet, and the
// scheduler needs the answer in order to route around it.
func TestTheRaceGivesUpWhenNothingAnswers(t *testing.T) {
	meConn := loopbackConn(t)
	me := NewProber(meConn, "cafecafecafecafe")
	pump(t, meConn, me)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := me.Race(ctx, []Candidate{
		{Kind: KindLAN, Addr: netip.MustParseAddrPort("127.0.0.1:9")},
	}, 50*time.Millisecond)
	if err == nil {
		t.Fatal("the race reported success against a port nothing is on")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("took %s to give up; the caller has other peers to try", d)
	}
}

// TestARaceNeedsSomewhereToTry. A peer that published nothing cannot be
// reached, and saying so is different from timing out.
func TestARaceNeedsSomewhereToTry(t *testing.T) {
	me := NewProber(loopbackConn(t), "cafecafecafecafe")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := me.Race(ctx, nil, 0); err == nil {
		t.Error("racing against an empty candidate list reported success")
	}
}

// TestSomebodyElsesReplyIsIgnored. Every attempt carries a nonce, and a
// reply to a different one arriving on the shared socket must not resolve
// this race - two connections being set up at once is ordinary.
func TestSomebodyElsesReplyIsIgnored(t *testing.T) {
	meConn := loopbackConn(t)
	me := NewProber(meConn, "cafecafecafecafe")
	pump(t, meConn, me)

	// A prober that replies with a nonce nobody asked about.
	noise := loopbackConn(t)
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := noise.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			if _, ok := parseProbe(buf[:n]); !ok {
				continue
			}
			bogus := probe{kind: probeReply, nonce: [8]byte{9, 9, 9, 9, 9, 9, 9, 9}, from: fingerprint8("deadbeefdeadbeef")}
			_, _ = noise.WriteToUDPAddrPort(bogus.marshal(), from)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := me.Race(ctx, []Candidate{{Kind: KindLAN, Addr: addrOf(noise)}}, 50*time.Millisecond); err == nil {
		t.Error("a reply carrying another attempt's nonce resolved this race")
	}
}

// TestTheBirthdayOddsAreWorthTheBurst.
//
// This is the arithmetic the whole technique rests on, checked rather than
// asserted in a comment. With S mappings open and P probes across the usable
// port space, the chance of no collision is (1 - S/ports)^P. If that number
// were not small the burst would be wasted packets.
func TestTheBirthdayOddsAreWorthTheBurst(t *testing.T) {
	ports := float64(65535 - birthdayPortLow)
	miss := math.Pow(1-float64(birthdaySockets)/ports, float64(birthdayProbes))
	if hit := 1 - miss; hit < 0.90 {
		t.Errorf("with %d sockets and %d probes the hit rate is %.3f; "+
			"below 0.90 the burst is not worth sending",
			birthdaySockets, birthdayProbes, hit)
	}
	// And the cost stays bounded: a few hundred small packets, not a scan.
	if birthdayProbes > 1000 {
		t.Errorf("%d probes is no longer a burst, it is a port scan", birthdayProbes)
	}
}

// TestTheBirthdayPunchConnectsThroughASymmetricNAT is the measured case:
// this project's two machines, where an ordinary punch moved 400 packets in
// each direction and received nothing.
//
// The symmetric side opens many sockets, the other sprays the port space,
// and they collide. Loopback stands in for the two routers - what is being
// tested is that the two halves meet and hand back the socket the mapping
// belongs to, which is the part that has to be right for QUIC to follow.
func TestTheBirthdayPunchConnectsThroughASymmetricNAT(t *testing.T) {
	// The field settings give a 90% hit, which is the right trade when the
	// alternative is relaying every byte - and a test that fails one run in
	// ten. Narrowed to the range the kernel actually hands out ephemeral
	// ports from, and given more probes, a miss becomes about one run in a
	// hundred million. The technique is unchanged; only the luck is removed.
	lo, hi := ephemeralRange(t)
	restore := []func(){
		set(&birthdayPortLow, lo), set(&birthdayPortHigh, hi),
		set(&birthdayProbes, 2000), set(&birthdayTick, time.Millisecond),
	}
	t.Cleanup(func() {
		for _, f := range restore {
			f()
		}
	})

	// The predictable side, spraying.
	sprayConn := loopbackConn(t)
	spray := NewProber(sprayConn, "cafecafecafecafe")
	pump(t, sprayConn, spray)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// The symmetric side opens its many sockets toward the sprayer.
	type result struct {
		conn *net.UDPConn
		from netip.AddrPort
		err  error
	}
	done := make(chan result, 1)
	go func() {
		c, from, err := OpenMany(ctx, addrOf(sprayConn), "beefbeefbeefbeef", 15*time.Second)
		done <- result{c, from, err}
	}()

	// Give the sockets a moment to exist before spraying at them.
	time.Sleep(300 * time.Millisecond)

	hit, err := spray.SprayToward(ctx, netip.MustParseAddr("127.0.0.1"), netip.AddrPort{}, 15*time.Second)
	if err != nil {
		t.Fatalf("the spray found none of the open sockets: %v", err)
	}

	r := <-done
	if r.err != nil {
		t.Fatalf("the symmetric side reported no hit: %v", r.err)
	}
	if r.conn == nil {
		t.Fatal("no socket was handed back; QUIC would have to dial from the wrong mapping")
	}
	defer r.conn.Close()

	// The socket handed back must be the one whose mapping the sprayer
	// reached: dialling from any other arrives at the far router as an
	// unrelated flow and is dropped.
	//
	// Compared by port, because the socket is bound to the wildcard address -
	// which is right, since the address that matters is the router's, not
	// this machine's - so LocalAddr reports 0.0.0.0 while the packet arrived
	// on a specific interface. The port is what identifies the mapping.
	if got := addrOf(r.conn).Port(); got != hit.Port() {
		t.Errorf("the sprayer reached port %d but the socket handed back is on %d",
			hit.Port(), got)
	}
	if r.from != addrOf(sprayConn) {
		t.Errorf("the hit came from %s, want the sprayer at %s", r.from, addrOf(sprayConn))
	}
}

// TestAProbeIsNotMistakenForQUIC. Both share one socket by necessity, so
// whoever reads it has to tell them apart without parsing either.
func TestAProbeIsNotMistakenForQUIC(t *testing.T) {
	p := NewProber(loopbackConn(t), "cafecafecafecafe")

	// A QUIC Initial: long header, version 1, connection ids, and enough
	// payload to be a real one. Full length matters - a short packet is
	// rejected on size alone, which would let a magic check that had stopped
	// working still look correct.
	quicish := []byte{0xc0, 0x00, 0x00, 0x00, 0x01, 0x08}
	for i := 0; i < 64; i++ {
		quicish = append(quicish, byte(i))
	}
	if p.Deliver(quicish, netip.MustParseAddrPort("127.0.0.1:1")) {
		t.Error("a QUIC packet was consumed as a probe; the handshake would never see it")
	}

	// And a datagram that is long enough but is not ours at all.
	junk := make([]byte, 64)
	for i := range junk {
		junk[i] = 0x5a
	}
	if p.Deliver(junk, netip.MustParseAddrPort("127.0.0.1:1")) {
		t.Error("an unrelated datagram was consumed as a probe")
	}
	if p.Deliver(nil, netip.MustParseAddrPort("127.0.0.1:1")) {
		t.Error("an empty datagram was consumed as a probe")
	}

	real := probe{kind: probeRequest, nonce: [8]byte{1}, from: fingerprint8("beefbeefbeefbeef")}.marshal()
	if !p.Deliver(real, netip.MustParseAddrPort("127.0.0.1:1")) {
		t.Error("a probe was not recognised, so punching would never answer")
	}
}

// TestUnreachableCarriesTheReason. The scheduler routes around a peer it
// cannot reach, and the user has to be able to tell a firewall from a bad
// moment - one is permanent and one is worth retrying.
func TestUnreachableCarriesTheReason(t *testing.T) {
	err := error(&Unreachable{Peer: "beefbeefbeefbeef", Reason: ReasonBothSymmetric, Tried: 4})
	u, ok := IsUnreachable(err)
	if !ok {
		t.Fatal("an Unreachable did not identify itself")
	}
	if u.Reason != ReasonBothSymmetric {
		t.Errorf("reason = %q", u.Reason)
	}
	if got := err.Error(); got == "" ||
		!contains(got, "beefbeef") || !contains(got, string(ReasonBothSymmetric)) {
		t.Errorf("message %q names neither the peer nor the reason", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// set assigns a package variable and returns a function restoring it.
func set[T any](p *T, v T) func() {
	old := *p
	*p = v
	return func() { *p = old }
}

// ephemeralRange is the port range the kernel allocates from, which is where
// OpenMany's sockets will land. Reading it rather than assuming it keeps this
// test honest on a machine whose range has been tuned.
func ephemeralRange(t *testing.T) (int, int) {
	t.Helper()
	b, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return 32768, 60999
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		return 32768, 60999
	}
	lo, err1 := strconv.Atoi(f[0])
	hi, err2 := strconv.Atoi(f[1])
	if err1 != nil || err2 != nil || lo >= hi {
		return 32768, 60999
	}
	return lo, hi
}
