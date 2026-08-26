package nattype

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"
)

// fakeReflector answers with whatever address it is told to claim, so a test
// can stage a router that rewrites the source port per destination without
// needing such a router.
func fakeReflector(t *testing.T, claim func(from net.Addr) string) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	go func() {
		buf := make([]byte, 1024)
		for {
			_, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			reply, _ := json.Marshal(Reflection{You: claim(from)})
			_, _ = pc.WriteTo(reply, from)
		}
	}()
	return pc.LocalAddr().String()
}

// TestEndpointIndependentMappingIsPunchable. When every destination sees the
// same external address, a third peer can be told where to send, so the two
// machines can be introduced and talk directly.
func TestEndpointIndependentMappingIsPunchable(t *testing.T) {
	same := func(net.Addr) string { return "203.0.113.7:41641" }
	a := fakeReflector(t, same)
	b := fakeReflector(t, same)

	got, err := Probe(context.Background(), []string{a, b}, time.Second)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if got.Mapping != EndpointIndependent {
		t.Errorf("mapping = %q, want %q (both servers saw the same address)",
			got.Mapping, EndpointIndependent)
	}
	if got.External != "203.0.113.7:41641" {
		t.Errorf("external = %q", got.External)
	}
	if got.Blocked {
		t.Error("reported blocked though both servers answered")
	}
}

// TestAddressDependentMappingNeedsARelay. A router that allocates a new port
// per destination gives a third peer an address that was never valid for it,
// so no amount of coordination produces a direct path.
func TestAddressDependentMappingNeedsARelay(t *testing.T) {
	n := 0
	varying := func(net.Addr) string {
		n++
		return "203.0.113.7:5000" + string(rune('0'+n))
	}
	a := fakeReflector(t, varying)
	b := fakeReflector(t, varying)

	got, err := Probe(context.Background(), []string{a, b}, time.Second)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if got.Mapping != AddressDependent {
		t.Errorf("mapping = %q, want %q (each server saw a different port)",
			got.Mapping, AddressDependent)
	}
	if len(got.Seen) < 2 {
		t.Errorf("only %d distinct address(es) recorded; the evidence for the "+
			"verdict is missing", len(got.Seen))
	}
}

// TestNoAnswerIsBlockedNotSymmetric. Outbound UDP being dropped is a different
// problem from a difficult router, and reporting it as one sends the operator
// to configure the wrong thing.
func TestNoAnswerIsBlockedNotSymmetric(t *testing.T) {
	// Two addresses nothing is listening on. Reserved for documentation, so
	// no real host answers either.
	got, err := Probe(context.Background(), []string{"192.0.2.1:9", "192.0.2.2:9"}, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !got.Blocked {
		t.Error("nothing answered, but the result does not say the path is blocked")
	}
	if got.Mapping != Unknown {
		t.Errorf("mapping = %q, want %q: with no answers there is nothing to "+
			"classify, and guessing would send the user to fix the wrong thing",
			got.Mapping, Unknown)
	}
}

// TestOneServerIsNotEnough. The whole method is a comparison; with a single
// server there is nothing to compare and any verdict would be invented.
func TestOneServerIsNotEnough(t *testing.T) {
	a := fakeReflector(t, func(net.Addr) string { return "203.0.113.7:1" })
	if _, err := Probe(context.Background(), []string{a}, time.Second); err == nil {
		t.Error("a single server was accepted; the classification would be a guess")
	}
}

// TestProbeUsesOneLocalPort is the detail the whole test rests on. Opening a
// fresh socket per probe compares two unrelated mappings, so every router in
// the world classifies as symmetric and internet mode would always relay.
func TestProbeUsesOneLocalPort(t *testing.T) {
	ports := make(chan string, 4)
	record := func(from net.Addr) string {
		select {
		case ports <- from.String():
		default:
		}
		return "203.0.113.7:41641"
	}
	a := fakeReflector(t, record)
	b := fakeReflector(t, record)

	if _, err := Probe(context.Background(), []string{a, b}, time.Second); err != nil {
		t.Fatalf("probe: %v", err)
	}
	close(ports)
	var first string
	for p := range ports {
		if first == "" {
			first = p
			continue
		}
		if p != first {
			t.Errorf("probes came from %s and %s; they must share one local port "+
				"or the comparison is meaningless", first, p)
		}
	}
	if first == "" {
		t.Fatal("no probe reached a reflector")
	}
}

// TestReflectAnswersWithTheSourceAddress covers the server half against the
// client half, so the two cannot drift apart.
func TestReflectAnswersWithTheSourceAddress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	// Serve on the socket already bound, so there is no window between the
	// port being known and the reflector answering on it.
	go func() { _ = ReflectOn(ctx, pc) }()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	target, _ := net.ResolveUDPAddr("udp", addr)

	got, err := reflectOnce(ctx, conn, target, 2*time.Second)
	if err != nil {
		t.Fatalf("no reflection: %v", err)
	}
	if got != conn.LocalAddr().String() {
		t.Errorf("reflector said %q, want %q", got, conn.LocalAddr().String())
	}
}

// TestPunchKeepsSendingAfterItSucceeds covers a flaw that made a working path
// look like a broken one.
//
// Stopping the moment the first packet lands is the obvious thing and it
// breaks the other side: whoever succeeds first goes quiet, and the peer -
// whose router may not have had its state ready yet - hears nothing and
// reports failure. Measured exactly that way against a public host: one side
// declared success in 0.4s and exited, the other sent 199 packets into
// silence and called the network unpunchable.
func TestPunchKeepsSendingAfterItSucceeds(t *testing.T) {
	// A peer that answers once and then only counts.
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	after := make(chan int, 1)
	go func() {
		buf := make([]byte, 1500)
		var got int
		var replied bool
		for {
			if err := peer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return
			}
			_, from, err := peer.ReadFromUDP(buf)
			if err != nil {
				select {
				case after <- got:
				default:
				}
				return
			}
			if !replied {
				replied = true
				_, _ = peer.WriteToUDP([]byte("punch-back"), from)
				got = 0 // start counting only after we have answered
				continue
			}
			got++
			if got >= 5 {
				select {
				case after <- got:
				default:
				}
				return
			}
		}
	}()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ok, _, st, err := Punch(context.Background(), conn, peer.LocalAddr().String(), 10*time.Second)
	if err != nil {
		t.Fatalf("punch: %v", err)
	}
	if !ok {
		t.Fatalf("punch failed though the peer answered: %+v", st)
	}
	select {
	case n := <-after:
		if n < 3 {
			t.Errorf("only %d packet(s) sent after the path opened; the peer needs "+
				"to keep hearing us long enough to confirm its own side", n)
		}
	case <-time.After(2 * time.Second):
		t.Error("the peer saw nothing after the path opened: we went quiet immediately")
	}
}

// TestFilteringIsMeasuredSeparatelyFromMapping covers the distinction that
// made an earlier verdict wrong.
//
// Mapping - the address a router presents - is the half a public STUN server
// can answer, and net-check used to report "no relay needed" on the strength
// of it alone. Whether the router accepts a packet from a host it has never
// written to is a different question and the one an introduced peer's first
// packet depends on. Measured on a phone-tethered machine: endpoint-
// independent mapping, and nothing inbound arrives at all.
func TestFilteringIsMeasuredSeparatelyFromMapping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = ReflectPair(ctx, a, b) }()
	go func() { _ = ReflectPair(ctx, b, a) }()

	got, err := Probe(ctx, []string{a.LocalAddr().String(), b.LocalAddr().String()}, 2*time.Second)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	// Loopback accepts anything, so the cross-address reply arrives.
	if got.Filter != FilterOpen {
		t.Errorf("filter = %q, want %q: the reflector answered from an address "+
			"this socket never wrote to, and on loopback that must arrive",
			got.Filter, FilterOpen)
	}
}

// TestFilteringUnknownAgainstAPlainReflector. A reflector that cannot answer
// from elsewhere - or a public STUN server - leaves the question open, and
// "unknown" has to be distinguishable from "restricted" or the report claims
// a measurement it never made.
func TestFilteringUnknownAgainstAPlainReflector(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// No sibling: these cannot run the filtering test.
	go func() { _ = ReflectPair(ctx, a, nil) }()
	go func() { _ = ReflectPair(ctx, b, nil) }()

	got, err := Probe(ctx, []string{a.LocalAddr().String(), b.LocalAddr().String()}, time.Second)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if got.Mapping != EndpointIndependent {
		t.Errorf("mapping = %q; the reflectors both saw the same source", got.Mapping)
	}
	if got.Filter == FilterOpen {
		t.Error("reported open filtering though no reflector answered from " +
			"anywhere else; that verdict was never measured")
	}
}

// TestFilterTestRunsBeforeTheMappingProbes pins down the ordering the test's
// validity rests on.
//
// The filtering test asks one reflector to answer from another address, and
// that address is only a stranger while this socket has never written to it.
// The mapping probes write to every server. Run in the wrong order the test
// measures nothing and says "accepts strangers" about everything - which it
// did, on a phone-tethered link that accepted nothing at all.
func TestFilterTestRunsBeforeTheMappingProbes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// b records whether it had been written to before it was asked to answer
	// on a's behalf.
	writtenToB := make(chan struct{}, 1)
	crossReply := make(chan bool, 1)

	b, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	a, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	go func() {
		buf := make([]byte, 1024)
		for {
			_, from, err := b.ReadFrom(buf)
			if err != nil {
				return
			}
			select {
			case writtenToB <- struct{}{}:
			default:
			}
			reply, _ := json.Marshal(Reflection{You: from.String(), From: b.LocalAddr().String()})
			_, _ = b.WriteTo(reply, from)
		}
	}()
	go func() {
		buf := make([]byte, 1024)
		for {
			n, from, err := a.ReadFrom(buf)
			if err != nil {
				return
			}
			var msg probeMsg
			_ = json.Unmarshal(buf[:n], &msg)
			if msg.FromOther {
				// Record whether b had already been contacted directly when
				// the cross-reply was requested.
				var already bool
				select {
				case <-writtenToB:
					already = true
					writtenToB <- struct{}{}
				default:
				}
				select {
				case crossReply <- already:
				default:
				}
				reply, _ := json.Marshal(Reflection{You: from.String(), From: b.LocalAddr().String()})
				_, _ = b.WriteTo(reply, from)
				continue
			}
			reply, _ := json.Marshal(Reflection{You: from.String(), From: a.LocalAddr().String()})
			_, _ = a.WriteTo(reply, from)
		}
	}()

	if _, err := Probe(ctx, []string{a.LocalAddr().String(), b.LocalAddr().String()}, time.Second); err != nil {
		t.Fatalf("probe: %v", err)
	}

	select {
	case already := <-crossReply:
		if already {
			t.Error("the filtering test ran after the mapping probes had already " +
				"written to the second address, so the reply was not from a stranger " +
				"and the verdict measures nothing")
		}
	case <-time.After(time.Second):
		t.Fatal("the filtering test never ran")
	}
}
