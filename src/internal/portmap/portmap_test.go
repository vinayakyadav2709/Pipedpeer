package portmap

import (
	"context"
	"encoding/binary"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The routing table this machine happens to have is not the situation any of
// these describe, so the gateway read is pointed at fixtures. A test that
// only passes on the developer's own network is the kind this project keeps
// finding and deleting.
func writeRoute(t *testing.T, body string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "route")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	old := procNetRoute
	procNetRoute = p
	t.Cleanup(func() { procNetRoute = old })
}

// TestGatewayIsReadFromTheRoutingTable. Guessing .1 of the local subnet is
// wrong often enough to matter: tethers, routers on .254, and machines with
// several interfaces all disagree with the guess.
func TestGatewayIsReadFromTheRoutingTable(t *testing.T) {
	// 0100A8C0 little-endian is 192.168.0.1; FE00A8C0 is 192.168.0.254.
	writeRoute(t, "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\n"+
		"eth0\t0000A8C0\t00000000\t0001\t0\t0\t100\t00FFFFFF\n"+
		"eth0\t00000000\tFE00A8C0\t0003\t0\t0\t100\t00000000\n")

	gw, err := defaultGateway()
	if err != nil {
		t.Fatal(err)
	}
	if gw.String() != "192.168.0.254" {
		t.Errorf("gateway = %s, want 192.168.0.254", gw)
	}
}

// A default route with no gateway is a point-to-point link - there is no
// router in front to ask, and returning 0.0.0.0 would send the mapping
// request into nothing.
func TestAPointToPointLinkHasNoGatewayToAsk(t *testing.T) {
	writeRoute(t, "Iface\tDestination\tGateway\tFlags\n"+
		"tun0\t00000000\t00000000\t0001\n")

	if gw, err := defaultGateway(); err == nil {
		t.Errorf("got gateway %s from a route with none", gw)
	}
}

// TestADoubleNATMappingIsNotAdvertised is the check that matters on a phone
// tether: the router that answers is the phone, and the carrier's NAT above
// it was never asked. A mapping like that is real and locally useful, and
// publishing its address tells a peer to connect to something that means
// something else entirely on its own network.
func TestADoubleNATMappingIsNotAdvertised(t *testing.T) {
	cases := []struct {
		addr string
		pub  bool
		why  string
	}{
		{"203.0.113.9", true, "an ordinary public address"},
		{"100.64.0.7", false, "carrier-grade NAT (RFC 6598)"},
		{"100.127.255.1", false, "the far end of the carrier-grade range"},
		{"192.168.1.4", false, "private"},
		{"10.1.2.3", false, "private"},
		{"172.16.5.6", false, "private"},
		{"127.0.0.1", false, "loopback"},
		{"169.254.3.4", false, "link-local"},
		{"100.128.0.1", true, "just outside the carrier-grade range"},
		{"99.63.255.255", true, "just below it"},
	}
	for _, tc := range cases {
		got := isPublic(netip.MustParseAddr(tc.addr))
		if got != tc.pub {
			t.Errorf("isPublic(%s) = %v, want %v — %s", tc.addr, got, tc.pub, tc.why)
		}
	}
}

// fakeNATPMP answers NAT-PMP and PCP on a loopback port.
//
// speakPCP decides which: a router that speaks only the older protocol is
// the common case, and the fallback from PCP to NAT-PMP is the path most
// likely to be exercised in the field.
type fakeNATPMP struct {
	conn     *net.UDPConn
	speakPCP bool
	extIP    [4]byte
	extPort  uint16
	grant    uint32
	lastLife uint32
}

func startFakeNATPMP(t *testing.T, speakPCP bool, extIP [4]byte, extPort uint16, grant uint32) *fakeNATPMP {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeNATPMP{conn: conn, speakPCP: speakPCP, extIP: extIP, extPort: extPort, grant: grant}
	t.Cleanup(func() { conn.Close() })
	go f.serve()
	return f
}

func (f *fakeNATPMP) port() uint16 {
	return uint16(f.conn.LocalAddr().(*net.UDPAddr).Port)
}

func (f *fakeNATPMP) serve() {
	buf := make([]byte, 1500)
	for {
		n, from, err := f.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		req := buf[:n]
		var resp []byte
		switch {
		case req[0] == 2 && f.speakPCP && n >= 60:
			f.lastLife = binary.BigEndian.Uint32(req[4:8])
			resp = make([]byte, 0, 60)
			resp = append(resp, 2, 0x81, 0, 0) // version, response|MAP, reserved, success
			resp = binary.BigEndian.AppendUint32(resp, f.grant)
			resp = binary.BigEndian.AppendUint32(resp, 0) // epoch
			resp = append(resp, make([]byte, 12)...)      // reserved
			resp = append(resp, req[24:36]...)            // nonce echoed back
			resp = append(resp, 17, 0, 0, 0)
			resp = append(resp, req[40:42]...) // internal port
			resp = binary.BigEndian.AppendUint16(resp, f.extPort)
			ext := netip.AddrFrom4(f.extIP).As16()
			resp = append(resp, ext[:]...)
		case req[0] == 2:
			// A PCP request to a NAT-PMP-only router: refuse with
			// "unsupported version", which is what sends the caller on.
			resp = []byte{2, 0x81, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0}
			resp = append(resp, make([]byte, 48)...)
		case req[0] == 0 && req[1] == 0:
			resp = make([]byte, 0, 12)
			resp = append(resp, 0, 128, 0, 0)
			resp = binary.BigEndian.AppendUint32(resp, 0)
			resp = append(resp, f.extIP[:]...)
		case req[0] == 0 && req[1] == 1:
			f.lastLife = binary.BigEndian.Uint32(req[8:12])
			resp = make([]byte, 0, 16)
			resp = append(resp, 0, 129, 0, 0)
			resp = binary.BigEndian.AppendUint32(resp, 0)
			resp = append(resp, req[4:6]...) // internal port
			resp = binary.BigEndian.AppendUint16(resp, f.extPort)
			resp = binary.BigEndian.AppendUint32(resp, f.grant)
		default:
			continue
		}
		_, _ = f.conn.WriteToUDP(resp, from)
	}
}

// pointAtFake makes the protocol code talk to the fake instead of :5351.
func pointAtFake(t *testing.T, f *fakeNATPMP) netip.Addr {
	t.Helper()
	old := mapPortOverride
	mapPortOverride = int(f.port())
	t.Cleanup(func() { mapPortOverride = old })
	return netip.MustParseAddr("127.0.0.1")
}

// TestPCPMappingIsUsedWhenTheRouterSpeaksIt.
func TestPCPMappingIsUsedWhenTheRouterSpeaksIt(t *testing.T) {
	f := startFakeNATPMP(t, true, [4]byte{203, 0, 113, 9}, 41999, 1800)
	gw := pointAtFake(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ext, granted, err := pcpMap(ctx, gw, 38447, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if ext.String() != "203.0.113.9:41999" {
		t.Errorf("external = %s, want 203.0.113.9:41999", ext)
	}
	// The granted lifetime, not the requested one. A router that shortens a
	// lease and is renewed on the original schedule loses the mapping in
	// between, and nothing reports an error - packets just stop arriving.
	if granted != 1800*time.Second {
		t.Errorf("lifetime = %s, want the granted 30m", granted)
	}
}

// TestNATPMPIsTriedWhenPCPIsRefused. Most consumer routers speak only the
// older protocol, so this fallback is the one that will actually run.
func TestNATPMPIsTriedWhenPCPIsRefused(t *testing.T) {
	f := startFakeNATPMP(t, false, [4]byte{198, 51, 100, 4}, 40100, 7200)
	gw := pointAtFake(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, _, err := pcpMap(ctx, gw, 38447, time.Hour); err == nil {
		t.Fatal("PCP succeeded against a router that refuses it")
	}
	ext, granted, err := natpmpMap(ctx, gw, 38447, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if ext.String() != "198.51.100.4:40100" {
		t.Errorf("external = %s, want 198.51.100.4:40100", ext)
	}
	if granted != 7200*time.Second {
		t.Errorf("lifetime = %s, want 2h", granted)
	}
}

// TestAMappingIsGivenBackOnClose. A mapping left behind by every run
// accumulates on the router until it refuses new ones.
func TestAMappingIsGivenBackOnClose(t *testing.T) {
	f := startFakeNATPMP(t, false, [4]byte{198, 51, 100, 4}, 40100, 7200)
	gw := pointAtFake(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := natpmpMap(ctx, gw, 38447, time.Hour); err != nil {
		t.Fatal(err)
	}
	if f.lastLife == 0 {
		t.Fatal("precondition: the mapping request carried a zero lifetime")
	}
	if _, _, err := natpmpMap(ctx, gw, 38447, 0); err != nil {
		t.Fatal(err)
	}
	if f.lastLife != 0 {
		t.Errorf("delete asked for lifetime %d, want 0 — the mapping was not released", f.lastLife)
	}
}

// TestAWrongNonceIsRefused. The nonce is what ties a PCP answer to this
// request; accepting one with a different nonce means accepting somebody
// else's mapping and publishing a port that is not ours.
func TestAWrongNonceIsRefused(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n < 60 {
				continue
			}
			resp := make([]byte, 0, 60)
			resp = append(resp, 2, 0x81, 0, 0)
			resp = binary.BigEndian.AppendUint32(resp, 1800)
			resp = binary.BigEndian.AppendUint32(resp, 0)
			resp = append(resp, make([]byte, 12)...)
			resp = append(resp, make([]byte, 12)...) // a nonce of zeroes, not ours
			resp = append(resp, 17, 0, 0, 0)
			resp = append(resp, req2(buf[:n])...)
			resp = binary.BigEndian.AppendUint16(resp, 41999)
			ext := netip.AddrFrom4([4]byte{203, 0, 113, 9}).As16()
			resp = append(resp, ext[:]...)
			_, _ = conn.WriteToUDP(resp, from)
		}
	}()

	old := mapPortOverride
	mapPortOverride = conn.LocalAddr().(*net.UDPAddr).Port
	t.Cleanup(func() { mapPortOverride = old })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := pcpMap(ctx, netip.MustParseAddr("127.0.0.1"), 38447, time.Hour); err == nil {
		t.Error("a PCP reply with somebody else's nonce was accepted")
	}
}

func req2(req []byte) []byte { return req[40:42] }

// TestRenewalHappensBeforeTheLeaseLapses. Mappings are leases and routers do
// forget them; renewing at the half-life means one missed attempt still
// leaves as long again to recover.
func TestRenewalHappensBeforeTheLeaseLapses(t *testing.T) {
	if got := renewInterval(2 * time.Hour); got != time.Hour {
		t.Errorf("renewInterval(2h) = %s, want 1h", got)
	}
	if got := renewInterval(120 * time.Second); got != 60*time.Second {
		t.Errorf("renewInterval(120s) = %s, want 60s", got)
	}
	// Floored: a router granting a very short lease must not put this into
	// a renewal spin.
	if got := renewInterval(4 * time.Second); got != 30*time.Second {
		t.Errorf("renewInterval(4s) = %s, want the 30s floor", got)
	}
}

// TestNoMappingIsNotAFailure. Plenty of routers have port mapping switched
// off. The answer to that is to punch instead, so a daemon must start
// perfectly well without one.
func TestNoMappingIsNotAFailure(t *testing.T) {
	writeRoute(t, "Iface\tDestination\tGateway\tFlags\n")
	old := ssdpAddr
	ssdpAddr = "127.0.0.1:1" // nothing is listening
	t.Cleanup(func() { ssdpAddr = old })

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	m, err := Map(ctx, 38447)
	if err == nil {
		t.Fatal("a mapping was reported where none was possible")
	}
	if m.Kind != KindNone {
		t.Errorf("kind = %s, want none", m.Kind)
	}
	if m.String() != "none" {
		t.Errorf("String() = %q, want %q", m.String(), "none")
	}
}

// TestTheGrantedLeaseIsWhatGetsRenewed goes through Map, not the protocol
// call, because that is where the requested and granted lifetimes meet.
//
// A router that shortens a lease and is renewed on the schedule that was
// asked for loses the mapping in between, and nothing reports an error:
// packets simply stop arriving, which is the hardest kind of fault to find.
func TestTheGrantedLeaseIsWhatGetsRenewed(t *testing.T) {
	// Router grants 20 minutes against a 2 hour request.
	f := startFakeNATPMP(t, true, [4]byte{203, 0, 113, 9}, 41999, 1200)
	writeRoute(t, "Iface\tDestination\tGateway\tFlags\n"+
		"lo\t00000000\t0100007F\t0003\n") // gateway 127.0.0.1
	pointAtFake(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	m, err := mapWith(ctx, 38447, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if m.Lifetime != 20*time.Minute {
		t.Errorf("Lifetime = %s, want the granted 20m — renewing on the "+
			"requested 2h would let the mapping lapse unnoticed", m.Lifetime)
	}
	if !m.Public {
		t.Errorf("203.0.113.9 should be publishable")
	}
	if m.Kind != KindPCP {
		t.Errorf("Kind = %s, want pcp", m.Kind)
	}
}

// A router that grants nothing at all (lifetime 0) must fall back to the
// requested figure rather than scheduling renewals in a tight loop.
func TestAZeroGrantFallsBackToWhatWasAsked(t *testing.T) {
	f := startFakeNATPMP(t, true, [4]byte{203, 0, 113, 9}, 41999, 0)
	writeRoute(t, "Iface\tDestination\tGateway\tFlags\n"+
		"lo\t00000000\t0100007F\t0003\n")
	pointAtFake(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	m, err := mapWith(ctx, 38447, 90*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if m.Lifetime != 90*time.Minute {
		t.Errorf("Lifetime = %s, want the requested 90m", m.Lifetime)
	}
}
