package adbmdns

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

func srv(owner string, port uint16) dnsmessage.Resource {
	return dnsmessage.Resource{
		Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(owner), Class: dnsmessage.ClassINET, TTL: 120},
		Body:   &dnsmessage.SRVResource{Port: port, Target: dnsmessage.MustNewName("phone.local.")},
	}
}

func pack(t *testing.T, ans, extra []dnsmessage.Resource) []byte {
	t.Helper()
	m := dnsmessage.Message{Header: dnsmessage.Header{Response: true}, Answers: ans, Additionals: extra}
	b, err := m.Pack()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

const (
	pairOwn = "adb-R58-x._adb-tls-pairing._tcp.local."
	connOwn = "adb-R58-x._adb-tls-connect._tcp.local."
)

func TestParseSRV(t *testing.T) {
	a := dnsmessage.Resource{
		Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName("phone.local."), Class: dnsmessage.ClassINET},
		Body:   &dnsmessage.AResource{A: [4]byte{10, 0, 0, 5}},
	}
	ptr := dnsmessage.Resource{
		Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(PairService), Class: dnsmessage.ClassINET},
		Body:   &dnsmessage.PTRResource{PTR: dnsmessage.MustNewName(pairOwn)},
	}
	cases := []struct {
		name       string
		pkt        []byte
		pair, conn []int
		wantErr    bool
	}{
		{"4.1 both in answer", pack(t, []dnsmessage.Resource{srv(pairOwn, 41913), srv(connOwn, 42965)}, nil), []int{41913}, []int{42965}, false},
		{"4.2 connect only", pack(t, []dnsmessage.Resource{srv(connOwn, 42965)}, nil), nil, []int{42965}, false},
		{"4.3 two phones", pack(t, []dnsmessage.Resource{srv(connOwn, 1111), srv("adb-other._adb-tls-connect._tcp.local.", 2222)}, nil), nil, []int{1111, 2222}, false},
		{"4.4 extra section", pack(t, []dnsmessage.Resource{ptr}, []dnsmessage.Resource{srv(pairOwn, 41913)}), []int{41913}, nil, false},
		{"4.7 garbage", []byte{1, 2, 3}, nil, nil, true},
		{"a only A record", pack(t, []dnsmessage.Resource{a}, nil), nil, nil, false},
		{"b PTR only", pack(t, []dnsmessage.Resource{ptr}, nil), nil, nil, false},
		{"c uppercase", pack(t, []dnsmessage.Resource{srv("ADB-X._ADB-TLS-PAIRING._TCP.LOCAL.", 7)}, nil), []int{7}, nil, false},
		{"f duplicate", pack(t, []dnsmessage.Resource{srv(connOwn, 9), srv(connOwn, 9)}, []dnsmessage.Resource{srv(connOwn, 9)}), nil, []int{9}, false},
	}
	for _, c := range cases {
		p, k, err := ParseSRV(c.pkt)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: err=%v", c.name, err)
		}
		if !reflect.DeepEqual(p, c.pair) || !reflect.DeepEqual(k, c.conn) {
			t.Errorf("%s: got %v %v want %v %v", c.name, p, k, c.pair, c.conn)
		}
	}
}

func TestParseGoodbye(t *testing.T) {
	zero := func(owner string) dnsmessage.Resource { r := srv(owner, 42965); r.Header.TTL = 0; return r }
	ptr0 := dnsmessage.Resource{
		Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(ConnectService), Class: dnsmessage.ClassINET},
		Body:   &dnsmessage.PTRResource{PTR: dnsmessage.MustNewName(connOwn)},
	}
	for _, c := range []struct {
		name string
		pkt  []byte
		want bool
	}{
		{"g1 TTL 0 connect SRV", pack(t, []dnsmessage.Resource{zero(connOwn)}, nil), true},
		{"g1b TTL 0 connect PTR", pack(t, []dnsmessage.Resource{ptr0}, nil), true},
		{"g2 TTL 120 connect SRV", pack(t, []dnsmessage.Resource{srv(connOwn, 42965)}, nil), false},
		{"g3 TTL 0 pairing SRV", pack(t, []dnsmessage.Resource{zero(pairOwn)}, nil), false},
		{"g4 garbage", []byte{1, 2, 3}, false},
	} {
		if got := ParseGoodbye(c.pkt); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
	// A goodbye's port is not a port to dial.
	if _, k, _ := ParseSRV(pack(t, []dnsmessage.Resource{zero(connOwn)}, nil)); len(k) != 0 {
		t.Errorf("TTL 0 SRV port returned: %v", k)
	}
}

func TestQuery(t *testing.T) {
	b, err := Query()
	if err != nil {
		t.Fatal(err)
	}
	var m dnsmessage.Message
	if err := m.Unpack(b); err != nil {
		t.Fatal(err)
	}
	if len(m.Questions) != 2 || m.Questions[0].Class != dnsmessage.ClassINET || m.Questions[1].Class != dnsmessage.ClassINET {
		t.Fatalf("questions %+v", m.Questions)
	}
}

// rig points the seams at a loopback socket; the returned func injects a packet.
func rig(t *testing.T, open ...int) func([]byte) {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	inj, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	oL, oD, oQ, oDial := Listen, dest, queryEvery, dialLocal
	t.Cleanup(func() { Listen, dest, queryEvery, dialLocal = oL, oD, oQ, oDial; inj.Close() })
	Listen = func() (net.PacketConn, error) { return pc, nil }
	dest = inj.LocalAddr()
	queryEvery = 20 * time.Millisecond
	dialLocal = func(p int) bool {
		for _, o := range open {
			if o == p {
				return true
			}
		}
		return false
	}
	return func(b []byte) { inj.WriteTo(b, pc.LocalAddr()) }
}

func TestDiscover(t *testing.T) {
	both := func(t *testing.T) []byte {
		return pack(t, []dnsmessage.Resource{srv(pairOwn, 41913), srv(connOwn, 42965)}, nil)
	}
	t.Run("4.1 both", func(t *testing.T) {
		send := rig(t, 41913, 42965)
		go send(both(t))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p, err := Discover(ctx, true)
		if err != nil || p != (Ports{Pair: 41913, Connect: 42965}) {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("d wantPair false returns at once", func(t *testing.T) {
		send := rig(t, 42965)
		go send(pack(t, []dnsmessage.Resource{srv(connOwn, 42965)}, nil))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p, err := Discover(ctx, false)
		if err != nil || p != (Ports{Connect: 42965}) {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("4.2 pairing absent keeps querying, returns found at timeout", func(t *testing.T) {
		send := rig(t, 42965)
		go send(pack(t, []dnsmessage.Resource{srv(connOwn, 42965)}, nil))
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		p, err := Discover(ctx, true)
		if !errors.Is(err, context.DeadlineExceeded) || p != (Ports{Connect: 42965}) {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("b PTR only", func(t *testing.T) {
		send := rig(t)
		ptr := dnsmessage.Resource{
			Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(PairService), Class: dnsmessage.ClassINET},
			Body:   &dnsmessage.PTRResource{PTR: dnsmessage.MustNewName(pairOwn)},
		}
		go send(pack(t, []dnsmessage.Resource{ptr}, nil))
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		p, err := Discover(ctx, false)
		if !errors.Is(err, context.DeadlineExceeded) || p != (Ports{}) {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("4.3 foreign port that does not dial is dropped", func(t *testing.T) {
		send := rig(t, 2222)
		go send(pack(t, []dnsmessage.Resource{srv(connOwn, 1111), srv(connOwn, 2222)}, nil))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p, err := Discover(ctx, false)
		if err != nil || p.Connect != 2222 {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("4.7 garbage skipped, listening continues", func(t *testing.T) {
		send := rig(t, 42965)
		go func() {
			send([]byte{9, 9})
			time.Sleep(30 * time.Millisecond)
			send(pack(t, []dnsmessage.Resource{srv(connOwn, 42965)}, nil))
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p, err := Discover(ctx, false)
		if err != nil || p.Connect != 42965 {
			t.Fatalf("%v %v", p, err)
		}
	})
	goodbye := func(t *testing.T) []byte {
		r := srv(connOwn, 42965)
		r.Header.TTL = 0
		return pack(t, []dnsmessage.Resource{r}, nil)
	}
	t.Run("g1 goodbye before the announcement: Goodbye set", func(t *testing.T) {
		send := rig(t, 42965)
		go func() {
			send(goodbye(t))
			time.Sleep(30 * time.Millisecond)
			send(pack(t, []dnsmessage.Resource{srv(connOwn, 42965)}, nil))
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p, err := Discover(ctx, false)
		if err != nil || p != (Ports{Connect: 42965, Goodbye: true}) {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("g2 announcement only: Goodbye clear", func(t *testing.T) {
		send := rig(t, 42965)
		go send(pack(t, []dnsmessage.Resource{srv(connOwn, 42965)}, nil))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p, err := Discover(ctx, false)
		if err != nil || p.Goodbye || p.Connect != 42965 {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("H1 DiscoverPair returns on the pairing port alone", func(t *testing.T) {
		send := rig(t) // nothing dials: the pairing port is not dialed
		go send(pack(t, []dnsmessage.Resource{srv(pairOwn, 41913)}, nil))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p, err := DiscoverPair(ctx)
		if err != nil || p != (Ports{Pair: 41913}) {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("H1 opposite: only the connect port announced, DiscoverPair times out", func(t *testing.T) {
		send := rig(t, 42965)
		go send(pack(t, []dnsmessage.Resource{srv(connOwn, 42965)}, nil))
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		p, err := DiscoverPair(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || p.Pair != 0 {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("M4 port announced before its listener is up is retried", func(t *testing.T) {
		send := rig(t)
		var calls atomic.Int32
		dialLocal = func(int) bool { return calls.Add(1) >= 3 }
		go func() {
			for i := 0; i < 10; i++ {
				send(pack(t, []dnsmessage.Resource{srv(connOwn, 42965)}, nil))
				time.Sleep(25 * time.Millisecond)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p, err := Discover(ctx, false)
		if err != nil || p.Connect != 42965 {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("M4 opposite: a port that never dials is never returned", func(t *testing.T) {
		send := rig(t)
		go send(pack(t, []dnsmessage.Resource{srv(connOwn, 42965)}, nil))
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		p, err := Discover(ctx, false)
		if !errors.Is(err, context.DeadlineExceeded) || p.Connect != 0 {
			t.Fatalf("%v %v", p, err)
		}
	})
	t.Run("e listen error", func(t *testing.T) {
		old := Listen
		defer func() { Listen = old }()
		boom := errors.New("boom")
		Listen = func() (net.PacketConn, error) { return nil, boom }
		_, err := Discover(context.Background(), true)
		if !errors.Is(err, ErrNoMulticast) {
			t.Fatalf("%v", err)
		}
	})
}
