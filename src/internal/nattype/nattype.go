// Package nattype works out whether two machines behind home routers can be
// made to talk directly, or whether their traffic has to be relayed.
//
// This is the question that decides the shape of internet mode, and it is
// worth answering with a measurement before choosing a transport. QUIC,
// WebRTC and a plain relay differ in what they cost to run and how much code
// they take; none of that matters if the networks in question refuse direct
// connections, and none of it matters much if they allow them. So: find out
// first.
//
// The test is the one STUN uses. A machine sends from a single local port to
// two different addresses on a public server and compares the source address
// the server saw each time. A router that keeps the same external port for
// both has an endpoint-independent mapping, and a peer told about that
// address can send to it - hole punching works. A router that allocates a new
// port per destination is symmetric, its mapping cannot be predicted by
// anyone, and traffic has to go through a relay.
package nattype

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// Mapping is what a router does with a local port.
type Mapping string

const (
	// EndpointIndependent: the same external address is presented to every
	// destination. A peer can be told where to send. Hole punching works.
	EndpointIndependent Mapping = "endpoint-independent"
	// AddressDependent: a new mapping per destination address, so the address
	// a third party is told is not the one they can reach. Relay required.
	AddressDependent Mapping = "address-dependent"
	// Unknown: not enough answers came back to say.
	Unknown Mapping = "unknown"
)

// Result is what a probe found.
type Result struct {
	Mapping Mapping
	// External is the address the server saw, from the first probe that
	// answered. Empty when nothing answered.
	External string
	// Seen lists every distinct external address observed, which is the
	// evidence for the verdict.
	Seen []string
	// Blocked is true when no probe answered at all: outbound UDP itself is
	// being dropped, which rules out every direct transport and is a
	// different problem from a difficult router.
	Blocked bool
	// Filter is what this router does with unsolicited inbound packets. Only
	// filled in when the reflector supports the test, which a public STUN
	// server does not.
	Filter Filtering
}

// Reflection is a reflector's answer.
type Reflection struct {
	// You is the source address the server observed.
	You string `json:"you"`
	// From is the address this reply was sent from, which is not always the
	// one that was asked - see the filtering test.
	From string `json:"from,omitempty"`
	// Ack marks the acknowledgement a reflector sends from the address that
	// was actually addressed, alongside the cross-address reply. Without it,
	// "this reflector cannot run the test" and "the cross-address reply was
	// dropped by the router" look identical - and the second is the answer
	// being looked for.
	Ack bool `json:"ack,omitempty"`
}

// probeMsg is what a client sends a reflector.
type probeMsg struct {
	Probe int `json:"probe"`
	// FromOther asks the reflector to answer from a different address than
	// the one addressed. A reply that still arrives means this router accepts
	// packets from hosts it has never sent to.
	FromOther bool `json:"from_other,omitempty"`
}

// Filtering is what a router does with inbound packets.
type Filtering string

const (
	// FilterOpen: packets arrive from hosts this machine never contacted.
	FilterOpen Filtering = "open"
	// FilterRestricted: only from hosts already sent to. Simultaneous hole
	// punching is supposed to work through this, and on a mobile carrier it
	// frequently does not.
	FilterRestricted Filtering = "restricted"
	// FilterUnknown: the reflector could not run the test.
	FilterUnknown Filtering = "unknown"
)

// Probe asks each server address where it sees us coming from, using one
// local port for all of them, and classifies the result.
//
// servers must be at least two distinct addresses - ideally two different
// hosts, since a router can key its mapping on address, port, or both, and
// two ports on one host only detects the port case.
func Probe(ctx context.Context, servers []string, timeout time.Duration) (Result, error) {
	if len(servers) < 2 {
		return Result{Mapping: Unknown}, fmt.Errorf(
			"need at least two server addresses to compare mappings, got %d", len(servers))
	}

	// One socket for every probe. Binding a fresh port per probe would
	// compare two unrelated mappings and call every router symmetric.
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return Result{Mapping: Unknown}, fmt.Errorf("opening a local port: %w", err)
	}
	defer conn.Close()

	// Filtering first, and this ordering is the whole validity of the test.
	// It asks one reflector to answer from another address, and that address
	// only counts as a stranger while this socket has never written to it -
	// which stops being true the moment the mapping probes below run against
	// every server. Run the other way round it reported "accepts strangers"
	// on a phone-tethered link that in fact accepted nothing at all.
	filter := FilterUnknown
	if len(servers) > 0 {
		if addr, err := net.ResolveUDPAddr("udp", servers[0]); err == nil {
			if f, ok := filterTest(ctx, conn, addr, timeout); ok {
				filter = f
			}
		}
	}

	seen := map[string]bool{}
	var order []string
	for _, s := range servers {
		addr, err := net.ResolveUDPAddr("udp", s)
		if err != nil {
			continue
		}
		// STUN first, falling back to this package's own reflector. A user
		// with no public server of their own still gets an answer, and a
		// cluster that runs `pipedpeer rendezvous` does not depend on
		// somebody else's uptime.
		ext, err := stunOnce(ctx, conn, addr, timeout)
		if err != nil || ext == "" {
			ext, err = reflectOnce(ctx, conn, addr, timeout)
		}
		if err == nil && ext != "" {
			if !seen[ext] {
				seen[ext] = true
				order = append(order, ext)
			}
		}
	}

	res := Result{Seen: order, Filter: filter}
	switch {
	case len(order) == 0:
		res.Mapping, res.Blocked = Unknown, true
	case len(order) == 1:
		res.Mapping, res.External = EndpointIndependent, order[0]
	default:
		// Different external addresses for different destinations: nobody can
		// predict which one a third peer would need.
		res.Mapping, res.External = AddressDependent, order[0]
	}

	return res, nil
}

// filterTest asks a reflector to answer from an address we have not written
// to. Reports whether the test could be run at all, since a plain STUN server
// cannot.
func filterTest(ctx context.Context, conn *net.UDPConn, addr *net.UDPAddr, timeout time.Duration) (Filtering, bool) {
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return FilterUnknown, false
	}
	body, _ := json.Marshal(probeMsg{Probe: 1, FromOther: true})
	acked := false
	// Several reads per attempt: the acknowledgement and the cross-address
	// reply are separate packets and either may arrive first.
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := conn.WriteToUDP(body, addr); err != nil {
			continue
		}
		for read := 0; read < 4; read++ {
			buf := make([]byte, 1024)
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				break
			}
			var r Reflection
			if json.Unmarshal(buf[:n], &r) != nil || r.From == "" {
				continue // not our reflector
			}
			if r.Ack && from.Port == addr.Port {
				acked = true
				continue
			}
			if from.Port != addr.Port {
				// Arrived from an address this socket never wrote to.
				return FilterOpen, true
			}
		}
		if acked {
			// The test definitely ran and the cross-address reply did not
			// survive, which is the router refusing a stranger.
			return FilterRestricted, true
		}
	}
	return FilterUnknown, false
}

// stunOnce asks one STUN server where it sees us from.
func stunOnce(ctx context.Context, conn *net.UDPConn, addr *net.UDPAddr, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return "", err
	}
	for attempt := 0; attempt < 3; attempt++ {
		msg, tid := stunRequest()
		if _, err := conn.WriteToUDP(msg, addr); err != nil {
			continue
		}
		buf := make([]byte, 1500)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		if ext, err := stunParse(buf[:n], tid); err == nil {
			return ext, nil
		}
	}
	return "", fmt.Errorf("no stun answer from %s", addr)
}

func reflectOnce(ctx context.Context, conn *net.UDPConn, addr *net.UDPAddr, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return "", err
	}
	// Retried because this is UDP and a single lost packet would be read as a
	// blocked network.
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := conn.WriteToUDP([]byte(`{"probe":1}`), addr); err != nil {
			continue
		}
		buf := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		var r Reflection
		if json.Unmarshal(buf[:n], &r) == nil && r.You != "" {
			return r.You, nil
		}
	}
	return "", fmt.Errorf("no answer from %s", addr)
}

// ReflectPair serves one socket and, when given a second, can answer from it
// instead.
//
// That second socket is what makes the filtering test possible. Mapping - the
// address a router presents to the world - is only half the question, and the
// half a public STUN server can answer. The other half is whether the router
// accepts a packet from a host it has never written to, which is what an
// introduced peer's first packet is. Answering from an address the client
// never contacted measures exactly that, and needs a reflector that has been
// asked to do it.
func ReflectPair(ctx context.Context, pc net.PacketConn, other net.PacketConn) error {
	defer pc.Close()

	go func() {
		<-ctx.Done()
		pc.Close()
	}()

	buf := make([]byte, 2048)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		var msg probeMsg
		_ = json.Unmarshal(buf[:n], &msg)

		if msg.FromOther && other != nil {
			// Two replies. The acknowledgement comes back the ordinary way and
			// proves the test ran; the other comes from an address the client
			// has never written to, and whether it arrives is the answer.
			ack, _ := json.Marshal(Reflection{
				You: from.String(), From: pc.LocalAddr().String(), Ack: true,
			})
			_, _ = pc.WriteTo(ack, from)

			cross, _ := json.Marshal(Reflection{
				You: from.String(), From: other.LocalAddr().String(),
			})
			_, _ = other.WriteTo(cross, from)
			continue
		}
		reply, _ := json.Marshal(Reflection{
			You:  from.String(),
			From: pc.LocalAddr().String(),
		})
		_, _ = pc.WriteTo(reply, from)
	}
}
