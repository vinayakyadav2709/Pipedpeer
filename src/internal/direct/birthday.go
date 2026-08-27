package direct

import (
	"context"
	"math/rand/v2"
	"net"
	"net/netip"
	"time"
)

// The hard case, and the one this project's own two machines are in.
//
// One side's router allocates a fresh external port for every destination it
// is asked about, so there is no address to publish: measured on the laptop
// here, a probe to one introducer port came back :38471 and the next :27486.
// The other side's router accepts inbound packets only from an address and
// port it has already written to. Neither is unusual, and together they
// defeat an ordinary punch - measured, 400 packets each way over forty
// seconds, nothing received in either direction.
//
// Nothing can predict the symmetric side's next port. But nobody has to: the
// two sides can collide instead. The symmetric side opens many sockets
// toward the same destination, so its router burns many external ports at
// once; the other side sprays probes across the port space. With 256
// mappings live and 600 probes over the ~64000 usable ports, the chance that
// no probe lands on any mapping is (1-256/64000)^600, which is under a
// tenth of a percent - the birthday paradox, doing the work prediction
// cannot.
//
// It costs one burst of a few hundred small packets, bounded below, and it
// is the difference between these two machines relaying every closure
// forever and connecting to each other directly.

// Tuned for the field, and variables rather than constants so a test can
// make a collision certain instead of merely likely. Ninety percent is the
// right trade when the alternative is relaying; it is a flaky test.
var (
	// birthdaySockets is how many mappings the symmetric side opens.
	birthdaySockets = 256
	// birthdayProbes is how many ports the other side tries.
	birthdayProbes = 600
	// birthdayPortLow and birthdayPortHigh bound where mappings are looked
	// for. Below 1024 ports are assigned rather than allocated, so no NAT
	// mapping will ever be there.
	birthdayPortLow  = 1024
	birthdayPortHigh = 65535
	// birthdayTick paces the spray. Several hundred packets in one burst
	// look exactly like a port scan, and a router that decides to drop the
	// flow drops the one packet that would have worked.
	birthdayTick = 2 * time.Millisecond
)

// SprayToward fires probes at random ports on a peer whose mapping cannot be
// predicted, hoping to hit one of the many it has open.
//
// Run by the side whose own address IS predictable, at the same time as the
// other side runs OpenMany. Returns the address a reply came from.
func (p *Prober) SprayToward(ctx context.Context, ip netip.Addr, known netip.AddrPort, budget time.Duration) (netip.AddrPort, error) {
	nonce, err := newNonce()
	if err != nil {
		return netip.AddrPort{}, err
	}
	ch := make(chan netip.AddrPort, 1)
	p.mu.Lock()
	p.waiting[nonce] = ch
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.waiting, nonce)
		p.mu.Unlock()
	}()

	msg := probe{kind: probeRequest, nonce: nonce, from: p.self}.marshal()
	deadline := time.Now().Add(budget)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	// The address the peer registered is still worth trying: it costs one
	// packet and it is the answer whenever the mapping happened to be
	// stable after all.
	if known.IsValid() {
		_, _ = p.conn.WriteToUDPAddrPort(msg, known)
	}

	tick := time.NewTicker(birthdayTick)
	defer tick.Stop()

	sent := 0
	for sent < birthdayProbes {
		select {
		case addr := <-ch:
			return addr, nil
		case <-ctx.Done():
			return netip.AddrPort{}, ctx.Err()
		case <-tick.C:
		}
		port := uint16(birthdayPortLow + rand.IntN(birthdayPortHigh-birthdayPortLow))
		_, _ = p.conn.WriteToUDPAddrPort(msg, netip.AddrPortFrom(ip, port))
		sent++
	}

	// Everything is out; wait out the remaining budget for a straggler.
	select {
	case addr := <-ch:
		return addr, nil
	case <-ctx.Done():
		return netip.AddrPort{}, ctx.Err()
	}
}

// OpenMany burns many external ports toward one destination, so a peer
// spraying the port space has many chances to hit one.
//
// Run by the side whose router allocates unpredictably, at the same time as
// the other side runs SprayToward. The sockets are separate from the shared
// one on purpose: the whole point is to create many mappings, and the shared
// socket can only ever have one per destination.
//
// A hit arrives as a probe on one of these sockets. That socket's mapping is
// the one the peer can reach, so the connection has to be made from it -
// which is why the working socket is returned rather than just a result.
func OpenMany(ctx context.Context, peer netip.AddrPort, self string, budget time.Duration) (*net.UDPConn, netip.AddrPort, error) {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	type hit struct {
		conn *net.UDPConn
		from netip.AddrPort
	}
	found := make(chan hit, 1)

	conns := make([]*net.UDPConn, 0, birthdaySockets)
	var winner *net.UDPConn
	defer func() {
		// Everything except the winner is closed on the way out; holding
		// them would keep hundreds of mappings alive on the router for
		// nothing, and a router with a mapping table full of ours is a
		// router that stops granting them to anyone.
		for _, c := range conns {
			if c != winner {
				_ = c.Close()
			}
		}
	}()

	msg := probe{kind: probeRequest, nonce: [8]byte{}, from: fingerprint8(self)}.marshal()

	for i := 0; i < birthdaySockets; i++ {
		c, err := net.ListenUDP("udp4", &net.UDPAddr{})
		if err != nil {
			break // out of file descriptors; carry on with what opened
		}
		conns = append(conns, c)

		// One packet out per socket, which is what makes the router allocate
		// a mapping for it. Where it lands does not matter - only that the
		// mapping exists for the peer's spray to hit.
		_, _ = c.WriteToUDPAddrPort(msg, peer)

		go func(c *net.UDPConn) {
			buf := make([]byte, 1500)
			for {
				_ = c.SetReadDeadline(time.Now().Add(budget))
				n, from, err := c.ReadFromUDPAddrPort(buf)
				if err != nil {
					return
				}
				pr, ok := parseProbe(buf[:n])
				if !ok || pr.kind != probeRequest {
					continue
				}
				// Reply so the far side knows which of its probes landed,
				// then hand this socket up: its mapping is the reachable one.
				reply := probe{kind: probeReply, nonce: pr.nonce, from: fingerprint8(self)}
				_, _ = c.WriteToUDPAddrPort(reply.marshal(), from)
				select {
				case found <- hit{conn: c, from: from}:
				default:
				}
				return
			}
		}(c)
	}

	if len(conns) == 0 {
		return nil, netip.AddrPort{}, ctx.Err()
	}

	select {
	case h := <-found:
		winner = h.conn
		return h.conn, h.from, nil
	case <-ctx.Done():
		return nil, netip.AddrPort{}, ctx.Err()
	}
}
