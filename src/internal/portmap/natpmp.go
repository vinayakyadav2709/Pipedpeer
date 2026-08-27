package portmap

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"time"
)

// Both protocols live on the same port. A router that speaks either answers
// there; one that speaks neither answers nothing at all, which is why every
// exchange here is bounded by a deadline rather than waiting for an error.
const mapPort = 5351

// mapPortOverride points the protocols at a different port under test. The
// alternative is a test that only runs on a machine whose real router
// happens to behave the way the test describes, which is not a test.
var mapPortOverride = 0

func askPort() int {
	if mapPortOverride != 0 {
		return mapPortOverride
	}
	return mapPort
}

// pcpMap asks for a mapping using PCP (RFC 6887).
//
// Tried before NAT-PMP because it is the successor and because a PCP server
// is required to reject a NAT-PMP-versioned request rather than misread it -
// so asking in this order cannot confuse a router that speaks both.
func pcpMap(ctx context.Context, gw netip.Addr, internal uint16, lifetime time.Duration) (netip.AddrPort, time.Duration, error) {
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: gw.AsSlice(), Port: askPort()})
	if err != nil {
		return netip.AddrPort{}, 0, err
	}
	defer conn.Close()

	// The client address the router should map. PCP has the client state it
	// explicitly, so a request relayed through anything is still unambiguous.
	local, ok := netip.AddrFromSlice(conn.LocalAddr().(*net.UDPAddr).IP)
	if !ok {
		return netip.AddrPort{}, 0, fmt.Errorf("no local address for the PCP request")
	}
	local16 := local.Unmap().As16()

	// A nonce ties the answer to this request, and re-using it is how a
	// renewal extends the same mapping instead of asking for a second one.
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return netip.AddrPort{}, 0, err
	}

	req := make([]byte, 0, 60)
	req = append(req, 2, 1, 0, 0) // version 2, request MAP, reserved
	req = binary.BigEndian.AppendUint32(req, uint32(lifetime.Seconds()))
	req = append(req, local16[:]...)
	req = append(req, nonce[:]...)
	req = append(req, 17, 0, 0, 0) // protocol UDP, reserved
	req = binary.BigEndian.AppendUint16(req, internal)
	req = binary.BigEndian.AppendUint16(req, internal) // suggested external
	var anyAddr [16]byte
	req = append(req, anyAddr[:]...) // no preference for the external address

	resp, err := exchange(ctx, conn, req, 60)
	if err != nil {
		return netip.AddrPort{}, 0, err
	}
	if resp[0] != 2 {
		return netip.AddrPort{}, 0, fmt.Errorf("not a PCP reply (version %d)", resp[0])
	}
	if resp[1] != 0x81 { // response bit set, opcode MAP
		return netip.AddrPort{}, 0, fmt.Errorf("unexpected PCP opcode %#x", resp[1])
	}
	if code := resp[3]; code != 0 {
		return netip.AddrPort{}, 0, fmt.Errorf("router refused the mapping: PCP result %d (%s)", code, pcpResult(code))
	}
	granted := time.Duration(binary.BigEndian.Uint32(resp[4:8])) * time.Second

	// The nonce must come back untouched, or this is an answer to somebody
	// else's request and the port in it belongs to them.
	for i := range nonce {
		if resp[24+i] != nonce[i] {
			return netip.AddrPort{}, 0, fmt.Errorf("PCP reply carries a different nonce")
		}
	}
	extPort := binary.BigEndian.Uint16(resp[42:44])
	var ext16 [16]byte
	copy(ext16[:], resp[44:60])
	ext := netip.AddrFrom16(ext16).Unmap()
	if !ext.IsValid() {
		return netip.AddrPort{}, 0, fmt.Errorf("PCP reply carries no external address")
	}
	return netip.AddrPortFrom(ext, extPort), granted, nil
}

func pcpResult(code byte) string {
	switch code {
	case 1:
		return "unsupported version"
	case 2:
		return "not authorised - port mapping is probably switched off on the router"
	case 3:
		return "the router has no resources to spare"
	case 8:
		return "the external port is already taken"
	default:
		return "see RFC 6887"
	}
}

// natpmpMap asks for a mapping using NAT-PMP (RFC 6886).
//
// The older protocol, and the one most consumer routers actually implement.
// It cannot state the client address, so it only works when sent directly to
// the router by the machine that wants the mapping.
func natpmpMap(ctx context.Context, gw netip.Addr, internal uint16, lifetime time.Duration) (netip.AddrPort, time.Duration, error) {
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: gw.AsSlice(), Port: askPort()})
	if err != nil {
		return netip.AddrPort{}, 0, err
	}
	defer conn.Close()

	// The external address is a separate question in NAT-PMP: the mapping
	// reply carries only a port. Ask first, because a router that will not
	// say where it lives cannot give a usable mapping either.
	ext, err := natpmpExternal(ctx, conn)
	if err != nil {
		return netip.AddrPort{}, 0, err
	}

	req := make([]byte, 0, 12)
	req = append(req, 0, 1, 0, 0) // version 0, op 1 (map UDP), reserved
	req = binary.BigEndian.AppendUint16(req, internal)
	req = binary.BigEndian.AppendUint16(req, internal) // suggested external
	req = binary.BigEndian.AppendUint32(req, uint32(lifetime.Seconds()))

	resp, err := exchange(ctx, conn, req, 16)
	if err != nil {
		return netip.AddrPort{}, 0, err
	}
	if resp[0] != 0 || resp[1] != 129 { // version 0, op 128+1
		return netip.AddrPort{}, 0, fmt.Errorf("not a NAT-PMP mapping reply (%d/%d)", resp[0], resp[1])
	}
	if code := binary.BigEndian.Uint16(resp[2:4]); code != 0 {
		return netip.AddrPort{}, 0, fmt.Errorf("router refused the mapping: NAT-PMP result %d", code)
	}
	extPort := binary.BigEndian.Uint16(resp[10:12])
	granted := time.Duration(binary.BigEndian.Uint32(resp[12:16])) * time.Second
	return netip.AddrPortFrom(ext, extPort), granted, nil
}

func natpmpExternal(ctx context.Context, conn *net.UDPConn) (netip.Addr, error) {
	resp, err := exchange(ctx, conn, []byte{0, 0}, 12)
	if err != nil {
		return netip.Addr{}, err
	}
	if resp[0] != 0 || resp[1] != 128 {
		return netip.Addr{}, fmt.Errorf("not a NAT-PMP address reply (%d/%d)", resp[0], resp[1])
	}
	if code := binary.BigEndian.Uint16(resp[2:4]); code != 0 {
		return netip.Addr{}, fmt.Errorf("NAT-PMP refused to say its address: result %d", code)
	}
	return netip.AddrFrom4([4]byte(resp[8:12])), nil
}

// exchange sends one request and waits for one reply of at least min bytes.
//
// Retried, because this is UDP to a device that may be busy, and bounded,
// because the common case for a router that speaks neither protocol is
// silence rather than a refusal. Three tries over roughly a second: long
// enough for a loaded router, short enough that a machine with no port
// mapping at all is not held up on every start.
func exchange(ctx context.Context, conn *net.UDPConn, req []byte, min int) ([]byte, error) {
	buf := make([]byte, 1100)
	delay := 250 * time.Millisecond
	var lastErr error
	for try := 0; try < 3; try++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		deadline := time.Now().Add(delay)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		_ = conn.SetDeadline(deadline)
		if _, err := conn.Write(req); err != nil {
			return nil, err
		}
		n, err := conn.Read(buf)
		if err != nil {
			lastErr = err
			delay *= 2
			continue
		}
		if n < min {
			lastErr = fmt.Errorf("reply is %d bytes, want at least %d", n, min)
			continue
		}
		return buf[:n], nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no reply")
	}
	return nil, fmt.Errorf("no usable reply from the router: %w", lastErr)
}
