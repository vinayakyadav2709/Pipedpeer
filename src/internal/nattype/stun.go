package nattype

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
)

// A minimal STUN client, so this works without a server of one's own.
//
// The reflector in this package is fine when there is somewhere public to run
// it, and a user setting pipedpeer up on a laptop has nowhere. Every operating
// system already ships against public STUN servers for exactly this question,
// and speaking their protocol is eighty lines: a fixed header, and one
// attribute in the reply. Pulling in a STUN library for that would be more
// dependency than code.
//
// Only what is needed is implemented - a binding request and the mapped
// address out of the response. No authentication, no ICE, no attributes
// beyond the one that answers the question.

const (
	stunBindingRequest  = 0x0001
	stunBindingResponse = 0x0101
	stunMagicCookie     = 0x2112A442
	attrMappedAddress   = 0x0001
	attrXorMappedAddr   = 0x0020
)

// DefaultSTUNServers are public reflectors on distinct hosts. Two different
// hosts, not two ports on one: a router can key its mapping on the
// destination address, the port, or both, and two ports on a single host
// would miss the address case and call a symmetric router punchable.
var DefaultSTUNServers = []string{
	"stun.l.google.com:19302",
	"stun.cloudflare.com:3478",
}

// stunRequest builds a binding request and returns it with its transaction ID.
func stunRequest() ([]byte, [12]byte) {
	var tid [12]byte
	_, _ = rand.Read(tid[:])

	msg := make([]byte, 20)
	binary.BigEndian.PutUint16(msg[0:], stunBindingRequest)
	binary.BigEndian.PutUint16(msg[2:], 0) // no attributes
	binary.BigEndian.PutUint32(msg[4:], stunMagicCookie)
	copy(msg[8:], tid[:])
	return msg, tid
}

// stunParse pulls the mapped address out of a binding response.
func stunParse(msg []byte, tid [12]byte) (string, error) {
	if len(msg) < 20 {
		return "", fmt.Errorf("stun reply is %d bytes, too short for a header", len(msg))
	}
	if binary.BigEndian.Uint16(msg[0:]) != stunBindingResponse {
		return "", fmt.Errorf("stun reply is not a binding response")
	}
	if binary.BigEndian.Uint32(msg[4:]) != stunMagicCookie {
		return "", fmt.Errorf("stun reply has the wrong magic cookie")
	}
	// The transaction ID ties the reply to our request. Without checking it a
	// stray packet from anything on the network would be read as our answer.
	for i := 0; i < 12; i++ {
		if msg[8+i] != tid[i] {
			return "", fmt.Errorf("stun reply is for a different transaction")
		}
	}

	length := int(binary.BigEndian.Uint16(msg[2:]))
	if 20+length > len(msg) {
		length = len(msg) - 20
	}
	body := msg[20 : 20+length]

	for len(body) >= 4 {
		typ := binary.BigEndian.Uint16(body[0:])
		alen := int(binary.BigEndian.Uint16(body[2:]))
		if 4+alen > len(body) {
			break
		}
		val := body[4 : 4+alen]
		switch typ {
		case attrXorMappedAddr:
			if addr, err := parseXorMapped(val); err == nil {
				return addr, nil
			}
		case attrMappedAddress:
			if addr, err := parseMapped(val); err == nil {
				return addr, nil
			}
		}
		// Attributes are padded to a multiple of four.
		advance := 4 + alen
		if pad := alen % 4; pad != 0 {
			advance += 4 - pad
		}
		if advance > len(body) {
			break
		}
		body = body[advance:]
	}
	return "", fmt.Errorf("stun reply carried no mapped address")
}

func parseXorMapped(v []byte) (string, error) {
	if len(v) < 8 {
		return "", fmt.Errorf("short xor-mapped-address")
	}
	// v[0] is padding, v[1] the family.
	port := binary.BigEndian.Uint16(v[2:]) ^ uint16(stunMagicCookie>>16)
	switch v[1] {
	case 0x01: // IPv4
		ip := make(net.IP, 4)
		binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(v[4:])^stunMagicCookie)
		return net.JoinHostPort(ip.String(), fmt.Sprint(port)), nil
	case 0x02: // IPv6: XORed with the cookie followed by the transaction id
		return "", fmt.Errorf("ipv6 mapped address not handled")
	}
	return "", fmt.Errorf("unknown address family %d", v[1])
}

func parseMapped(v []byte) (string, error) {
	if len(v) < 8 || v[1] != 0x01 {
		return "", fmt.Errorf("unsupported mapped-address")
	}
	port := binary.BigEndian.Uint16(v[2:])
	ip := net.IP(v[4:8])
	return net.JoinHostPort(ip.String(), fmt.Sprint(port)), nil
}
