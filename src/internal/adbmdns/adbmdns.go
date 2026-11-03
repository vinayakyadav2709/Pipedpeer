// Package adbmdns finds the Android Wireless debugging ports from inside
// Termux. It sends plain mDNS PTR queries on one multicast socket and reads
// SRV records from the answers. It does not enumerate interfaces, which Termux
// cannot do.
package adbmdns

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

const (
	PairService    = "_adb-tls-pairing._tcp.local."
	ConnectService = "_adb-tls-connect._tcp.local."
)

// Ports holds the discovered local ports; 0 means not found.
type Ports struct{ Pair, Connect int }

// ErrNoMulticast is returned when the multicast socket cannot be opened.
var ErrNoMulticast = errors.New("mDNS: cannot listen on 224.0.0.251:5353")

var groupAddr = &net.UDPAddr{IP: net.IPv4(224, 0, 0, 251), Port: 5353}

// Seams for tests.
var (
	// Listen opens the multicast socket.
	Listen = func() (net.PacketConn, error) {
		return net.ListenMulticastUDP("udp4", nil, groupAddr)
	}
	dest       net.Addr = groupAddr
	queryEvery          = 2 * time.Second
	dialLocal           = func(port int) bool {
		c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 300*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}
)

// Query packs one PTR query with both questions, QU bit clear.
func Query() ([]byte, error) {
	var qs []dnsmessage.Question
	for _, s := range []string{PairService, ConnectService} {
		qs = append(qs, dnsmessage.Question{
			Name:  dnsmessage.MustNewName(s),
			Type:  dnsmessage.TypePTR,
			Class: dnsmessage.ClassINET,
		})
	}
	m := dnsmessage.Message{Questions: qs}
	return m.Pack()
}

// ParseSRV returns the SRV ports of the pairing and connect services found in
// the Answer and Additional sections, deduplicated, owner names matched
// case-insensitively.
func ParseSRV(pkt []byte) (pair, connect []int, err error) {
	var p dnsmessage.Parser
	if _, err = p.Start(pkt); err != nil {
		return nil, nil, fmt.Errorf("mDNS packet: %w", err)
	}
	if err = p.SkipAllQuestions(); err != nil {
		return nil, nil, fmt.Errorf("mDNS packet: %w", err)
	}
	ans, err := p.AllAnswers()
	if err != nil {
		return nil, nil, fmt.Errorf("mDNS packet: %w", err)
	}
	if err = p.SkipAllAuthorities(); err != nil {
		return nil, nil, fmt.Errorf("mDNS packet: %w", err)
	}
	extra, err := p.AllAdditionals()
	if err != nil {
		return nil, nil, fmt.Errorf("mDNS packet: %w", err)
	}
	for _, r := range append(ans, extra...) {
		srv, ok := r.Body.(*dnsmessage.SRVResource)
		if !ok {
			continue
		}
		name := strings.ToLower(r.Header.Name.String())
		port := int(srv.Port)
		switch {
		case strings.HasSuffix(name, PairService):
			pair = addUnique(pair, port)
		case strings.HasSuffix(name, ConnectService):
			connect = addUnique(connect, port)
		}
	}
	return pair, connect, nil
}

func addUnique(l []int, v int) []int {
	for _, x := range l {
		if x == v {
			return l
		}
	}
	return append(l, v)
}

// Discover re-queries every 2 s and keeps ports that dial on 127.0.0.1. It
// returns once Connect (and Pair when wantPair) is set, or ctx.Err() with what
// was found.
func Discover(ctx context.Context, wantPair bool) (Ports, error) {
	var found Ports
	pc, err := Listen()
	if err != nil {
		return found, fmt.Errorf("%w: %w", ErrNoMulticast, err)
	}
	defer pc.Close()
	q, err := Query()
	if err != nil {
		return found, err
	}
	pkts := make(chan []byte, 16)
	go func() {
		defer close(pkts)
		buf := make([]byte, 9000)
		for {
			n, _, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			b := append([]byte(nil), buf[:n]...)
			select {
			case pkts <- b:
			default:
			}
		}
	}()
	done := func() bool { return found.Connect != 0 && (!wantPair || found.Pair != 0) }
	tried := map[int]bool{}
	pick := func(cands []int, cur int) int {
		if cur != 0 {
			return cur
		}
		for _, c := range cands {
			if tried[c] {
				continue
			}
			tried[c] = true
			if dialLocal(c) {
				return c
			}
		}
		return 0
	}
	send := func() {
		if _, err := pc.WriteTo(q, dest); err != nil {
			log.Printf("adbmdns: sending query: %v", err)
		}
	}
	send()
	tick := time.NewTicker(queryEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return found, ctx.Err()
		case <-tick.C:
			send()
		case b, ok := <-pkts:
			if !ok {
				return found, fmt.Errorf("%w: socket closed", ErrNoMulticast)
			}
			pair, conn, err := ParseSRV(b)
			if err != nil {
				log.Printf("adbmdns: skipping packet: %v", err)
				continue
			}
			found.Pair = pick(pair, found.Pair)
			found.Connect = pick(conn, found.Connect)
			if done() {
				return found, nil
			}
		}
	}
}
