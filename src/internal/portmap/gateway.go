package portmap

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/netip"
	"os"
	"strings"
)

// procNetRoute is where the kernel lists the routing table. A variable so
// tests can point it at a fixture: the alternative is a test that only runs
// on a machine whose real routing table happens to have the shape being
// described, which is the class of test this project has spent a while
// removing.
var procNetRoute = "/proc/net/route"

// defaultGateway is the router this machine sends everything through, and so
// the one to ask for a port.
//
// Read from the routing table rather than guessed from the local address.
// "192.168.0.1" is the usual answer and is wrong often enough to matter -
// phone tethers hand out .1 of their own subnet, some routers sit on .254,
// and a machine with several interfaces has a preference that only the
// routing table knows.
func defaultGateway() (netip.Addr, error) {
	f, err := os.Open(procNetRoute)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("reading the routing table: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return netip.Addr{}, fmt.Errorf("%s is empty", procNetRoute)
	}
	for sc.Scan() {
		// Iface Destination Gateway Flags RefCnt Use Metric Mask ...
		fields := strings.Fields(sc.Text())
		if len(fields) < 3 {
			continue
		}
		// The default route is the one to 0.0.0.0.
		if fields[1] != "00000000" {
			continue
		}
		addr, err := hexLE(fields[2])
		if err != nil {
			continue
		}
		if addr.IsUnspecified() {
			// A default route with no gateway is a point-to-point link -
			// there is no router in front to ask.
			continue
		}
		return addr, nil
	}
	if err := sc.Err(); err != nil {
		return netip.Addr{}, fmt.Errorf("reading the routing table: %w", err)
	}
	return netip.Addr{}, fmt.Errorf("no default route in %s", procNetRoute)
}

// hexLE decodes the little-endian hex the kernel writes addresses in.
func hexLE(s string) (netip.Addr, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return netip.Addr{}, err
	}
	if len(b) != 4 {
		return netip.Addr{}, fmt.Errorf("want 4 bytes, got %d", len(b))
	}
	v := binary.LittleEndian.Uint32(b)
	var out [4]byte
	binary.BigEndian.PutUint32(out[:], v)
	return netip.AddrFrom4(out), nil
}
