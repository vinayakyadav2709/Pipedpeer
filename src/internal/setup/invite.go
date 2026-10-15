package setup

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/invite"
	"github.com/pipedpeer/pipedpeer/internal/netaddr"
)

// maxInviteAddrs keeps the code short enough to paste in one line.
const maxInviteAddrs = 6

// newToken is authtoken.Generate; a variable for tests.
var newToken = authtoken.Generate

// EnsureToken creates the cluster secret when this machine has none. created
// is true when it did: this machine is the first of a new cluster.
func EnsureToken() (created bool, err error) {
	if authtoken.Current() != "" {
		return false, nil
	}
	tok := newToken()
	if tok == "" {
		return false, errors.New("could not generate a cluster secret (no randomness)")
	}
	if err := authtoken.Set(tok); err != nil {
		return false, fmt.Errorf("saving the cluster secret: %w", err)
	}
	return true, nil
}

// InviteCode is the code `pipedpeer connect` takes: the secret plus where to
// find an introducer - this machine's LAN addresses first, then the
// introducers it knows - at most maxInviteAddrs, introducer port.
func InviteCode() (string, error) {
	tok := authtoken.Current()
	if tok == "" {
		return "", errors.New("this machine is in no cluster yet (no secret)")
	}
	cfg, err := clustercfg.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: could not read %s (%v); the invite lists this machine's addresses only\n", clustercfg.Path(), err)
	}
	own := lanIPs()
	if publicHint(cfg.PublicAddress, own) {
		fmt.Fprintln(os.Stderr, "pipedpeer: this machine has only private addresses; peers off this network "+
			"need `pipedpeer setup --public-address <addr>` here, or Tailscale on every machine")
	}
	return invite.Encode(invite.Invite{Secret: tok, Addrs: inviteAddrs(cfg.PublicAddress, own, cfg.Introducers)}), nil
}

// publicHint reports whether no address in the invite can be reached from
// off this network: no public address set and every own IP private.
func publicHint(public string, own []string) bool {
	if public != "" {
		return false
	}
	for _, s := range own {
		if ip := net.ParseIP(s); ip != nil && !ip.IsPrivate() {
			return false
		}
	}
	return true
}

// inviteAddrs joins the public address (first), own IPs (given the
// introducer port) and known introducers, deduplicated, capped.
func inviteAddrs(public string, own, known []string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(a string) {
		if a == "" || seen[a] || len(out) >= maxInviteAddrs {
			return
		}
		seen[a] = true
		out = append(out, a)
	}
	if public != "" {
		if n, err := clustercfg.Normalize(public); err == nil {
			add(n)
		} else {
			fmt.Fprintf(os.Stderr, "pipedpeer: skipping public_address %q: %v\n", public, err)
		}
	}
	for _, ip := range own {
		add(net.JoinHostPort(ip, strconv.Itoa(clustercfg.DefaultPort)))
	}
	for _, k := range known {
		if n, err := clustercfg.Normalize(k); err == nil {
			add(n)
		} else {
			fmt.Fprintf(os.Stderr, "pipedpeer: skipping saved introducer %q: %v\n", k, err)
		}
	}
	return out
}

// virtualPrefixes are interfaces no other machine can reach this one through.
var virtualPrefixes = []string{"docker", "br-", "veth", "virbr", "podman", "cni", "flannel", "lxc", "vnet"}

// lanIPs is this machine's IPv4 addresses on up, non-loopback, non-container
// interfaces (IPv6 is parked).
func lanIPs() []string {
	ifs, err := netaddr.Interfaces()
	if err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: could not list network interfaces: %v\n", err)
		return nil
	}
	var out []string
	for _, ifc := range ifs {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 || isVirtual(ifc.Name) {
			continue
		}
		addrs, err := netaddr.Addrs(ifc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pipedpeer: could not read addresses of %s: %v\n", ifc.Name, err)
			continue
		}
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.To4() != nil && !n.IP.IsLinkLocalUnicast() {
				out = append(out, n.IP.String())
			}
		}
	}
	return out
}

func isVirtual(name string) bool {
	for _, p := range virtualPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// SetPublicAddress saves this machine's public address (host or host:port)
// for invites; empty clears it.
func SetPublicAddress(addr string) error {
	cfg, err := clustercfg.Load()
	if err != nil {
		return fmt.Errorf("reading %s: %w", clustercfg.Path(), err)
	}
	if addr != "" {
		n, err := clustercfg.Normalize(addr)
		if err != nil {
			return fmt.Errorf("--public-address %s: %w", addr, err)
		}
		addr = n
	}
	cfg.PublicAddress = addr
	return clustercfg.Save(cfg)
}
