package portmap

import "net/netip"

// DefaultGateway is the router this machine sends everything through, read
// from the routing table; an error means there is no default route.
func DefaultGateway() (netip.Addr, error) { return defaultGateway() }
