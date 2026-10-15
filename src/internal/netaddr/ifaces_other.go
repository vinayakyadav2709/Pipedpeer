//go:build !linux

package netaddr

import (
	"errors"
	"net"
)

func dumpAddrs() ([]net.Interface, map[int][]net.Addr, error) {
	return nil, nil, errors.New("no netlink address dump on this platform")
}
