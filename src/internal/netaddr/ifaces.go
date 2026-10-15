package netaddr

import (
	"log"
	"net"
	"runtime"
	"sync"
)

// Every interface listing goes through here. On Android Go's netlink dumps
// fail ("netlinkrib: invalid argument", Android 11+ refuses RTM_GETLINK and
// the bind) or, on the S20 FE under proot, Interface.Addrs blocks in recvfrom
// forever: the daemon's poll loop hung in LAN discovery and every node went
// stale. So Android always uses dumpAddrs (unbound RTM_GETADDR, bounded
// wait); elsewhere net is used and dumpAddrs only when net returns an error.

var fallbackOnce sync.Once

// useNet reports whether to ask net first.
func useNet() bool { return runtime.GOOS != "android" }

func fellBack(what string, err error) {
	fallbackOnce.Do(func() {
		log.Printf("netaddr: %s refused (%v); listing addresses with a netlink address dump instead", what, err)
	})
}

// Interfaces is net.Interfaces. From dumpAddrs it lists only interfaces that
// have an address, with name and flags but no MTU or hardware address.
func Interfaces() ([]net.Interface, error) {
	if useNet() {
		ifs, err := net.Interfaces()
		if err == nil {
			return ifs, nil
		}
		fellBack("net.Interfaces", err)
	}
	ifs, _, err := dumpAddrs()
	if err != nil {
		log.Printf("netaddr: cannot list network interfaces: %v", err)
	}
	return ifs, err
}

// Addrs is ifi.Addrs().
func Addrs(ifi net.Interface) ([]net.Addr, error) {
	if useNet() {
		addrs, err := ifi.Addrs()
		if err == nil {
			return addrs, nil
		}
		fellBack("Interface.Addrs", err)
	}
	_, byIndex, err := dumpAddrs()
	if err != nil {
		log.Printf("netaddr: cannot read addresses of %s: %v", ifi.Name, err)
	}
	return byIndex[ifi.Index], err
}

// InterfaceAddrs is net.InterfaceAddrs.
func InterfaceAddrs() ([]net.Addr, error) {
	if useNet() {
		addrs, err := net.InterfaceAddrs()
		if err == nil {
			return addrs, nil
		}
		fellBack("net.InterfaceAddrs", err)
	}
	var addrs []net.Addr
	_, byIndex, err := dumpAddrs()
	if err != nil {
		log.Printf("netaddr: cannot list interface addresses: %v", err)
	}
	for _, a := range byIndex {
		addrs = append(addrs, a...)
	}
	return addrs, err
}
