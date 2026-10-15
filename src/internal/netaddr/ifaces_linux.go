package netaddr

import (
	"fmt"
	"net"
	"sort"
	"syscall"
	"unsafe"
)

// dumpAddrs lists every address with one RTM_GETADDR dump on a socket that is
// never bound (Android 11+ refuses the bind and RTM_GETLINK), then names each
// interface from IFA_LABEL or SIOCGIFNAME and reads its flags with
// SIOCGIFFLAGS. Also built for GOOS=android, which implies linux.
//
// DECIDED: hand-rolled rather than github.com/wlynxg/anet, whose Android path
// linknames net.zoneCache and so links only with -checklinkname=0 on Go 1.23+
// (its issue #11, open since 2025-10).
func dumpAddrs() ([]net.Interface, map[int][]net.Addr, error) {
	fd, err := syscall.Socket(syscall.AF_NETLINK, syscall.SOCK_RAW|syscall.SOCK_CLOEXEC, syscall.NETLINK_ROUTE)
	if err != nil {
		return nil, nil, fmt.Errorf("netlink socket: %w", err)
	}
	defer syscall.Close(fd)
	// Never block the caller's poll loop on a dump that is not answered.
	tv := syscall.Timeval{Sec: 5}
	if err := syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &tv); err != nil {
		return nil, nil, fmt.Errorf("netlink receive timeout: %w", err)
	}
	req := make([]byte, syscall.NLMSG_HDRLEN+syscall.SizeofRtGenmsg)
	*(*syscall.NlMsghdr)(unsafe.Pointer(&req[0])) = syscall.NlMsghdr{
		Len: uint32(len(req)), Type: syscall.RTM_GETADDR,
		Flags: syscall.NLM_F_DUMP | syscall.NLM_F_REQUEST, Seq: 1,
	}
	req[syscall.NLMSG_HDRLEN] = syscall.AF_UNSPEC
	if err := syscall.Sendto(fd, req, 0, &syscall.SockaddrNetlink{Family: syscall.AF_NETLINK}); err != nil {
		return nil, nil, fmt.Errorf("netlink send: %w", err)
	}

	byIndex := map[int][]net.Addr{}
	labels := map[int]string{}
	buf := make([]byte, 1<<16)
	for done := false; !done; {
		n, _, err := syscall.Recvfrom(fd, buf, 0)
		if err == syscall.EINTR { // Go's preemption signal; SO_RCVTIMEO still bounds the wait
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("netlink receive: %w", err)
		}
		msgs, err := syscall.ParseNetlinkMessage(buf[:n])
		if err != nil {
			return nil, nil, fmt.Errorf("netlink parse: %w", err)
		}
		for i := range msgs {
			m := &msgs[i]
			switch m.Header.Type {
			case syscall.NLMSG_DONE:
				done = true
			case syscall.NLMSG_ERROR:
				return nil, nil, fmt.Errorf("netlink address dump refused")
			case syscall.RTM_NEWADDR:
				if len(m.Data) < syscall.SizeofIfAddrmsg {
					continue
				}
				ifam := (*syscall.IfAddrmsg)(unsafe.Pointer(&m.Data[0]))
				attrs, err := syscall.ParseNetlinkRouteAttr(m)
				if err != nil {
					continue
				}
				idx := int(ifam.Index)
				var ip net.IP
				for _, a := range attrs {
					switch a.Attr.Type {
					case syscall.IFA_LABEL:
						labels[idx] = string(a.Value[:clen(a.Value)])
					case syscall.IFA_LOCAL: // the local end on a point-to-point link
						if ifam.Family == syscall.AF_INET {
							ip = net.IP(a.Value)
						}
					case syscall.IFA_ADDRESS:
						if ip == nil {
							ip = net.IP(a.Value)
						}
					}
				}
				if ip == nil {
					continue
				}
				bits := 8 * net.IPv6len
				if ifam.Family == syscall.AF_INET {
					ip, bits = net.IPv4(ip[0], ip[1], ip[2], ip[3]), 8*net.IPv4len
				}
				byIndex[idx] = append(byIndex[idx], &net.IPNet{IP: ip, Mask: net.CIDRMask(int(ifam.Prefixlen), bits)})
			}
		}
	}

	ifs := make([]net.Interface, 0, len(byIndex))
	for idx := range byIndex {
		ifc := net.Interface{Index: idx, Name: labels[idx]}
		if ifc.Name == "" {
			ifc.Name = ifName(idx)
		}
		ifc.Flags = ifFlags(ifc.Name)
		ifs = append(ifs, ifc)
	}
	sort.Slice(ifs, func(i, j int) bool { return ifs[i].Index < ifs[j].Index })
	return ifs, byIndex, nil
}

func clen(b []byte) int {
	for i, c := range b {
		if c == 0 {
			return i
		}
	}
	return len(b)
}

// ifreq is struct ifreq: a 16-byte name, then a union read as int32 (index)
// or uint16 (flags).
type ifreq [40]byte

func ioctl(req uintptr, r *ifreq) error {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	if _, _, e := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), req, uintptr(unsafe.Pointer(r))); e != 0 {
		return e
	}
	return nil
}

// ifName is the interface's name; an IPv6-only interface carries no label.
func ifName(idx int) string {
	var r ifreq
	*(*int32)(unsafe.Pointer(&r[syscall.IFNAMSIZ])) = int32(idx)
	if err := ioctl(syscall.SIOCGIFNAME, &r); err != nil {
		return fmt.Sprintf("if%d", idx)
	}
	return string(r[:clen(r[:syscall.IFNAMSIZ])])
}

// ifFlags maps the kernel's IFF_* bits onto net.Flags. An interface it cannot
// read is reported up: it has an address, and every caller skips down ones.
func ifFlags(name string) net.Flags {
	var r ifreq
	copy(r[:syscall.IFNAMSIZ-1], name)
	if err := ioctl(syscall.SIOCGIFFLAGS, &r); err != nil {
		return net.FlagUp
	}
	raw := *(*uint16)(unsafe.Pointer(&r[syscall.IFNAMSIZ]))
	var f net.Flags
	for _, m := range []struct {
		iff  uint16
		flag net.Flags
	}{
		{syscall.IFF_UP, net.FlagUp},
		{syscall.IFF_BROADCAST, net.FlagBroadcast},
		{syscall.IFF_LOOPBACK, net.FlagLoopback},
		{syscall.IFF_POINTOPOINT, net.FlagPointToPoint},
		{syscall.IFF_MULTICAST, net.FlagMulticast},
		{syscall.IFF_RUNNING, net.FlagRunning},
	} {
		if raw&m.iff != 0 {
			f |= m.flag
		}
	}
	return f
}
